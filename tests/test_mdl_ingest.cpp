#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/message.h"
#include "l2flow/ingest/raw_tap.h"
#include "l2flow/ingest/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::ingest {

struct MdlMessageHandlerTestAccess final {
    [[nodiscard]] static bool WaitUntilReady(
        MdlMessageHandler* handler,
        std::chrono::milliseconds timeout) noexcept {
        return handler != nullptr && handler->WaitUntilReady(timeout);
    }
};

}  // namespace l2flow::ingest

namespace {

using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

class RecordingRawTap final : public RawRecordTap {
public:
    RecordingRawTap() {
        ticks.reserve(16U);
        snapshots.reserve(16U);
    }

    [[nodiscard]] bool AppendTick(
        std::size_t decoder_lane,
        const CanonicalTick& tick) noexcept override {
        if (decoder_lane != 0U || reject_tick_ ||
            ticks.size() == ticks.capacity()) {
            return false;
        }
        ticks.push_back(tick);
        return true;
    }

    [[nodiscard]] bool AppendSnapshot(
        std::size_t decoder_lane,
        const CanonicalSnapshot& snapshot) noexcept override {
        if (decoder_lane != 0U || reject_snapshot_ ||
            snapshots.size() == snapshots.capacity()) {
            return false;
        }
        snapshots.push_back(snapshot);
        return true;
    }

    [[nodiscard]] bool PollTick(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept override {
        static_cast<void>(monotonic_ns);
        return decoder_lane == 0U;
    }

    [[nodiscard]] bool PollSnapshot(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept override {
        static_cast<void>(monotonic_ns);
        return decoder_lane == 0U;
    }

    [[nodiscard]] bool FlushTick(
        std::size_t decoder_lane) noexcept override {
        return decoder_lane == 0U;
    }

    [[nodiscard]] bool FlushSnapshot(
        std::size_t decoder_lane) noexcept override {
        return decoder_lane == 0U;
    }

    bool reject_tick_ = false;
    bool reject_snapshot_ = false;
    std::vector<CanonicalTick> ticks;
    std::vector<CanonicalSnapshot> snapshots;
};

template <typename Unsigned>
void PutUnsigned(std::span<std::byte> bytes,
                 std::size_t offset,
                 Unsigned value) {
    static_assert(std::is_unsigned_v<Unsigned>);
    CHECK(offset <= bytes.size());
    CHECK(sizeof(Unsigned) <= bytes.size() - offset);
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto shifted = static_cast<std::uint64_t>(value) >>
                             static_cast<unsigned int>(index * 8U);
        bytes[offset + index] = static_cast<std::byte>(
            shifted & UINT64_C(0xff));
    }
}

void PutI32(std::span<std::byte> bytes,
            std::size_t offset,
            std::int32_t value) {
    PutUnsigned(bytes, offset, std::bit_cast<std::uint32_t>(value));
}

void PutI64(std::span<std::byte> bytes,
            std::size_t offset,
            std::int64_t value) {
    PutUnsigned(bytes, offset, std::bit_cast<std::uint64_t>(value));
}

void AppendText(std::vector<std::byte>* body,
                std::size_t descriptor_offset,
                std::string_view text) {
    CHECK(body != nullptr);
    CHECK(text.size() <= 65'535U);
    const std::size_t start = body->size();
    body->insert(body->end(),
                 reinterpret_cast<const std::byte*>(text.data()),
                 reinterpret_cast<const std::byte*>(text.data()) +
                     text.size());
    PutUnsigned<std::uint16_t>(
        *body, descriptor_offset,
        static_cast<std::uint16_t>(text.size()));
    PutUnsigned<std::uint32_t>(
        *body, descriptor_offset + 2U,
        static_cast<std::uint32_t>(start - descriptor_offset));
}

[[nodiscard]] std::array<std::byte, kMdlHeaderBytes> MakeHeader(
    MessageKey key,
    std::size_t body_size,
    std::uint64_t vendor_sequence = 1U) {
    CHECK(body_size <=
          std::numeric_limits<std::uint32_t>::max() - kMdlHeaderBytes);
    std::array<std::byte, kMdlHeaderBytes> header{};
    PutUnsigned<std::uint8_t>(header, 0U,
                              static_cast<std::uint8_t>(kMdlHeaderBytes));
    PutUnsigned<std::uint32_t>(
        header, 1U,
        static_cast<std::uint32_t>(kMdlHeaderBytes + body_size));
    PutUnsigned<std::uint8_t>(header, 5U, kBinaryEncoding);
    PutUnsigned<std::uint8_t>(header, 6U, key.service_id);
    PutUnsigned<std::uint16_t>(header, 7U, key.service_version);
    PutUnsigned<std::uint16_t>(header, 9U, key.message_id);
    PutUnsigned<std::uint32_t>(header, 11U, 93'000'000U);
    PutUnsigned<std::uint64_t>(header, 15U, vendor_sequence);
    return header;
}

class TestMdlMessage final : public datayes::mdl::MDLMessage {
public:
    TestMdlMessage(std::array<std::byte, kMdlHeaderBytes> header,
                   std::vector<std::byte> body)
        : body_(std::move(body)) {
        static_assert(sizeof(head_) == kMdlHeaderBytes);
        std::memcpy(&head_, header.data(), header.size());
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    datayes::mdl::MDLMessageHead* GetHead() const override {
        return const_cast<datayes::mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return body_.empty()
            ? nullptr
            : reinterpret_cast<char*>(
                  const_cast<std::byte*>(body_.data()));
    }

protected:
    datayes::mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    datayes::mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

struct TestSubscriptionStatus final {
    MessageKey key{};
    std::uint32_t status = 0U;
};

[[nodiscard]] std::vector<std::byte> MakeSystemResponse(
    MessageKey response_key,
    std::span<const TestSubscriptionStatus> statuses,
    std::uint32_t return_code = 0U) {
    const bool logon = response_key == MessageKey{2U, 101U, 2U};
    CHECK((logon || response_key == MessageKey{2U, 101U, 23U}));
    std::vector<std::pair<std::uint32_t, std::uint32_t>> services;
    for (const TestSubscriptionStatus& status : statuses) {
        const auto key = std::pair{
            static_cast<std::uint32_t>(status.key.service_id),
            static_cast<std::uint32_t>(status.key.service_version)};
        if (std::find(services.begin(), services.end(), key) ==
            services.end()) {
            services.push_back(key);
        }
    }

    const std::size_t fixed_bytes = logon ? 24U : 8U;
    const std::size_t services_descriptor = logon ? 12U : 0U;
    const std::size_t services_start = fixed_bytes;
    std::vector<std::byte> body(
        fixed_bytes + services.size() * 16U);
    PutUnsigned<std::uint32_t>(
        body, services_descriptor,
        static_cast<std::uint32_t>(services.size()));
    PutUnsigned<std::uint32_t>(
        body, services_descriptor + 4U,
        services.empty()
            ? 0U
            : static_cast<std::uint32_t>(
                  services_start - services_descriptor));
    if (logon) {
        PutUnsigned<std::uint32_t>(body, 20U, return_code);
    }

    for (std::size_t service_index = 0U;
         service_index < services.size(); ++service_index) {
        const std::size_t service_offset =
            services_start + service_index * 16U;
        PutUnsigned<std::uint32_t>(
            body, service_offset, services[service_index].first);
        PutUnsigned<std::uint32_t>(
            body, service_offset + 4U, services[service_index].second);
        std::vector<TestSubscriptionStatus> service_statuses;
        for (const TestSubscriptionStatus& status : statuses) {
            if (status.key.service_id == services[service_index].first &&
                status.key.service_version ==
                    services[service_index].second) {
                service_statuses.push_back(status);
            }
        }
        const std::size_t descriptor_offset = service_offset + 8U;
        const std::size_t messages_start = body.size();
        body.resize(body.size() + service_statuses.size() * 8U);
        PutUnsigned<std::uint32_t>(
            body, descriptor_offset,
            static_cast<std::uint32_t>(service_statuses.size()));
        PutUnsigned<std::uint32_t>(
            body, descriptor_offset + 4U,
            service_statuses.empty()
                ? 0U
                : static_cast<std::uint32_t>(
                      messages_start - descriptor_offset));
        for (std::size_t message_index = 0U;
             message_index < service_statuses.size(); ++message_index) {
            const std::size_t message_offset =
                messages_start + message_index * 8U;
            PutUnsigned<std::uint32_t>(
                body, message_offset,
                service_statuses[message_index].key.message_id);
            PutUnsigned<std::uint32_t>(
                body, message_offset + 4U,
                service_statuses[message_index].status);
        }
    }
    return body;
}

void DeliverToHandler(MdlMessageHandler* handler,
                      MessageKey key,
                      std::vector<std::byte> body) {
    CHECK(handler != nullptr);
    const std::size_t body_size = body.size();
    TestMdlMessage message(MakeHeader(key, body_size), std::move(body));
    handler->OnMessage(nullptr, &message);
}

[[nodiscard]] std::vector<std::byte> MakeShTick(
    std::int64_t sequence,
    std::string_view security,
    std::string_view type = "T",
    std::string_view flag = "B",
    std::int32_t channel = 1) {
    std::vector<std::byte> body(70U);
    PutI64(body, 0U, sequence);
    PutI32(body, 8U, channel);
    PutUnsigned<std::uint32_t>(body, 18U, 93'000'123U);
    PutI64(body, 28U, 101);
    PutI64(body, 36U, 202);
    PutI32(body, 44U, 12'345);
    PutI64(body, 48U, 100);
    PutI64(body, 56U, 1'234'500);
    AppendText(&body, 12U, security);
    AppendText(&body, 22U, type);
    AppendText(&body, 64U, flag);
    return body;
}

[[nodiscard]] std::vector<std::byte> MakeSzOrder(
    std::int64_t sequence,
    std::string_view security,
    std::uint32_t channel = 7U) {
    std::vector<std::byte> body(58U);
    PutUnsigned<std::uint32_t>(body, 0U, channel);
    PutI64(body, 4U, sequence);
    PutI64(body, 30U, 123'400);
    PutI64(body, 38U, 900);
    PutI32(body, 46U, 49);
    PutUnsigned<std::uint32_t>(body, 50U, 93'000'456U);
    PutI32(body, 54U, 50);
    AppendText(&body, 12U, "010");
    AppendText(&body, 18U, security);
    AppendText(&body, 24U, "102");
    return body;
}

[[nodiscard]] std::vector<std::byte> MakeSzTransaction(
    std::int64_t sequence,
    std::string_view security,
    std::uint32_t channel = 7U,
    std::int32_t execution_type = 70) {
    std::vector<std::byte> body(70U);
    PutUnsigned<std::uint32_t>(body, 0U, channel);
    PutI64(body, 4U, sequence);
    PutI64(body, 18U, 111);
    PutI64(body, 26U, 222);
    PutI64(body, 46U, 123'400);
    PutI64(body, 54U, 10);
    PutI32(body, 62U, execution_type);
    PutUnsigned<std::uint32_t>(body, 66U, 93'000'789U);
    AppendText(&body, 12U, "010");
    AppendText(&body, 34U, security);
    AppendText(&body, 40U, "102");
    return body;
}

[[nodiscard]] std::vector<std::byte> MakeShSnapshot(
    std::string_view security) {
    std::vector<std::byte> body(248U);
    PutUnsigned<std::uint32_t>(body, 0U, 93'001'000U);
    PutI32(body, 10U, 1);
    for (std::size_t offset : {14U, 18U, 22U, 26U, 30U, 34U}) {
        PutI32(body, offset, 12'345);
    }
    PutUnsigned<std::uint32_t>(body, 44U, 5U);
    PutI64(body, 48U, 1'000);
    PutI64(body, 56U, 12'345'000);
    PutI64(body, 64U, 400);
    PutI32(body, 72U, 12'340);
    PutI64(body, 80U, 500);
    PutI32(body, 88U, 12'350);
    AppendText(&body, 4U, security);
    AppendText(&body, 38U, "T");
    return body;
}

[[nodiscard]] std::vector<std::byte> MakeSzSnapshot(
    std::string_view security) {
    std::vector<std::byte> body(224U);
    PutUnsigned<std::uint32_t>(body, 0U, 93'001'500U);
    PutUnsigned<std::uint32_t>(body, 4U, 7U);
    PutI64(body, 32U, 123'400);
    PutI64(body, 40U, 8);
    PutI64(body, 48U, 2'000);
    PutI64(body, 56U, 2'468'000);
    for (std::size_t offset : {64U, 72U, 80U, 88U}) {
        PutI64(body, offset, 12'340'000);
    }
    PutI64(body, 144U, 700);
    PutI64(body, 152U, 12'350'000);
    PutI64(body, 160U, 600);
    PutI64(body, 168U, 12'330'000);
    AppendText(&body, 8U, "010");
    AppendText(&body, 14U, security);
    AppendText(&body, 20U, "102");
    AppendText(&body, 26U, "T0");
    return body;
}

[[nodiscard]] InstrumentCatalog MakeCatalog() {
    std::vector<InstrumentDefinition> definitions;
    definitions.push_back({1U, Market::kShanghai, {}, "600000"});
    definitions.push_back({2U, Market::kShenzhen, "102", "000001"});
    InstrumentCatalog catalog;
    std::string error;
    CHECK(InstrumentCatalog::Build(
        std::move(definitions), &catalog, &error));
    return catalog;
}

[[nodiscard]] EngineConfig MakeConfig(StartMode mode) {
    EngineConfig config{};
    config.trade_date = 20260806U;
    config.start_mode = mode;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.instrument_workers = 1U;
    config.tick_slots_per_lane = 64U;
    config.snapshot_slots_per_lane = 16U;
    config.maximum_tick_body_bytes = 512U;
    config.maximum_snapshot_body_bytes = 4'096U;
    config.dispatch_queue_capacity = 64U;
    config.diagnostic_queue_capacity = 64U;
    config.late_recovery_queue_capacity = 64U;
    config.maximum_channels_per_tick_lane = 8U;
    config.reorder_entries_per_channel = 16U;
    config.maximum_reorder_span = 15U;
    config.partial_initial_hold_ns = 0U;
    config.partial_gap_wait_ns = 0U;
    config.from_open_gap_wait_ns = UINT64_C(1'000'000'000);
    config.recovery_timer_scan_ns = 1'000U;
    config.maximum_depth_items = 32U;
    config.maximum_queue_items = 128U;
    return config;
}

[[nodiscard]] std::unique_ptr<IngestEngine> MakeEngine(StartMode mode) {
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        MakeConfig(mode), MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));
    return engine;
}

void Admit(IngestEngine* engine,
           MessageKey key,
           const std::vector<std::byte>& body,
           std::uint64_t receive_time) {
    const auto header = MakeHeader(key, body.size(), receive_time);
    CHECK(engine->AdmitMdlMessage(header, body, receive_time) ==
          AdmissionResult::kAccepted);
}

template <typename Poll>
[[nodiscard]] bool WaitFor(Poll&& poll,
                           std::chrono::milliseconds timeout =
                               std::chrono::milliseconds(2'000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (poll()) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void TestHeaderAndUnsupportedTuple() {
    ParsedHeader parsed{};
    const auto header = MakeHeader({4U, 101U, 24U}, 70U, 99U);
    CHECK(ParseMdlHeader(header, &parsed));
    CHECK(parsed.head_size == 23U);
    CHECK(parsed.message_size == 93U);
    CHECK((parsed.key == MessageKey{4U, 101U, 24U}));
    CHECK(parsed.vendor_sequence_id == 99U);

    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    const std::vector<std::byte> empty;
    const auto unsupported = MakeHeader({6U, 101U, 53U}, 0U);
    CHECK(engine->AdmitMdlMessage(unsupported, empty, 1U) ==
          AdmissionResult::kUnsupportedMessage);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestStreamSelectionDisablesUnlistedTuple() {
    EngineConfig config = MakeConfig(StartMode::kFromOpen);
    config.enabled_streams = StreamBit({4U, 101U, 24U});
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));
    const std::vector<std::byte> body = MakeShSnapshot("600000");
    const auto header = MakeHeader({4U, 101U, 4U}, body.size());
    CHECK(engine->AdmitMdlMessage(header, body, 1U) ==
          AdmissionResult::kDisabledMessage);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestFromOpenReordersShanghai() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(2, "600000"), base_time);
    CanonicalTick tick{};
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK(!engine->TryPollTick(0U, &tick));
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(1, "600000"), base_time + 1U);
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 1U);
    CHECK(tick.price.p6_valid);
    CHECK(tick.price.p6 == 12'345'000);
    CHECK((tick.validity & kTickChannelHistoryValid) != 0U);
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 2U);
    engine->Stop();
}

void TestGapWaitDefaultsAreEqual() {
    const EngineConfig config{};
    CHECK(config.from_open_gap_wait_ns == config.partial_gap_wait_ns);
    CHECK(config.from_open_gap_wait_ns == UINT64_C(500'000));
}

void TestFromOpenGapTimeoutAdvancesAndRoutesLateBackfill() {
    EngineConfig config = MakeConfig(StartMode::kFromOpen);
    config.from_open_gap_wait_ns = 0U;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(2, "600000"), base_time);
    ChannelGap gap{};
    CHECK(WaitFor([&] { return engine->TryPollGap(&gap); }));
    CHECK(gap.first_missing == 1U);
    CHECK(gap.last_missing == 1U);

    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 2U);
    CHECK((tick.validity & kTickChannelHistoryValid) == 0U);

    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(1, "600000"), base_time + 1U);
    LateRecoveryTick late{};
    CHECK(WaitFor([&] { return engine->TryPollLateRecovery(&late); }));
    CHECK(late.tick.common.native_sequence == 1U);
    CHECK(late.committed_next_sequence == 3U);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestFromOpenCapacityAdvancesWithoutLossAndRoutesBackfill() {
    EngineConfig config = MakeConfig(StartMode::kFromOpen);
    config.reorder_entries_per_channel = 4U;
    config.maximum_reorder_span = 4U;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const std::uint64_t base_time = MonotonicNowNs();
    for (std::uint64_t sequence = 2U; sequence <= 6U; ++sequence) {
        Admit(engine.get(), {4U, 101U, 24U},
              MakeShTick(static_cast<std::int64_t>(sequence), "600000"),
              base_time + sequence);
    }

    ChannelGap gap{};
    CHECK(WaitFor([&] { return engine->TryPollGap(&gap); }));
    CHECK(gap.first_missing == 1U);
    CHECK(gap.last_missing == 1U);
    CHECK(gap.first_present_after_gap == 2U);
    CHECK(gap.gap_epoch == 1U);

    CanonicalTick tick{};
    for (std::uint64_t expected = 2U; expected <= 6U; ++expected) {
        CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
        CHECK(tick.common.native_sequence == expected);
        CHECK((tick.validity & kTickChannelHistoryValid) == 0U);
    }

    // A network backfill that arrives after the committed advance cannot be
    // inserted backward; preserve its canonical body for reconciliation.
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(1, "600000"), base_time + 7U);
    LateRecoveryTick late{};
    CHECK(WaitFor([&] { return engine->TryPollLateRecovery(&late); }));
    CHECK(late.tick.common.native_sequence == 1U);
    CHECK(late.committed_next_sequence == 7U);
    CHECK(late.reason ==
          LateRecoveryReason::kBehindCommittedFrontier);
    CHECK(engine->stats().from_open_channels_frozen == 0U);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestShenzhenOrderAndTransactionShareDomain() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {6U, 101U, 36U},
          MakeSzTransaction(2, "000001"), base_time);
    Admit(engine.get(), {6U, 101U, 33U},
          MakeSzOrder(1, "000001"), base_time + 1U);
    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 1U);
    CHECK(tick.common.kind == CanonicalKind::kShenzhenOrder);
    CHECK(tick.order_type == OrderType::kLimit);
    CHECK(tick.price.p6 == 12'340'000);
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 2U);
    CHECK(tick.common.kind == CanonicalKind::kShenzhenTransaction);
    CHECK(tick.action == TickAction::kTrade);
    engine->Stop();
}

void TestPartialSkipsGapWithoutFreezing() {
    std::unique_ptr<IngestEngine> engine = MakeEngine(StartMode::kPartial);
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(10, "600000"), 10U);
    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 10U);
    CHECK((tick.validity & kTickChannelHistoryValid) == 0U);
    CHECK((tick.common.quality_flags &
           kQualityChannelHistoryIncomplete) != 0U);

    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(12, "600000"), 12U);
    ChannelGap gap{};
    CHECK(WaitFor([&] { return engine->TryPollGap(&gap); }));
    CHECK(gap.first_missing == 11U);
    CHECK(gap.last_missing == 11U);
    CHECK(gap.first_present_after_gap == 12U);
    CHECK(gap.gap_epoch == 1U);
    CHECK(gap.cumulative_missing_sequences == 1U);
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 12U);
    CHECK(tick.common.gap_before_first == 11U);
    CHECK(tick.common.gap_before_last == 11U);
    CHECK((tick.common.quality_flags & kQualitySequenceGapBefore) != 0U);

    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(11, "600000"), 13U);
    LateRecoveryTick late{};
    CHECK(WaitFor([&] { return engine->TryPollLateRecovery(&late); }));
    CHECK(late.tick.common.native_sequence == 11U);
    CHECK(late.committed_next_sequence == 13U);
    CHECK(late.observed_gap_epoch == 1U);
    CHECK(late.reason ==
          LateRecoveryReason::kBehindCommittedFrontier);
    CHECK(late.catalog_match);
    CHECK((late.tick.validity & kTickChannelHistoryValid) == 0U);
    CHECK((late.tick.common.quality_flags & kQualityLateRecovery) != 0U);
    CHECK(!engine->TryPollTick(0U, &tick));
    CHECK(engine->healthy());
    CHECK(engine->stats().duplicates_or_late >= 1U);
    CHECK(engine->stats().late_recovery_dispatched == 1U);
    engine->Stop();
}

void TestPartialReorderCapacityAdvancesWithoutLoss() {
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.reorder_entries_per_channel = 4U;
    config.maximum_reorder_span = 4U;
    config.partial_gap_wait_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(3, "600000"), base_time);
    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 3U);

    for (std::uint64_t sequence = 5U; sequence <= 9U; ++sequence) {
        Admit(engine.get(), {4U, 101U, 24U},
              MakeShTick(static_cast<std::int64_t>(sequence), "600000"),
              base_time + sequence);
    }

    ChannelGap gap{};
    CHECK(WaitFor([&] { return engine->TryPollGap(&gap); }));
    CHECK(gap.first_missing == 4U);
    CHECK(gap.last_missing == 4U);
    CHECK(gap.first_present_after_gap == 5U);
    CHECK(gap.cumulative_missing_sequences == 1U);

    for (std::uint64_t expected = 5U; expected <= 9U; ++expected) {
        CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
        CHECK(tick.common.native_sequence == expected);
        CHECK((tick.validity & kTickChannelHistoryValid) == 0U);
    }
    CHECK(engine->healthy());
    CHECK(engine->stats().gaps_skipped == 1U);
    engine->Stop();
}

void TestPartialGapMailboxCoalescesWithoutOverflow() {
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.diagnostic_queue_capacity = 2U;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(10, "600001"), base_time);
    CHECK(WaitFor([&] { return engine->stats().decoded_ticks == 1U; }));
    constexpr std::uint64_t kGapCount = 100U;
    for (std::uint64_t index = 1U; index <= kGapCount; ++index) {
        const std::uint64_t sequence = 10U + index * 2U;
        Admit(engine.get(), {4U, 101U, 24U},
              MakeShTick(static_cast<std::int64_t>(sequence), "600001"),
              base_time + index);
        CHECK(WaitFor([&] {
            const EngineStats stats = engine->stats();
            return stats.decoded_ticks == index + 1U &&
                   stats.gaps_skipped == index;
        }));
    }

    ChannelGap gap{};
    CHECK(WaitFor([&] { return engine->TryPollGap(&gap); }));
    CHECK(gap.gap_epoch == kGapCount);
    CHECK(gap.cumulative_missing_sequences == kGapCount);
    CHECK(gap.first_missing == 10U + kGapCount * 2U - 1U);
    CHECK(gap.last_missing == gap.first_missing);
    CHECK(engine->stats().gaps_skipped == kGapCount);
    CHECK(engine->stats().dispatch_overflows == 0U);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestCatalogMissStillAdvancesNativeSequence() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(1, "600001"), 1U);
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(2, "600000"), 2U);
    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 2U);
    CHECK(tick.common.instrument_id == 1U);
    CHECK(engine->stats().catalog_misses == 1U);
    engine->Stop();
}

void TestRawTapRetainsRetransmissionsBeforeRecoveryMutation() {
    RecordingRawTap raw;
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.raw_record_tap = &raw;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const MessageKey key{4U, 101U, 24U};
    const std::vector<std::byte> first = MakeShTick(1, "600000");
    const std::vector<std::byte> after_gap = MakeShTick(3, "600000");
    const auto first_header = MakeHeader(key, first.size(), 100U);
    const auto repeated_header = MakeHeader(key, after_gap.size(), 101U);
    CHECK(engine->AdmitMdlMessage(first_header, first, 1U) ==
          AdmissionResult::kAccepted);
    CHECK(engine->AdmitMdlMessage(repeated_header, after_gap, 2U) ==
          AdmissionResult::kAccepted);
    CHECK(engine->AdmitMdlMessage(repeated_header, after_gap, 3U) ==
          AdmissionResult::kAccepted);
    CHECK(WaitFor([&] { return engine->stats().decoded_ticks == 3U; }));
    engine->Stop();

    CHECK(raw.ticks.size() == 3U);
    CHECK(raw.ticks[0U].common.native_sequence == 1U);
    CHECK(raw.ticks[1U].common.native_sequence == 3U);
    CHECK(raw.ticks[2U].common.native_sequence == 3U);
    CHECK(raw.ticks[1U].common.ingress_sequence !=
          raw.ticks[2U].common.ingress_sequence);
    CHECK((raw.ticks[1U].common.quality_flags &
           kQualitySequenceGapBefore) == 0U);
    CHECK((raw.ticks[2U].common.quality_flags &
           kQualitySequenceGapBefore) == 0U);
    CHECK(raw.ticks[1U].common.gap_before_first == 0U);
    CHECK(raw.ticks[1U].common.gap_before_last == 0U);

    std::vector<CanonicalTick> ordered;
    CanonicalTick tick{};
    while (engine->TryPollTick(0U, &tick)) {
        ordered.push_back(tick);
    }
    CHECK(ordered.size() == 2U);
    CHECK(ordered[0U].common.native_sequence == 1U);
    CHECK(ordered[1U].common.native_sequence == 3U);
    CHECK((ordered[1U].common.quality_flags &
           kQualitySequenceGapBefore) != 0U);
    CHECK(ordered[1U].common.gap_before_first == 2U);
    CHECK(ordered[1U].common.gap_before_last == 2U);
}

void TestRawTapRetainsCatalogMissesBeforeOwnerSuppression() {
    RecordingRawTap raw;
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.raw_record_tap = &raw;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(1, "600001"), 1U);
    Admit(engine.get(), {4U, 101U, 4U},
          MakeShSnapshot("600001"), 2U);
    CHECK(WaitFor([&] {
        const EngineStats stats = engine->stats();
        return stats.decoded_ticks == 1U &&
               stats.decoded_snapshots == 1U;
    }));
    engine->Stop();

    CHECK(raw.ticks.size() == 1U);
    CHECK(raw.snapshots.size() == 1U);
    CHECK((raw.ticks.front().common.quality_flags &
           kQualityInstrumentNotInCatalog) != 0U);
    CHECK((raw.snapshots.front().common.quality_flags &
           kQualityInstrumentNotInCatalog) != 0U);
    CanonicalTick tick{};
    CanonicalSnapshot snapshot{};
    CHECK(!engine->TryPollTick(0U, &tick));
    CHECK(!engine->TryPollSnapshot(0U, &snapshot));
    CHECK(engine->stats().catalog_misses == 2U);
}

void TestRawTapFailureClosesAdmission() {
    RecordingRawTap raw;
    raw.reject_tick_ = true;
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.raw_record_tap = &raw;
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const MessageKey key{4U, 101U, 24U};
    const std::vector<std::byte> body = MakeShTick(1, "600000");
    const auto header = MakeHeader(key, body.size(), 100U);
    CHECK(engine->AdmitMdlMessage(header, body, 1U) ==
          AdmissionResult::kAccepted);
    CHECK(WaitFor([&] { return !engine->healthy(); }));
    CHECK(engine->AdmitMdlMessage(header, body, 2U) !=
          AdmissionResult::kAccepted);
    engine->Stop();
    CHECK(engine->fatal_error().find("raw Tick batch queue") !=
          std::string::npos);
    CanonicalTick tick{};
    CHECK(!engine->TryPollTick(0U, &tick));

    RecordingRawTap snapshot_raw;
    snapshot_raw.reject_snapshot_ = true;
    config = MakeConfig(StartMode::kPartial);
    config.raw_record_tap = &snapshot_raw;
    engine = IngestEngine::Create(config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const MessageKey snapshot_key{4U, 101U, 4U};
    const std::vector<std::byte> snapshot_body = MakeShSnapshot("600000");
    const auto snapshot_header =
        MakeHeader(snapshot_key, snapshot_body.size(), 101U);
    CHECK(engine->AdmitMdlMessage(
              snapshot_header, snapshot_body, 3U) ==
          AdmissionResult::kAccepted);
    CHECK(WaitFor([&] { return !engine->healthy(); }));
    engine->Stop();
    CHECK(engine->fatal_error().find("raw Snapshot batch queue") !=
          std::string::npos);
    CanonicalSnapshot snapshot{};
    CHECK(!engine->TryPollSnapshot(0U, &snapshot));
}

void TestBothSnapshotLayouts() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    Admit(engine.get(), {4U, 101U, 4U},
          MakeShSnapshot("600000"), 1U);
    Admit(engine.get(), {6U, 101U, 28U},
          MakeSzSnapshot("000001"), 2U);
    CanonicalSnapshot snapshot{};
    CHECK(WaitFor([&] {
        return engine->TryPollSnapshot(0U, &snapshot);
    }));
    CHECK(snapshot.common.kind == CanonicalKind::kShanghaiSnapshot);
    CHECK(snapshot.last.p6 == 12'345'000);
    CHECK(snapshot.trade_count_valid && snapshot.trade_count == 5U);
    CHECK(WaitFor([&] {
        return engine->TryPollSnapshot(0U, &snapshot);
    }));
    CHECK(snapshot.common.kind == CanonicalKind::kShenzhenSnapshot);
    CHECK(snapshot.last.p6 == 12'340'000);
    CHECK(snapshot.common.channel == 7U);
    engine->Stop();
}

void TestMalformedSnapshotListFailsDecode() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    std::vector<std::byte> body = MakeShSnapshot("600000");
    PutUnsigned<std::uint32_t>(body, 228U, 1U);
    PutUnsigned<std::uint32_t>(body, 232U, 8U);
    Admit(engine.get(), {4U, 101U, 4U}, body, 1U);
    CHECK(WaitFor([&] { return engine->stats().decode_errors == 1U; }));
    CanonicalSnapshot snapshot{};
    CHECK(!engine->TryPollSnapshot(0U, &snapshot));
    engine->Stop();
}

void TestDynamicRangesMayNotOverlap() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    std::vector<std::byte> body = MakeShTick(1, "600000");
    // Type is made to alias SecurityID's dynamic bytes. Each individual
    // descriptor is in bounds; only range-overlap validation rejects it.
    PutUnsigned<std::uint16_t>(body, 22U, 6U);
    PutUnsigned<std::uint32_t>(body, 24U, 48U);
    Admit(engine.get(), {4U, 101U, 24U}, body, 1U);
    CHECK(WaitFor([&] { return engine->stats().decode_errors == 1U; }));
    CanonicalTick tick{};
    CHECK(!engine->TryPollTick(0U, &tick));
    engine->Stop();
}

void TestShanghaiDepthAndNestedQueue() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    std::vector<std::byte> body = MakeShSnapshot("600000");
    const std::size_t level_start = body.size();
    body.resize(level_start + 28U);
    PutUnsigned<std::uint32_t>(body, 228U, 1U);
    PutUnsigned<std::uint32_t>(
        body, 232U,
        static_cast<std::uint32_t>(level_start - 228U));
    PutI32(body, level_start + 4U, 12'340);
    PutI64(body, level_start + 8U, 500);
    PutUnsigned<std::uint32_t>(body, level_start + 16U, 1U);
    const std::size_t queue_start = body.size();
    body.resize(queue_start + 16U);
    PutUnsigned<std::uint32_t>(body, level_start + 20U, 1U);
    PutUnsigned<std::uint32_t>(
        body, level_start + 24U,
        static_cast<std::uint32_t>(queue_start - (level_start + 20U)));
    PutI64(body, queue_start + 8U, 500);

    Admit(engine.get(), {4U, 101U, 4U}, body, 1U);
    CanonicalSnapshot snapshot{};
    CHECK(WaitFor([&] {
        return engine->TryPollSnapshot(0U, &snapshot);
    }));
    CHECK(snapshot.source_bid_depth == 1U);
    CHECK(snapshot.retained_bid_depth == 1U);
    CHECK(snapshot.bids[0].price.p6 == 12'340'000);
    CHECK(snapshot.bids[0].quantity.raw == 500);
    CHECK(snapshot.bids[0].source_order_count == 1U);
    engine->Stop();
}

void TestFromOpenConflictFreezesOnlyThatChannel() {
    std::unique_ptr<IngestEngine> engine =
        MakeEngine(StartMode::kFromOpen);
    std::vector<std::byte> first = MakeShTick(2, "600000");
    std::vector<std::byte> conflicting = first;
    PutI32(conflicting, 44U, 12'346);
    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U}, first, base_time);
    Admit(engine.get(), {4U, 101U, 24U}, conflicting, base_time + 1U);
    ChannelFault fault{};
    CHECK(WaitFor([&] { return engine->TryPollChannelFault(&fault); }));
    CHECK(fault.market == Market::kShanghai);
    CHECK(fault.channel == 1U);
    CHECK(fault.reason == ChannelFaultReason::kCanonicalConflict);
    CHECK(fault.expected_sequence == 1U);
    CHECK(fault.observed_sequence == 2U);
    CHECK(engine->stats().from_open_channels_frozen == 1U);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestPartialConflictKeepsFirstWithoutFreezing() {
    EngineConfig config = MakeConfig(StartMode::kPartial);
    config.partial_gap_wait_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    const std::uint64_t base_time = MonotonicNowNs();
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(10, "600000"), base_time);
    CanonicalTick tick{};
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 10U);

    std::vector<std::byte> first = MakeShTick(12, "600000");
    std::vector<std::byte> conflicting = first;
    PutI32(conflicting, 44U, 12'346);
    Admit(engine.get(), {4U, 101U, 24U}, first, base_time + 1U);
    Admit(engine.get(), {4U, 101U, 24U}, conflicting, base_time + 2U);
    LateRecoveryTick late{};
    CHECK(WaitFor([&] { return engine->TryPollLateRecovery(&late); }));
    CHECK(late.reason ==
          LateRecoveryReason::kPendingCanonicalConflict);
    CHECK(late.tick.price.p6 == 12'346'000);
    Admit(engine.get(), {4U, 101U, 24U},
          MakeShTick(11, "600000"), base_time + 3U);

    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 11U);
    CHECK(WaitFor([&] { return engine->TryPollTick(0U, &tick); }));
    CHECK(tick.common.native_sequence == 12U);
    CHECK(tick.price.p6 == 12'345'000);
    ChannelFault fault{};
    CHECK(!engine->TryPollChannelFault(&fault));
    CHECK(engine->stats().from_open_channels_frozen == 0U);
    CHECK(engine->healthy());
    engine->Stop();
}

void TestMdlConnectionBoundaryClassification() {
    CHECK(ClassifyMdlConnectionBoundary({1U, 101U, 2U}) ==
          MdlConnectionBoundaryReason::kConnectError);
    CHECK(ClassifyMdlConnectionBoundary({1U, 101U, 3U}) ==
          MdlConnectionBoundaryReason::kDisconnected);
    CHECK(ClassifyMdlConnectionBoundary({1U, 101U, 5U}) ==
          MdlConnectionBoundaryReason::kServiceTimeout);
    CHECK(ClassifyMdlConnectionBoundary({1U, 101U, 6U}) ==
          MdlConnectionBoundaryReason::kMessageDiscarded);
    CHECK(ClassifyMdlConnectionBoundary({1U, 101U, 1U}) ==
          MdlConnectionBoundaryReason::kNone);
    CHECK(ClassifyMdlConnectionBoundary({4U, 101U, 24U}) ==
          MdlConnectionBoundaryReason::kNone);
}

void TestMdlConnectionBoundaryStopsAdmission() {
    std::unique_ptr<IngestEngine> engine = MakeEngine(StartMode::kPartial);
    MdlMessageHandler handler(engine.get());

    TestMdlMessage disconnected(
        MakeHeader({1U, 101U, 3U}, 12U),
        std::vector<std::byte>(12U));
    handler.OnMessage(nullptr, &disconnected);
    CHECK(handler.last_result() == AdmissionResult::kAccepted);
    CHECK(handler.connection_boundary_reason() ==
          MdlConnectionBoundaryReason::kDisconnected);
    const std::uint64_t boundary_time =
        handler.connection_boundary_monotonic_ns();
    CHECK(boundary_time != 0U);

    const std::vector<std::byte> tick_body = MakeShTick(1, "600000");
    TestMdlMessage tick(
        MakeHeader({4U, 101U, 24U}, tick_body.size()), tick_body);
    handler.OnMessage(nullptr, &tick);
    CHECK(handler.last_result() == AdmissionResult::kNotRunning);
    CHECK(handler.connection_boundary_reason() ==
          MdlConnectionBoundaryReason::kDisconnected);
    CHECK(handler.connection_boundary_monotonic_ns() == boundary_time);
    CHECK(engine->stats().admitted == 0U);
    engine->Stop();
}

void TestMdlReadinessRequiresLogonAndConfiguredStatuses() {
    constexpr MessageKey kShanghaiTick{4U, 101U, 24U};
    constexpr MessageKey kShenzhenOrder{6U, 101U, 33U};
    const StreamMask expected =
        StreamBit(kShanghaiTick) | StreamBit(kShenzhenOrder);

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        CHECK(handler.expected_streams() == expected);
        const std::array statuses{
            TestSubscriptionStatus{kShanghaiTick, 0U},
            TestSubscriptionStatus{kShenzhenOrder, 0U},
        };
        DeliverToHandler(
            &handler, {2U, 101U, 23U},
            MakeSystemResponse({2U, 101U, 23U}, statuses));
        CHECK(!handler.feed_ready());
        CHECK(handler.feed_ready_monotonic_ns() == 0U);

        DeliverToHandler(
            &handler, {2U, 101U, 2U},
            MakeSystemResponse(
                {2U, 101U, 2U},
                std::span<const TestSubscriptionStatus>{}));
        CHECK(handler.feed_ready());
        const std::uint64_t ready_time =
            handler.feed_ready_monotonic_ns();
        CHECK(ready_time != 0U);

        DeliverToHandler(
            &handler, {2U, 101U, 23U},
            MakeSystemResponse({2U, 101U, 23U}, statuses));
        CHECK(handler.feed_ready_monotonic_ns() == ready_time);
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kNone);
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        const std::array first_status{
            TestSubscriptionStatus{kShanghaiTick, 0U}};
        DeliverToHandler(
            &handler, {2U, 101U, 2U},
            MakeSystemResponse({2U, 101U, 2U}, first_status));
        CHECK(!handler.feed_ready());

        const std::array second_status{
            TestSubscriptionStatus{kShenzhenOrder, 0U}};
        DeliverToHandler(
            &handler, {2U, 101U, 23U},
            MakeSystemResponse({2U, 101U, 23U}, second_status));
        CHECK(handler.feed_ready());
        CHECK(handler.feed_ready_monotonic_ns() != 0U);
        engine->Stop();
    }
}

void TestMdlReadinessRejectsFailedAndMalformedResponses() {
    constexpr MessageKey kShanghaiTick{4U, 101U, 24U};
    const StreamMask expected = StreamBit(kShanghaiTick);

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        DeliverToHandler(
            &handler, {2U, 101U, 2U},
            MakeSystemResponse(
                {2U, 101U, 2U},
                std::span<const TestSubscriptionStatus>{},
                static_cast<std::uint32_t>(
                    datayes::mdl::MDLEC_UNAUTHORIZED)));
        CHECK(!handler.feed_ready());
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kSubscriptionRejected);
        CHECK(handler.connection_boundary_detail().find("code 5") !=
              std::string::npos);
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        const std::array failed_status{
            TestSubscriptionStatus{
                kShanghaiTick,
                static_cast<std::uint32_t>(datayes::mdl::MDLEC_TIMEOUT)}};
        DeliverToHandler(
            &handler, {2U, 101U, 2U},
            MakeSystemResponse({2U, 101U, 2U}, failed_status));
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kSubscriptionRejected);
        CHECK(handler.connection_boundary_detail().find("4.101.24") !=
              std::string::npos);
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        std::vector<std::byte> malformed = MakeSystemResponse(
            {2U, 101U, 2U},
            std::span<const TestSubscriptionStatus>{});
        PutUnsigned<std::uint16_t>(malformed, 0U, 1U);
        PutUnsigned<std::uint32_t>(malformed, 2U, UINT32_MAX);
        DeliverToHandler(
            &handler, {2U, 101U, 2U}, std::move(malformed));
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kControlProtocolError);
        CHECK(handler.connection_boundary_detail().find("string field") !=
              std::string::npos);
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        std::vector<std::byte> malformed = MakeSystemResponse(
            {2U, 101U, 2U},
            std::span<const TestSubscriptionStatus>{});
        PutUnsigned<std::uint32_t>(malformed, 12U, 1U);
        PutUnsigned<std::uint32_t>(malformed, 16U, UINT32_MAX);
        DeliverToHandler(
            &handler, {2U, 101U, 2U}, std::move(malformed));
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kControlProtocolError);
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get(), expected);
        const std::array duplicate_statuses{
            TestSubscriptionStatus{kShanghaiTick, 0U},
            TestSubscriptionStatus{kShanghaiTick, 0U},
        };
        DeliverToHandler(
            &handler, {2U, 101U, 2U},
            MakeSystemResponse({2U, 101U, 2U}, duplicate_statuses));
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kControlProtocolError);
        engine->Stop();
    }
}

void TestMdlApiFaultEventsAndFirstBoundaryAreSticky() {
    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get());
        DeliverToHandler(&handler, {1U, 101U, 5U}, {});
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kServiceTimeout);
        CHECK(handler.connection_boundary_detail() ==
              "MDL MessageServiceTimeOutEvent");
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get());
        DeliverToHandler(&handler, {1U, 101U, 6U}, {});
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kMessageDiscarded);
        CHECK(handler.connection_boundary_detail() ==
              "MDL MessageDiscardedEvent");
        engine->Stop();
    }

    {
        std::unique_ptr<IngestEngine> engine =
            MakeEngine(StartMode::kPartial);
        MdlMessageHandler handler(engine.get());
        DeliverToHandler(&handler, {1U, 101U, 3U}, {});
        const std::uint64_t first_time =
            handler.connection_boundary_monotonic_ns();
        const std::string first_detail =
            handler.connection_boundary_detail();
        DeliverToHandler(&handler, {1U, 101U, 2U}, {});
        CHECK(handler.connection_boundary_reason() ==
              MdlConnectionBoundaryReason::kDisconnected);
        CHECK(handler.connection_boundary_monotonic_ns() == first_time);
        CHECK(handler.connection_boundary_detail() == first_detail);
        engine->Stop();
    }
}

void TestMdlReadinessWaitTimeoutIsAnEpochBoundary() {
    std::unique_ptr<IngestEngine> engine = MakeEngine(StartMode::kPartial);
    MdlMessageHandler handler(
        engine.get(), StreamBit({4U, 101U, 24U}));
    const auto started = std::chrono::steady_clock::now();
    CHECK(!MdlMessageHandlerTestAccess::WaitUntilReady(
        &handler, std::chrono::milliseconds(1)));
    const auto elapsed = std::chrono::steady_clock::now() - started;
    CHECK(elapsed < std::chrono::seconds(1));
    CHECK(handler.connection_boundary_reason() ==
          MdlConnectionBoundaryReason::kReadyTimeout);
    CHECK(handler.connection_boundary_monotonic_ns() != 0U);
    CHECK(handler.connection_boundary_detail().find("timed out") !=
          std::string::npos);
    engine->Stop();
}

void TestMdlMarketDataBeforeReadinessStopsAdmission() {
    std::unique_ptr<IngestEngine> engine = MakeEngine(StartMode::kPartial);
    MdlMessageHandler handler(
        engine.get(), StreamBit({4U, 101U, 24U}));
    const std::vector<std::byte> body = MakeShTick(1, "600000");
    TestMdlMessage message(
        MakeHeader({4U, 101U, 24U}, body.size()), body);
    handler.OnMessage(nullptr, &message);
    CHECK(handler.last_result() == AdmissionResult::kFeedNotReady);
    CHECK(handler.connection_boundary_reason() ==
          MdlConnectionBoundaryReason::kControlProtocolError);
    CHECK(handler.connection_boundary_detail().find("before MDL feed") !=
          std::string::npos);
    CHECK(engine->stats().admitted == 0U);
    engine->Stop();
}

}  // namespace

int main() {
    TestHeaderAndUnsupportedTuple();
    TestStreamSelectionDisablesUnlistedTuple();
    TestFromOpenReordersShanghai();
    TestGapWaitDefaultsAreEqual();
    TestFromOpenGapTimeoutAdvancesAndRoutesLateBackfill();
    TestFromOpenCapacityAdvancesWithoutLossAndRoutesBackfill();
    TestShenzhenOrderAndTransactionShareDomain();
    TestPartialSkipsGapWithoutFreezing();
    TestPartialReorderCapacityAdvancesWithoutLoss();
    TestPartialGapMailboxCoalescesWithoutOverflow();
    TestCatalogMissStillAdvancesNativeSequence();
    TestRawTapRetainsRetransmissionsBeforeRecoveryMutation();
    TestRawTapRetainsCatalogMissesBeforeOwnerSuppression();
    TestRawTapFailureClosesAdmission();
    TestBothSnapshotLayouts();
    TestMalformedSnapshotListFailsDecode();
    TestDynamicRangesMayNotOverlap();
    TestShanghaiDepthAndNestedQueue();
    TestFromOpenConflictFreezesOnlyThatChannel();
    TestPartialConflictKeepsFirstWithoutFreezing();
    TestMdlConnectionBoundaryClassification();
    TestMdlConnectionBoundaryStopsAdmission();
    TestMdlReadinessRequiresLogonAndConfiguredStatuses();
    TestMdlReadinessRejectsFailedAndMalformedResponses();
    TestMdlApiFaultEventsAndFirstBoundaryAreSticky();
    TestMdlReadinessWaitTimeoutIsAnEpochBoundary();
    TestMdlMarketDataBeforeReadinessStopsAdmission();
    std::cout << "all mdl_ingest tests passed\n";
    return 0;
}
