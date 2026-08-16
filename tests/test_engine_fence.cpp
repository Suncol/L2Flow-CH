#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/message.h"
#include "l2flow/outbox/wal.h"

#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::abort();                                                    \
        }                                                                    \
    } while (false)

using l2flow::ingest::AdmissionResult;
using l2flow::ingest::EngineConfig;
using l2flow::ingest::IngestEngine;
using l2flow::ingest::InstrumentCatalog;
using l2flow::ingest::InstrumentDefinition;
using l2flow::ingest::Market;
using l2flow::ingest::MessageKey;
using l2flow::ingest::MonotonicNowNs;
using l2flow::ingest::StartMode;
using l2flow::outbox::CanonicalRecord;
using l2flow::outbox::ConsumerKind;
using l2flow::outbox::DurableOutbox;
using l2flow::outbox::DurableOutboxConfig;
using l2flow::outbox::RecordKind;
using l2flow::outbox::RecordView;
using l2flow::outbox::WalPosition;

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        const std::uint64_t stamp = static_cast<std::uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
            ("l2flow-engine-fence-" + std::to_string(::getpid()) + "-" +
             std::to_string(stamp));
        CHECK(std::filesystem::create_directories(path_));
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

template <typename Unsigned>
void PutUnsigned(std::span<std::byte> bytes,
                 std::size_t offset,
                 Unsigned value) {
    static_assert(std::is_unsigned_v<Unsigned>);
    CHECK(offset <= bytes.size());
    CHECK(sizeof(Unsigned) <= bytes.size() - offset);
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const std::uint64_t shifted = static_cast<std::uint64_t>(value) >>
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
    CHECK(text.size() <=
          static_cast<std::size_t>(
              std::numeric_limits<std::uint16_t>::max()));
    const std::size_t start = body->size();
    body->insert(
        body->end(), reinterpret_cast<const std::byte*>(text.data()),
        reinterpret_cast<const std::byte*>(text.data()) + text.size());
    PutUnsigned<std::uint16_t>(
        *body, descriptor_offset,
        static_cast<std::uint16_t>(text.size()));
    PutUnsigned<std::uint32_t>(
        *body, descriptor_offset + 2U,
        static_cast<std::uint32_t>(start - descriptor_offset));
}

[[nodiscard]] std::array<std::byte, l2flow::ingest::kMdlHeaderBytes>
MakeHeader(MessageKey key,
           std::size_t body_size,
           std::uint64_t vendor_sequence) {
    CHECK(body_size <=
          std::numeric_limits<std::uint32_t>::max() -
              l2flow::ingest::kMdlHeaderBytes);
    std::array<std::byte, l2flow::ingest::kMdlHeaderBytes> header{};
    PutUnsigned<std::uint8_t>(
        header, 0U,
        static_cast<std::uint8_t>(l2flow::ingest::kMdlHeaderBytes));
    PutUnsigned<std::uint32_t>(
        header, 1U,
        static_cast<std::uint32_t>(
            l2flow::ingest::kMdlHeaderBytes + body_size));
    PutUnsigned<std::uint8_t>(
        header, 5U, l2flow::ingest::kBinaryEncoding);
    PutUnsigned<std::uint8_t>(header, 6U, key.service_id);
    PutUnsigned<std::uint16_t>(header, 7U, key.service_version);
    PutUnsigned<std::uint16_t>(header, 9U, key.message_id);
    PutUnsigned<std::uint32_t>(header, 11U, 93'000'000U);
    PutUnsigned<std::uint64_t>(header, 15U, vendor_sequence);
    return header;
}

[[nodiscard]] std::vector<std::byte> MakeShanghaiTick(
    std::int64_t sequence) {
    std::vector<std::byte> body(70U);
    PutI64(body, 0U, sequence);
    PutI32(body, 8U, 1);
    PutUnsigned<std::uint32_t>(body, 18U, 93'000'123U);
    PutI64(body, 28U, 101);
    PutI64(body, 36U, 202);
    PutI32(body, 44U, 12'345);
    PutI64(body, 48U, 100);
    PutI64(body, 56U, 1'234'500);
    AppendText(&body, 12U, "600000");
    AppendText(&body, 22U, "T");
    AppendText(&body, 64U, "B");
    return body;
}

[[nodiscard]] InstrumentCatalog MakeCatalog() {
    std::vector<InstrumentDefinition> definitions;
    definitions.push_back({1U, Market::kShanghai, {}, "600000"});
    InstrumentCatalog catalog;
    std::string error;
    CHECK(InstrumentCatalog::Build(
        std::move(definitions), &catalog, &error));
    return catalog;
}

template <typename Poll>
[[nodiscard]] bool WaitFor(Poll&& poll,
                           std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (poll()) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void TestBarrierCannotOvertakeAcceptedRecoveryInput() {
    TemporaryDirectory directory;
    DurableOutboxConfig outbox_config{};
    outbox_config.root_directory = directory.path();
    outbox_config.feed_session_epoch = 41U;
    outbox_config.producer_queue_records = 128U;
    outbox_config.commit_batch_records = 16U;
    outbox_config.commit_batch_bytes = 4U * 1'024U;
    outbox_config.segment_max_bytes = 16U * 1'024U;
    outbox_config.maximum_reservoir_bytes = 64U * 1'024U;
    outbox_config.commit_max_delay_ns = 100'000U;
    outbox_config.cursor_checkpoint_interval_ns = 100'000U;
    std::string error;
    std::unique_ptr<DurableOutbox> outbox = DurableOutbox::Create(
        outbox_config, &error);
    CHECK(outbox != nullptr);
    CHECK(outbox->Start(&error));

    EngineConfig engine_config{};
    engine_config.trade_date = 20'260'814U;
    engine_config.feed_session_epoch = outbox_config.feed_session_epoch;
    engine_config.start_mode = StartMode::kFromOpen;
    engine_config.tick_decoder_lanes = 1U;
    engine_config.snapshot_decoder_lanes = 1U;
    engine_config.instrument_workers = 1U;
    engine_config.tick_slots_per_lane = 16U;
    engine_config.snapshot_slots_per_lane = 4U;
    engine_config.maximum_tick_body_bytes = 512U;
    engine_config.maximum_snapshot_body_bytes = 4U * 1'024U;
    engine_config.maximum_channels_per_tick_lane = 4U;
    engine_config.reorder_entries_per_channel = 16U;
    engine_config.maximum_reorder_span = 15U;
    engine_config.from_open_gap_wait_ns = 10'000'000'000U;
    engine_config.recovery_timer_scan_ns = 10'000U;
    engine_config.maximum_depth_items = 32U;
    engine_config.maximum_queue_items = 128U;
    engine_config.outbox = outbox.get();
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        engine_config, MakeCatalog(), &error);
    CHECK(engine != nullptr);
    CHECK(engine->Start(&error));

    constexpr MessageKey kShanghaiTick{4U, 101U, 24U};
    const std::uint64_t second_receive = MonotonicNowNs();
    const std::vector<std::byte> second = MakeShanghaiTick(2);
    const auto second_header = MakeHeader(
        kShanghaiTick, second.size(), 2U);
    CHECK(engine->AdmitMdlMessage(
              second_header, second, second_receive) ==
          AdmissionResult::kAccepted);
    CHECK(WaitFor(
        [&] { return engine->stats().decoded_ticks == 1U; },
        std::chrono::milliseconds(500)));

    CHECK(!engine->FenceAcceptedInputs(5'000'000U, &error));
    CHECK(error.find("timed out") != std::string::npos);
    CHECK(outbox->durable_tail().lsn == 0U);

    const std::vector<std::byte> first = MakeShanghaiTick(1);
    const auto first_header = MakeHeader(kShanghaiTick, first.size(), 1U);
    CHECK(engine->AdmitMdlMessage(
              first_header, first, MonotonicNowNs()) ==
          AdmissionResult::kAccepted);
    CHECK(engine->FenceAcceptedInputs(1'000'000'000U, &error));

    CanonicalRecord barrier{};
    barrier.kind = RecordKind::kFreshnessBarrier;
    barrier.barrier.frontier_id = 1U;
    barrier.barrier.created_monotonic_ns = MonotonicNowNs();
    barrier.barrier.created_utc_ns = 1U;
    CHECK(outbox->Enqueue(barrier));
    CHECK(outbox->Flush(&error));
    CHECK(outbox->durable_tail().lsn == 3U);

    std::array<WalPosition, 3U> positions{};
    for (std::uint64_t lsn = 1U; lsn <= 3U; ++lsn) {
        RecordView view{};
        CHECK(outbox->TryRead(lsn, &view));
        positions[static_cast<std::size_t>(lsn - 1U)] = view.position;
        if (lsn < 3U) {
            CHECK(view.record->kind == RecordKind::kTickOccurrence);
            CHECK(view.record->raw_tick.common.native_sequence == lsn);
        } else {
            CHECK(view.record->kind == RecordKind::kFreshnessBarrier);
        }
    }
    CHECK(outbox->Complete(ConsumerKind::kRaw, positions));
    CHECK(outbox->Complete(ConsumerKind::kEvent, positions));
    CHECK(outbox->Complete(ConsumerKind::kKLine, positions));

    engine->Stop();
    CHECK(engine->healthy());
    CHECK(outbox->Stop(&error));
}

}  // namespace

int main() {
    TestBarrierCannotOvertakeAcceptedRecoveryInput();
    std::cout << "all engine fence tests passed\n";
    return 0;
}
