#pragma once

#include "l2flow/kline/worker.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::kline {

struct KLineRuntimeConfig final {
    KLineWorkerConfig worker{};
    std::uint64_t feed_session_epoch = 0U;
    std::size_t micro_batch_rows = 256U;
    std::uint64_t micro_batch_max_delay_ns = 1'000'000U;
};

struct KLineRuntimeStats final {
    std::uint64_t ordered_dispositions_received = 0U;
    std::uint64_t hole_fill_dispositions_received = 0U;
    std::uint64_t rejected_dispositions_received = 0U;
    std::uint64_t gap_open_controls_received = 0U;
    std::uint64_t channel_seal_controls_received = 0U;
    std::uint64_t micro_batches_applied = 0U;
    std::uint64_t facts_in_micro_batches = 0U;
    std::uint64_t micro_batch_rows_max = 0U;
    std::uint64_t micro_batch_source_age_ns_max = 0U;
    std::uint64_t row_limit_flushes = 0U;
    std::uint64_t timer_flushes = 0U;
    std::uint64_t explicit_flushes = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t invalid_inputs = 0U;
    KLineWorkerStats workers{};
};

[[nodiscard]] bool ValidateKLineRuntimeConfig(
    const KLineRuntimeConfig& config,
    std::string* error) noexcept;

class KLineRuntime final {
public:
    ~KLineRuntime();
    KLineRuntime(const KLineRuntime&) = delete;
    KLineRuntime& operator=(const KLineRuntime&) = delete;

    [[nodiscard]] static std::unique_ptr<KLineRuntime> Create(
        KLineRuntimeConfig config,
        KLineRevisionSink* sink,
        std::string* error);

    [[nodiscard]] bool AppendDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept;
    [[nodiscard]] bool FlushDue(
        std::size_t owner,
        std::uint64_t monotonic_ns) noexcept;
    [[nodiscard]] bool Flush(std::size_t owner) noexcept;
    [[nodiscard]] bool FlushAll() noexcept;
    [[nodiscard]] bool DrainAll() noexcept;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] KLineRuntimeStats stats() const noexcept;
    [[nodiscard]] KLineWorker* worker(std::size_t owner) noexcept;
    [[nodiscard]] std::size_t owner_for_instrument(
        std::uint32_t instrument_ordinal) const noexcept;
    [[nodiscard]] const KLineRuntimeConfig& config() const noexcept;

private:
    class Impl;
    explicit KLineRuntime(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::kline
