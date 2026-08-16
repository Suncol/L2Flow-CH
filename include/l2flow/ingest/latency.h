#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace l2flow::ingest {

inline constexpr std::uint64_t kDispatchLatencySampleEvery = 64U;
inline constexpr std::size_t kDispatchLatencyRingCapacity = 4'096U;

[[nodiscard]] constexpr bool ShouldSampleDispatchLatency(
    std::uint64_t ingress_sequence) noexcept {
    return ingress_sequence % kDispatchLatencySampleEvery == 0U;
}

struct DispatchLatencySample final {
    std::uint64_t sequence = 0U;
    std::uint64_t latency_ns = 0U;
};

struct DispatchLatencySampleSlot final {
    std::atomic_flag busy = ATOMIC_FLAG_INIT;
    std::uint64_t sequence = 0U;
    std::uint64_t latency_ns = 0U;
};

// One drain thread writes each sampler. The reporting thread never blocks the
// writer: if it owns a slot when that slot wraps, the new sample is dropped and
// later reported as overwritten.
struct alignas(64) DispatchLatencySampler final {
    std::array<DispatchLatencySampleSlot,
               kDispatchLatencyRingCapacity> samples{};
    std::atomic<std::uint64_t> published{0U};
    std::atomic<std::uint64_t> clock_errors{0U};
    std::uint64_t next_sequence = 0U;
};

struct DispatchLatencyWindow final {
    std::vector<DispatchLatencySample> samples;
    std::uint64_t overwritten = 0U;
    std::uint64_t clock_errors = 0U;
};

struct DispatchLatencySummary final {
    std::size_t count = 0U;
    double average_us = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double maximum_us = 0.0;
};

class DispatchLatencyAggregate final {
public:
    DispatchLatencyAggregate();

    void Add(const DispatchLatencyWindow& window) noexcept;

    [[nodiscard]] std::uint64_t count() const noexcept;
    [[nodiscard]] std::uint64_t overwritten() const noexcept;
    [[nodiscard]] std::uint64_t clock_errors() const noexcept;
    [[nodiscard]] double average_us() const noexcept;
    [[nodiscard]] double maximum_us() const noexcept;
    [[nodiscard]] double PercentileUs(
        std::uint64_t percentile) const noexcept;

private:
    std::vector<std::uint64_t> histogram_;
    std::uint64_t count_ = 0U;
    std::uint64_t maximum_ns_ = 0U;
    std::uint64_t overwritten_ = 0U;
    std::uint64_t clock_errors_ = 0U;
    long double sum_ns_ = 0.0L;
};

void ObserveDispatchLatency(std::uint64_t ingress_sequence,
                            std::uint64_t receive_monotonic_ns,
                            std::uint64_t now_monotonic_ns,
                            DispatchLatencySampler* sampler) noexcept;

[[nodiscard]] DispatchLatencyWindow CollectDispatchLatency(
    DispatchLatencySampler* samplers,
    std::size_t sampler_count,
    std::vector<std::uint64_t>* cursors,
    std::vector<std::uint64_t>* clock_error_cursors);

[[nodiscard]] DispatchLatencySummary SummarizeDispatchLatency(
    std::vector<DispatchLatencySample>* samples);

}  // namespace l2flow::ingest
