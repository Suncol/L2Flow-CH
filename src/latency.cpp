#include "l2flow/ingest/latency.h"

#include <algorithm>
#include <bit>
#include <limits>

namespace l2flow::ingest {
namespace {

inline constexpr unsigned int kHistogramPrecisionBits = 8U;
inline constexpr std::size_t kExactHistogramBuckets =
    std::size_t{1U} << kHistogramPrecisionBits;
inline constexpr std::size_t kHistogramBucketsPerPowerOfTwo =
    kExactHistogramBuckets / 2U;
inline constexpr std::size_t kHistogramBucketCount =
    kExactHistogramBuckets +
    (std::numeric_limits<std::uint64_t>::digits -
     kHistogramPrecisionBits) *
        kHistogramBucketsPerPowerOfTwo;

[[nodiscard]] std::size_t HistogramIndex(std::uint64_t value) noexcept {
    if (value < kExactHistogramBuckets) {
        return static_cast<std::size_t>(value);
    }
    const unsigned int exponent = static_cast<unsigned int>(
        std::bit_width(value) - 1U);
    const unsigned int shift =
        exponent - (kHistogramPrecisionBits - 1U);
    const std::uint64_t normalized = value >> shift;
    return kExactHistogramBuckets +
        static_cast<std::size_t>(exponent - kHistogramPrecisionBits) *
            kHistogramBucketsPerPowerOfTwo +
        static_cast<std::size_t>(
            normalized - kHistogramBucketsPerPowerOfTwo);
}

[[nodiscard]] double HistogramBucketMidpoint(std::size_t index) noexcept {
    if (index < kExactHistogramBuckets) {
        return static_cast<double>(index);
    }
    const std::size_t relative = index - kExactHistogramBuckets;
    const std::size_t exponent_group =
        relative / kHistogramBucketsPerPowerOfTwo;
    const std::size_t offset =
        relative % kHistogramBucketsPerPowerOfTwo;
    const unsigned int exponent = static_cast<unsigned int>(
        exponent_group + kHistogramPrecisionBits);
    const unsigned int shift =
        exponent - (kHistogramPrecisionBits - 1U);
    const std::uint64_t normalized =
        static_cast<std::uint64_t>(
            kHistogramBucketsPerPowerOfTwo + offset);
    const long double lower = static_cast<long double>(normalized) *
        static_cast<long double>(std::uint64_t{1U} << shift);
    const long double width = static_cast<long double>(
        std::uint64_t{1U} << shift);
    return static_cast<double>(lower + width / 2.0L);
}

[[nodiscard]] std::uint64_t PercentileRank(std::uint64_t count,
                                           std::uint64_t percentile) noexcept {
    return (count / 100U) * percentile +
        ((count % 100U) * percentile + 99U) / 100U;
}

}  // namespace

DispatchLatencyAggregate::DispatchLatencyAggregate()
    : histogram_(kHistogramBucketCount, 0U) {}

void DispatchLatencyAggregate::Add(
    const DispatchLatencyWindow& window) noexcept {
    overwritten_ += window.overwritten;
    clock_errors_ += window.clock_errors;
    for (const DispatchLatencySample& sample : window.samples) {
        ++count_;
        sum_ns_ += static_cast<long double>(sample.latency_ns);
        maximum_ns_ = std::max(maximum_ns_, sample.latency_ns);
        ++histogram_[HistogramIndex(sample.latency_ns / 1'000U)];
    }
}

std::uint64_t DispatchLatencyAggregate::count() const noexcept {
    return count_;
}

std::uint64_t DispatchLatencyAggregate::overwritten() const noexcept {
    return overwritten_;
}

std::uint64_t DispatchLatencyAggregate::clock_errors() const noexcept {
    return clock_errors_;
}

double DispatchLatencyAggregate::average_us() const noexcept {
    if (count_ == 0U) {
        return 0.0;
    }
    return static_cast<double>(
               sum_ns_ / static_cast<long double>(count_)) /
        1'000.0;
}

double DispatchLatencyAggregate::maximum_us() const noexcept {
    return static_cast<double>(maximum_ns_) / 1'000.0;
}

double DispatchLatencyAggregate::PercentileUs(
    std::uint64_t percentile) const noexcept {
    if (count_ == 0U || percentile == 0U || percentile > 100U) {
        return 0.0;
    }
    const std::uint64_t target = PercentileRank(count_, percentile);
    std::uint64_t cumulative = 0U;
    for (std::size_t index = 0U; index < histogram_.size(); ++index) {
        cumulative += histogram_[index];
        if (cumulative >= target) {
            return HistogramBucketMidpoint(index);
        }
    }
    return maximum_us();
}

void ObserveDispatchLatency(std::uint64_t ingress_sequence,
                            std::uint64_t receive_monotonic_ns,
                            std::uint64_t now_monotonic_ns,
                            DispatchLatencySampler* sampler) noexcept {
    if (sampler == nullptr ||
        !ShouldSampleDispatchLatency(ingress_sequence)) {
        return;
    }
    if (now_monotonic_ns < receive_monotonic_ns) {
        sampler->clock_errors.fetch_add(1U, std::memory_order_relaxed);
        return;
    }

    const std::uint64_t published_sequence = sampler->next_sequence + 1U;
    DispatchLatencySampleSlot& slot = sampler->samples[
        static_cast<std::size_t>(
            sampler->next_sequence % kDispatchLatencyRingCapacity)];
    if (!slot.busy.test_and_set(std::memory_order_acquire)) {
        slot.sequence = published_sequence;
        slot.latency_ns = now_monotonic_ns - receive_monotonic_ns;
        slot.busy.clear(std::memory_order_release);
    }
    sampler->next_sequence = published_sequence;
    sampler->published.store(published_sequence, std::memory_order_release);
}

DispatchLatencyWindow CollectDispatchLatency(
    DispatchLatencySampler* samplers,
    std::size_t sampler_count,
    std::vector<std::uint64_t>* cursors,
    std::vector<std::uint64_t>* clock_error_cursors) {
    DispatchLatencyWindow window;
    if (samplers == nullptr || cursors == nullptr ||
        clock_error_cursors == nullptr || cursors->size() != sampler_count ||
        clock_error_cursors->size() != sampler_count) {
        return window;
    }
    for (std::size_t index = 0U; index < sampler_count; ++index) {
        DispatchLatencySampler& sampler = samplers[index];
        const std::uint64_t end =
            sampler.published.load(std::memory_order_acquire);
        std::uint64_t begin = (*cursors)[index];
        if (end < begin) {
            begin = end;
        }
        if (end - begin > kDispatchLatencyRingCapacity) {
            window.overwritten +=
                end - begin - kDispatchLatencyRingCapacity;
            begin = end - kDispatchLatencyRingCapacity;
        }
        for (std::uint64_t sequence = begin; sequence < end; ++sequence) {
            DispatchLatencySampleSlot& slot = sampler.samples[
                static_cast<std::size_t>(
                    sequence % kDispatchLatencyRingCapacity)];
            if (slot.busy.test_and_set(std::memory_order_acquire)) {
                ++window.overwritten;
                continue;
            }
            const DispatchLatencySample sample{
                slot.sequence,
                slot.latency_ns,
            };
            slot.busy.clear(std::memory_order_release);
            if (sample.sequence == sequence + 1U) {
                window.samples.push_back(sample);
            } else {
                ++window.overwritten;
            }
        }
        (*cursors)[index] = end;

        const std::uint64_t clock_errors =
            sampler.clock_errors.load(std::memory_order_acquire);
        const std::uint64_t previous_clock_errors =
            (*clock_error_cursors)[index];
        if (clock_errors >= previous_clock_errors) {
            window.clock_errors += clock_errors - previous_clock_errors;
        }
        (*clock_error_cursors)[index] = clock_errors;
    }
    return window;
}

DispatchLatencySummary SummarizeDispatchLatency(
    std::vector<DispatchLatencySample>* samples) {
    DispatchLatencySummary summary;
    if (samples == nullptr || samples->empty()) {
        return summary;
    }
    std::sort(samples->begin(), samples->end(),
              [](const DispatchLatencySample& left,
                 const DispatchLatencySample& right) {
                  return left.latency_ns < right.latency_ns;
              });
    summary.count = samples->size();
    long double sum = 0.0L;
    for (const DispatchLatencySample& sample : *samples) {
        sum += static_cast<long double>(sample.latency_ns);
    }
    summary.average_us = static_cast<double>(
        sum / static_cast<long double>(summary.count)) / 1'000.0;
    const auto percentile = [samples](std::size_t percent) {
        const std::size_t rank =
            (samples->size() / 100U) * percent +
            ((samples->size() % 100U) * percent + 99U) / 100U - 1U;
        return static_cast<double>((*samples)[rank].latency_ns) / 1'000.0;
    };
    summary.p50_us = percentile(50U);
    summary.p95_us = percentile(95U);
    summary.p99_us = percentile(99U);
    summary.maximum_us =
        static_cast<double>(samples->back().latency_ns) / 1'000.0;
    return summary;
}

}  // namespace l2flow::ingest
