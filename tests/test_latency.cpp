#include "l2flow/ingest/latency.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

namespace {

using l2flow::ingest::CollectDispatchLatency;
using l2flow::ingest::DispatchLatencyAggregate;
using l2flow::ingest::DispatchLatencySample;
using l2flow::ingest::DispatchLatencySampler;
using l2flow::ingest::DispatchLatencyWindow;
using l2flow::ingest::ObserveDispatchLatency;
using l2flow::ingest::kDispatchLatencyRingCapacity;
using l2flow::ingest::kDispatchLatencySampleEvery;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

DispatchLatencyWindow CollectOne(DispatchLatencySampler* sampler,
                                 std::uint64_t* cursor,
                                 std::uint64_t* clock_error_cursor) {
    std::vector<std::uint64_t> cursors{*cursor};
    std::vector<std::uint64_t> clock_error_cursors{*clock_error_cursor};
    DispatchLatencyWindow window = CollectDispatchLatency(
        sampler, 1U, &cursors, &clock_error_cursors);
    *cursor = cursors[0U];
    *clock_error_cursor = clock_error_cursors[0U];
    return window;
}

void TestSamplingAndClockErrors() {
    DispatchLatencySampler sampler{};
    std::uint64_t cursor = 0U;
    std::uint64_t clock_error_cursor = 0U;
    ObserveDispatchLatency(
        kDispatchLatencySampleEvery - 1U, 10U, 20U, &sampler);
    ObserveDispatchLatency(
        kDispatchLatencySampleEvery, 10U, 60U, &sampler);
    ObserveDispatchLatency(
        kDispatchLatencySampleEvery * 2U, 60U, 50U, &sampler);
    DispatchLatencyWindow window =
        CollectOne(&sampler, &cursor, &clock_error_cursor);
    CHECK(window.samples.size() == 1U);
    CHECK(window.samples[0U].sequence == 1U);
    CHECK(window.samples[0U].latency_ns == 50U);
    CHECK(window.overwritten == 0U);
    CHECK(window.clock_errors == 1U);
}

void TestOverwriteAccounting() {
    DispatchLatencySampler sampler{};
    constexpr std::uint64_t kExtra = 17U;
    const std::uint64_t total =
        static_cast<std::uint64_t>(kDispatchLatencyRingCapacity) + kExtra;
    for (std::uint64_t sequence = 1U; sequence <= total; ++sequence) {
        ObserveDispatchLatency(
            sequence * kDispatchLatencySampleEvery,
            0U, sequence, &sampler);
    }
    std::uint64_t cursor = 0U;
    std::uint64_t clock_error_cursor = 0U;
    const DispatchLatencyWindow window =
        CollectOne(&sampler, &cursor, &clock_error_cursor);
    CHECK(window.overwritten == kExtra);
    CHECK(window.samples.size() == kDispatchLatencyRingCapacity);
    for (const DispatchLatencySample& sample : window.samples) {
        CHECK(sample.sequence == sample.latency_ns);
    }
}

void TestConcurrentWrapNeverPairsDifferentSequences() {
    DispatchLatencySampler sampler{};
    constexpr std::uint64_t kSamples = 250'000U;
    std::atomic<bool> done{false};
    std::thread writer([&] {
        for (std::uint64_t sequence = 1U;
             sequence <= kSamples; ++sequence) {
            ObserveDispatchLatency(
                sequence * kDispatchLatencySampleEvery,
                0U, sequence, &sampler);
        }
        done.store(true, std::memory_order_release);
    });

    std::uint64_t cursor = 0U;
    std::uint64_t clock_error_cursor = 0U;
    std::uint64_t accepted = 0U;
    std::uint64_t overwritten = 0U;
    const auto collect = [&] {
        DispatchLatencyWindow window =
            CollectOne(&sampler, &cursor, &clock_error_cursor);
        for (const DispatchLatencySample& sample : window.samples) {
            CHECK(sample.sequence == sample.latency_ns);
        }
        accepted += static_cast<std::uint64_t>(window.samples.size());
        overwritten += window.overwritten;
    };
    while (!done.load(std::memory_order_acquire)) {
        collect();
        std::this_thread::yield();
    }
    writer.join();
    collect();
    CHECK(accepted != 0U);
    CHECK(accepted + overwritten == kSamples);
    CHECK(clock_error_cursor == 0U);
}

void TestAggregateHasNoUpperLatencyClip() {
    DispatchLatencyWindow window;
    window.samples = {
        {1U, 200'000'000U},
        {2U, 500'000'000U},
        {3U, 2'000'000'000U},
    };
    DispatchLatencyAggregate aggregate;
    aggregate.Add(window);
    CHECK(aggregate.count() == 3U);
    CHECK(aggregate.PercentileUs(50U) > 400'000.0);
    CHECK(aggregate.PercentileUs(99U) > 1'900'000.0);
    CHECK(aggregate.maximum_us() == 2'000'000.0);

    DispatchLatencyWindow extreme_window;
    extreme_window.samples = {
        {1U, std::numeric_limits<std::uint64_t>::max()},
    };
    DispatchLatencyAggregate extreme;
    extreme.Add(extreme_window);
    CHECK(extreme.PercentileUs(99U) > 1.0e16);
}

}  // namespace

int main() {
    TestSamplingAndClockErrors();
    TestOverwriteAccounting();
    TestConcurrentWrapNeverPairsDifferentSequences();
    TestAggregateHasNoUpperLatencyClip();
    std::cout << "test_latency: ok\n";
    return 0;
}
