cmake_minimum_required(VERSION 3.20)

project(L2FlowCH VERSION 0.1.0 LANGUAGES CXX)

option(L2FLOW_CH_BUILD_TESTS "Build deterministic ingest tests" ON)
option(L2FLOW_CH_BUILD_BENCHMARKS
       "Build the synthetic ingest and optional Arrow end-to-end benchmark" ON)
option(L2FLOW_CH_ENABLE_ARROW_RING
       "Build the Linux shared-memory Apache Arrow hot path" ON)
option(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW
       "Build the batched raw Tick/Snapshot ClickHouse sink" ON)
option(L2FLOW_CH_ENABLE_ASAN_UBSAN "Enable ASan and UBSan" OFF)
option(L2FLOW_CH_ENABLE_TSAN "Enable TSan" OFF)
set(L2FLOW_CH_PYTHON_EXECUTABLE "" CACHE FILEPATH
    "Python with PyArrow 25 and Polars for the cross-language ring test")

set(L2FLOW_CH_SDK_INCLUDE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/../L2Flow/mdl_sdk_2_13_234/include"
    CACHE PATH "Directory containing MDL SDK 2.13.234 headers")

if(NOT EXISTS "${L2FLOW_CH_SDK_INCLUDE_DIR}/mdl_api.h")
    message(FATAL_ERROR
        "L2FLOW_CH_SDK_INCLUDE_DIR must point at the MDL 2.13.234 include directory")
endif()

if(L2FLOW_CH_ENABLE_ASAN_UBSAN AND L2FLOW_CH_ENABLE_TSAN)
    message(FATAL_ERROR "ASan/UBSan and TSan are separate build modes")
endif()

find_package(Threads REQUIRED)
if(L2FLOW_CH_ENABLE_ARROW_RING OR L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
    find_package(Arrow 25.0.0 EXACT CONFIG REQUIRED)
endif()
if(L2FLOW_CH_ENABLE_ARROW_RING)
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(FATAL_ERROR
            "L2FLOW_CH_ENABLE_ARROW_RING currently requires Linux mmap/process APIs")
    endif()
endif()
if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
    find_package(CURL REQUIRED)
endif()

set(L2FLOW_CH_WARNINGS)
if(MSVC)
    set(L2FLOW_CH_WARNINGS /W4 /permissive- /WX)
else()
    set(L2FLOW_CH_WARNINGS
        -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
endif()

set(L2FLOW_CH_SANITIZER_FLAGS)
if(L2FLOW_CH_ENABLE_ASAN_UBSAN)
    set(L2FLOW_CH_SANITIZER_FLAGS
        -fno-omit-frame-pointer -fsanitize=address,undefined)
elseif(L2FLOW_CH_ENABLE_TSAN)
    set(L2FLOW_CH_SANITIZER_FLAGS
        -fno-omit-frame-pointer -fsanitize=thread)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        list(APPEND L2FLOW_CH_SANITIZER_FLAGS -Wno-error=tsan)
    endif()
endif()

function(l2flow_ch_configure target)
    set_target_properties(${target} PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
    target_include_directories(${target}
        PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>")
    target_include_directories(${target}
        SYSTEM PUBLIC "$<BUILD_INTERFACE:${L2FLOW_CH_SDK_INCLUDE_DIR}>")
    target_compile_options(${target} PRIVATE
        ${L2FLOW_CH_WARNINGS} ${L2FLOW_CH_SANITIZER_FLAGS})
    target_link_options(${target} PRIVATE ${L2FLOW_CH_SANITIZER_FLAGS})
endfunction()

add_library(l2flow_ch_ingest STATIC
    src/catalog.cpp
    src/decoder.cpp
    src/engine.cpp
    src/stream_config.cpp
    src/sdk_runtime.cpp)
add_library(L2FlowCH::ingest ALIAS l2flow_ch_ingest)
l2flow_ch_configure(l2flow_ch_ingest)
target_link_libraries(l2flow_ch_ingest PUBLIC Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(l2flow_ch_ingest PUBLIC ${CMAKE_DL_LIBS})
    target_compile_definitions(l2flow_ch_ingest PRIVATE _GNU_SOURCE)
endif()

add_library(l2flow_ch_journal STATIC
    src/fact_journal.cpp)
add_library(L2FlowCH::journal ALIAS l2flow_ch_journal)
l2flow_ch_configure(l2flow_ch_journal)
target_link_libraries(l2flow_ch_journal PUBLIC Threads::Threads)

if(L2FLOW_CH_ENABLE_ARROW_RING OR L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
    add_library(l2flow_ch_arrow_schemas STATIC
        src/arrow_schemas.cpp)
    add_library(L2FlowCH::arrow_schemas ALIAS l2flow_ch_arrow_schemas)
    l2flow_ch_configure(l2flow_ch_arrow_schemas)
    set_target_properties(l2flow_ch_arrow_schemas PROPERTIES
        POSITION_INDEPENDENT_CODE ON)
    target_link_libraries(l2flow_ch_arrow_schemas PUBLIC Arrow::arrow_shared)
endif()

if(L2FLOW_CH_ENABLE_ARROW_RING)
    add_library(l2flow_ch_arrow SHARED
        src/arrow_c_api.cpp
        src/arrow_egress.cpp
        src/arrow_ring.cpp)
    add_library(L2FlowCH::arrow ALIAS l2flow_ch_arrow)
    l2flow_ch_configure(l2flow_ch_arrow)
    target_link_libraries(l2flow_ch_arrow
        PUBLIC L2FlowCH::arrow_schemas Arrow::arrow_shared Threads::Threads)
endif()

if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
    add_library(l2flow_ch_clickhouse STATIC
        src/blake3.cpp
        src/clickhouse_event_sink.cpp
        src/clickhouse_kline_sink.cpp
        src/clickhouse_raw_sink.cpp
        src/event_runtime.cpp
        src/event_worker.cpp
        src/kline_runtime.cpp
        src/kline_worker.cpp)
    add_library(L2FlowCH::clickhouse ALIAS l2flow_ch_clickhouse)
    l2flow_ch_configure(l2flow_ch_clickhouse)
    target_link_libraries(l2flow_ch_clickhouse
        PUBLIC L2FlowCH::ingest L2FlowCH::journal L2FlowCH::arrow_schemas
               Arrow::arrow_shared CURL::libcurl Threads::Threads)
endif()

add_executable(mdl_ingestd apps/mdl_ingestd.cpp)
l2flow_ch_configure(mdl_ingestd)
target_link_libraries(mdl_ingestd PRIVATE L2FlowCH::ingest)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(mdl_ingestd PRIVATE _GNU_SOURCE)
endif()
if(L2FLOW_CH_ENABLE_ARROW_RING)
    target_link_libraries(mdl_ingestd PRIVATE L2FlowCH::arrow)
    target_compile_definitions(mdl_ingestd PRIVATE
        L2FLOW_CH_HAS_ARROW_RING=1)
endif()
if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
    target_link_libraries(mdl_ingestd PRIVATE L2FlowCH::clickhouse)
    target_compile_definitions(mdl_ingestd PRIVATE
        L2FLOW_CH_HAS_CLICKHOUSE_RAW=1)
endif()

if(L2FLOW_CH_BUILD_TESTS)
    enable_testing()
    add_executable(test_mdl_ingest tests/test_mdl_ingest.cpp)
    l2flow_ch_configure(test_mdl_ingest)
    target_link_libraries(test_mdl_ingest PRIVATE L2FlowCH::ingest)
    add_test(NAME mdl_ingest_core COMMAND test_mdl_ingest)

    add_executable(test_fact_journal tests/test_fact_journal.cpp)
    l2flow_ch_configure(test_fact_journal)
    target_link_libraries(test_fact_journal PRIVATE L2FlowCH::journal)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR
        CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
        target_compile_definitions(test_fact_journal PRIVATE
            L2FLOW_TEST_WRAP_PWRITE=1)
        target_link_options(test_fact_journal PRIVATE
            -Wl,--wrap=pwrite -Wl,--wrap=pread -Wl,--wrap=fdatasync)
    endif()
    add_test(NAME fact_journal COMMAND test_fact_journal)

    add_executable(test_event_lazy_paged_hash
        tests/test_event_lazy_paged_hash.cpp)
    l2flow_ch_configure(test_event_lazy_paged_hash)
    target_include_directories(test_event_lazy_paged_hash PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME event_lazy_paged_hash COMMAND test_event_lazy_paged_hash)

    add_executable(test_event_exact_list
        tests/test_event_exact_list.cpp)
    l2flow_ch_configure(test_event_exact_list)
    target_include_directories(test_event_exact_list PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME event_exact_list COMMAND test_event_exact_list)

    if(L2FLOW_CH_ENABLE_ARROW_RING)
        add_executable(test_arrow_ring tests/test_arrow_ring.cpp)
        l2flow_ch_configure(test_arrow_ring)
        target_link_libraries(test_arrow_ring PRIVATE L2FlowCH::arrow)
        add_test(NAME arrow_ring COMMAND test_arrow_ring)

        add_executable(arrow_ring_fixture tests/arrow_ring_fixture.cpp)
        l2flow_ch_configure(arrow_ring_fixture)
        target_link_libraries(arrow_ring_fixture PRIVATE L2FlowCH::arrow)

        if(L2FLOW_CH_PYTHON_EXECUTABLE)
            if(NOT EXISTS "${L2FLOW_CH_PYTHON_EXECUTABLE}")
                message(FATAL_ERROR
                    "L2FLOW_CH_PYTHON_EXECUTABLE does not exist")
            endif()
            add_test(
                NAME arrow_ring_python
                COMMAND "${L2FLOW_CH_PYTHON_EXECUTABLE}"
                    ${CMAKE_CURRENT_SOURCE_DIR}/python/test_reader.py
                    $<TARGET_FILE:arrow_ring_fixture>
                    $<TARGET_FILE:l2flow_ch_arrow>)
        endif()
    endif()
    if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
        add_executable(test_event_worker tests/test_event_worker.cpp)
        l2flow_ch_configure(test_event_worker)
        target_link_libraries(test_event_worker PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME event_worker COMMAND test_event_worker)

        add_executable(test_kline_worker tests/test_kline_worker.cpp)
        l2flow_ch_configure(test_kline_worker)
        target_link_libraries(test_kline_worker PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME kline_worker COMMAND test_kline_worker)

        add_executable(test_clickhouse_event tests/test_clickhouse_event.cpp)
        l2flow_ch_configure(test_clickhouse_event)
        target_link_libraries(test_clickhouse_event PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME clickhouse_event COMMAND test_clickhouse_event)

        add_executable(test_clickhouse_kline tests/test_clickhouse_kline.cpp)
        l2flow_ch_configure(test_clickhouse_kline)
        target_link_libraries(test_clickhouse_kline PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME clickhouse_kline COMMAND test_clickhouse_kline)

        add_executable(test_clickhouse_raw tests/test_clickhouse_raw.cpp)
        l2flow_ch_configure(test_clickhouse_raw)
        target_link_libraries(test_clickhouse_raw PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME clickhouse_raw COMMAND test_clickhouse_raw)
        add_test(
            NAME mdl_ingest_clickhouse_config
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --clickhouse-feed-epoch 42
                --clickhouse-source-instance-id
                    000102030405060708090a0b0c0d0e0f
                --clickhouse-queue-batches-per-lane 2
                --validate-only)
        add_test(
            NAME mdl_ingest_clickhouse_requires_epoch
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --validate-only)
        set_tests_properties(
            mdl_ingest_clickhouse_requires_epoch PROPERTIES WILL_FAIL TRUE)
        add_test(
            NAME mdl_ingest_event_config
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --clickhouse-feed-epoch 42
                --event-enable
                --fact-journal-path /tmp/l2flow-event-config.fjn
                --fact-journal-maximum-directory-pages 16
                --event-revision-epoch 9
                --event-writer-lanes 4
                --event-calculation-run-id
                    000102030405060708090a0b0c0d0e0f
                --validate-only)
        add_test(
            NAME mdl_ingest_event_requires_revision_epoch
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --clickhouse-feed-epoch 42
                --event-enable
                --event-calculation-run-id
                    000102030405060708090a0b0c0d0e0f
                --validate-only)
        set_tests_properties(
            mdl_ingest_event_requires_revision_epoch
            PROPERTIES WILL_FAIL TRUE)
        add_test(
            NAME mdl_ingest_rejects_removed_event_fact_capacity
            COMMAND ${CMAKE_COMMAND}
                -DPROGRAM=$<TARGET_FILE:mdl_ingestd>
                -DARGUMENT=--event-maximum-facts
                -DVALUE=4194304
                "-DEXPECTED=unknown option: --event-maximum-facts"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/expect_command_failure.cmake)
        add_test(
            NAME mdl_ingest_kline_config
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --clickhouse-feed-epoch 42
                --kline-enable
                --fact-journal-path /tmp/l2flow-kline-config.fjn
                --kline-interval-seconds 5
                --kline-interval-seconds 1
                --kline-interval-seconds 5
                --kline-revision-epoch 10
                --kline-writer-lanes 4
                --kline-calculation-run-id
                    101112131415161718191a1b1c1d1e1f
                --validate-only)
        add_test(
            NAME mdl_ingest_kline_requires_revision_epoch
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --stream-config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/production.streams.conf
                --clickhouse-url http://127.0.0.1:8123
                --clickhouse-feed-epoch 42
                --kline-enable
                --kline-interval-seconds 5
                --kline-calculation-run-id
                    101112131415161718191a1b1c1d1e1f
                --validate-only)
        set_tests_properties(
            mdl_ingest_kline_requires_revision_epoch
            PROPERTIES WILL_FAIL TRUE)
        add_test(
            NAME mdl_ingest_rejects_removed_kline_fact_capacity
            COMMAND ${CMAKE_COMMAND}
                -DPROGRAM=$<TARGET_FILE:mdl_ingestd>
                -DARGUMENT=--kline-maximum-facts
                -DVALUE=4194304
                "-DEXPECTED=unknown option: --kline-maximum-facts"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/expect_command_failure.cmake)
    endif()
    add_test(
        NAME mdl_ingest_default_config
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --validate-only)
    add_test(
        NAME mdl_ingest_test_operation_mode
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --operation-mode test
            --run-seconds 1
            --validate-only)
    add_test(
        NAME mdl_ingest_custom_ready_timeout
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --sdk-ready-timeout-seconds 1
            --validate-only)
    add_test(
        NAME mdl_ingest_dispatch_queue_capacity
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --dispatch-queue-capacity 65536
            --validate-only)
    add_test(
        NAME mdl_ingest_reorder_window_boundary
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --reorder-entries-per-channel 8
            --maximum-reorder-span 8
            --validate-only)
    add_test(
        NAME mdl_ingest_rejects_reorder_span_over_capacity
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --reorder-entries-per-channel 8
            --maximum-reorder-span 9
            --validate-only)
    add_test(
        NAME mdl_ingest_rejects_non_power_of_two_reorder_capacity
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --reorder-entries-per-channel 7
            --maximum-reorder-span 7
            --validate-only)
    add_test(
        NAME mdl_ingest_rejects_zero_ready_timeout
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --sdk-ready-timeout-seconds 0
            --validate-only)
    if(L2FLOW_CH_ENABLE_ARROW_RING)
        add_test(
            NAME mdl_ingest_rejects_zero_arrow_epoch
            COMMAND mdl_ingestd
                --mode partial
                --trade-date 20260806
                --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                --arrow-ring-dir /tmp/l2flow-arrow-validate-only
                --arrow-feed-epoch 0
                --validate-only)
        set_tests_properties(
            mdl_ingest_rejects_zero_arrow_epoch PROPERTIES WILL_FAIL TRUE)
    endif()
    add_test(
        NAME mdl_ingest_live_rejects_run_seconds
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260806
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --run-seconds 1
            --validate-only)
    add_test(
        NAME mdl_ingest_config_file
        COMMAND ${CMAKE_COMMAND} -E env
            L2FLOW_CONFIG_TEST_TRADE_DATE=20260808
            L2FLOW_CONFIG_TEST_CATALOG=${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            $<TARGET_FILE:mdl_ingestd>
            --operation-mode test
            --config
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/config/mdl_ingestd-valid.conf
            --validate-only)
    add_test(
        NAME mdl_ingest_config_missing_environment
        COMMAND ${CMAKE_COMMAND} -E env
            --unset=L2FLOW_CONFIG_TEST_REQUIRED_UNSET
            $<TARGET_FILE:mdl_ingestd>
            --config
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/config/mdl_ingestd-missing-env.conf
            --validate-only)
    add_test(
        NAME mdl_ingest_config_rejects_malformed_line
        COMMAND mdl_ingestd
            --config
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/config/mdl_ingestd-malformed.conf
            --validate-only)
    add_test(
        NAME mdl_ingest_config_rejects_malformed_environment
        COMMAND mdl_ingestd
            --config
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/config/mdl_ingestd-malformed-env.conf
            --validate-only)
    add_test(
        NAME mdl_ingest_config_rejects_nested_config
        COMMAND mdl_ingestd
            --config
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/config/mdl_ingestd-nested.conf
            --validate-only)
    add_test(
        NAME mdl_ingest_process_cpu_list
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260808
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --process-cpus 0-1,3
            --validate-only)
    add_test(
        NAME mdl_ingest_rejects_malformed_cpu_list
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260808
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --process-cpus 3-1
            --validate-only)
    add_test(
        NAME mdl_ingest_rejects_decoder_outside_process_cpus
        COMMAND mdl_ingestd
            --mode partial
            --trade-date 20260808
            --catalog ${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
            --tick-lanes 2
            --snapshot-lanes 1
            --first-decoder-cpu 0
            --process-cpus 0-1
            --validate-only)
    if(L2FLOW_CH_ENABLE_ARROW_RING AND L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
        add_test(
            NAME mdl_ingest_current_server_production_config
            COMMAND ${CMAKE_COMMAND} -E env
                L2FLOW_START_MODE=partial
                L2FLOW_TRADE_DATE=20260808
                L2FLOW_CATALOG=${CMAKE_CURRENT_SOURCE_DIR}/config/catalog.example.csv
                L2FLOW_MDL_USER=config-test
                L2FLOW_CLICKHOUSE_USER=default
                L2FLOW_CLICKHOUSE_PASSWORD=
                L2FLOW_FACT_JOURNAL_PATH=/tmp/l2flow-production-config.fjn
                L2FLOW_SOURCE_INSTANCE_HEX32=000102030405060708090a0b0c0d0e0f
                L2FLOW_RAW_FEED_EPOCH=42
                L2FLOW_EVENT_REVISION_EPOCH=9
                L2FLOW_EVENT_RUN_HEX32=101112131415161718191a1b1c1d1e1f
                L2FLOW_KLINE_INTERVAL_SECONDS=5
                L2FLOW_KLINE_REVISION_EPOCH=10
                L2FLOW_KLINE_RUN_HEX32=202122232425262728292a2b2c2d2e2f
                $<TARGET_FILE:mdl_ingestd>
                --config
                    ${CMAKE_CURRENT_SOURCE_DIR}/config/current-server.production.conf
                --validate-only)
    endif()
    set_tests_properties(
        mdl_ingest_live_rejects_run_seconds
        mdl_ingest_rejects_zero_ready_timeout
        mdl_ingest_rejects_reorder_span_over_capacity
        mdl_ingest_rejects_non_power_of_two_reorder_capacity
        mdl_ingest_config_missing_environment
        mdl_ingest_config_rejects_malformed_line
        mdl_ingest_config_rejects_malformed_environment
        mdl_ingest_config_rejects_nested_config
        mdl_ingest_rejects_malformed_cpu_list
        mdl_ingest_rejects_decoder_outside_process_cpus
        PROPERTIES WILL_FAIL TRUE)
    set_tests_properties(
        mdl_ingest_default_config
        mdl_ingest_test_operation_mode
        mdl_ingest_custom_ready_timeout
        mdl_ingest_dispatch_queue_capacity
        mdl_ingest_reorder_window_boundary
        mdl_ingest_rejects_reorder_span_over_capacity
        mdl_ingest_rejects_non_power_of_two_reorder_capacity
        mdl_ingest_config_file
        mdl_ingest_process_cpu_list
        PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    if(L2FLOW_CH_ENABLE_ARROW_RING AND L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
        set_tests_properties(
            mdl_ingest_current_server_production_config
            PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    endif()
endif()

if(L2FLOW_CH_BUILD_BENCHMARKS)
    add_executable(benchmark_fact_journal
        benchmarks/benchmark_fact_journal.cpp)
    l2flow_ch_configure(benchmark_fact_journal)
    target_link_libraries(benchmark_fact_journal PRIVATE
        L2FlowCH::journal)
    if(L2FLOW_CH_BUILD_TESTS)
        add_test(
            NAME fact_journal_benchmark_smoke
            COMMAND benchmark_fact_journal
                --rate 10000 --seconds 1 --batch-rows 128 --owners 4
                --journal-dir /tmp --hot-cache 4096 --probe-samples 64
                --mixed-read-rate 100 --seed-records 16)
    endif()

    add_executable(benchmark_mdl_ingest benchmarks/benchmark_mdl_ingest.cpp)
    l2flow_ch_configure(benchmark_mdl_ingest)
    target_link_libraries(benchmark_mdl_ingest PRIVATE L2FlowCH::ingest)
    if(L2FLOW_CH_ENABLE_ARROW_RING)
        target_link_libraries(benchmark_mdl_ingest PRIVATE L2FlowCH::arrow)
        target_compile_definitions(benchmark_mdl_ingest PRIVATE
            L2FLOW_CH_HAS_ARROW_RING=1)
    endif()
    if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
        target_link_libraries(benchmark_mdl_ingest PRIVATE
            L2FlowCH::clickhouse)
        target_compile_definitions(benchmark_mdl_ingest PRIVATE
            L2FLOW_CH_HAS_CLICKHOUSE_RAW=1)
        if(L2FLOW_CH_BUILD_TESTS)
            add_test(
                NAME mdl_ingest_event_benchmark_construction_smoke
                COMMAND benchmark_mdl_ingest
                    --rate 1 --seconds 1 --warmup-seconds 0
                    --channels 1 --tick-lanes 1 --owners 1
                    --producer-cpu 0 --first-consumer-cpu 1
                    --first-decoder-cpu 2
                    --clickhouse-url http://127.0.0.1:8123
                    --clickhouse-feed-epoch 1 --event-enable
                    --event-journal-dir /tmp
                    --event-construction-smoke)
        endif()
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_compile_definitions(benchmark_mdl_ingest PRIVATE _GNU_SOURCE)
    endif()
    if(L2FLOW_CH_ENABLE_CLICKHOUSE_RAW)
        add_executable(benchmark_event_state
            benchmarks/benchmark_event_state.cpp)
        l2flow_ch_configure(benchmark_event_state)
        target_link_libraries(benchmark_event_state PRIVATE
            L2FlowCH::clickhouse)

        add_executable(benchmark_event_sink
            benchmarks/benchmark_event_sink.cpp)
        l2flow_ch_configure(benchmark_event_sink)
        target_link_libraries(benchmark_event_sink PRIVATE
            L2FlowCH::clickhouse)
        if(L2FLOW_CH_BUILD_TESTS)
            add_test(
                NAME event_state_benchmark_smoke
                COMMAND benchmark_event_state
                    --rate 1000 --seconds 1 --actors 1
                    --micro-batch-rows 32)
        endif()
    endif()
endif()
