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
        src/clickhouse_raw_sink.cpp
        src/event_runtime.cpp
        src/event_worker.cpp)
    add_library(L2FlowCH::clickhouse ALIAS l2flow_ch_clickhouse)
    l2flow_ch_configure(l2flow_ch_clickhouse)
    target_link_libraries(l2flow_ch_clickhouse
        PUBLIC L2FlowCH::ingest L2FlowCH::arrow_schemas
               Arrow::arrow_shared CURL::libcurl Threads::Threads)
endif()

add_executable(mdl_ingestd apps/mdl_ingestd.cpp)
l2flow_ch_configure(mdl_ingestd)
target_link_libraries(mdl_ingestd PRIVATE L2FlowCH::ingest)
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

        add_executable(test_clickhouse_event tests/test_clickhouse_event.cpp)
        l2flow_ch_configure(test_clickhouse_event)
        target_link_libraries(test_clickhouse_event PRIVATE
            L2FlowCH::clickhouse)
        add_test(NAME clickhouse_event COMMAND test_clickhouse_event)

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
    set_tests_properties(
        mdl_ingest_live_rejects_run_seconds
        mdl_ingest_rejects_zero_ready_timeout PROPERTIES WILL_FAIL TRUE)
    set_tests_properties(
        mdl_ingest_default_config
        mdl_ingest_test_operation_mode
        mdl_ingest_custom_ready_timeout
        PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()

if(L2FLOW_CH_BUILD_BENCHMARKS)
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
        if(L2FLOW_CH_BUILD_TESTS)
            add_test(
                NAME event_state_benchmark_smoke
                COMMAND benchmark_event_state
                    --rate 1000 --seconds 1 --actors 1
                    --micro-batch-rows 32)
        endif()
    endif()
endif()
