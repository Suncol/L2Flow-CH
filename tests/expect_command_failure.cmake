foreach(required IN ITEMS PROGRAM ARGUMENT VALUE EXPECTED)
    if(NOT DEFINED ${required})
        message(FATAL_ERROR "missing -D${required}")
    endif()
endforeach()

execute_process(
    COMMAND "${PROGRAM}" "${ARGUMENT}" "${VALUE}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE standard_output
    ERROR_VARIABLE standard_error)

if("${result}" STREQUAL "0")
    message(FATAL_ERROR
        "command unexpectedly succeeded: ${PROGRAM} ${ARGUMENT} ${VALUE}")
endif()

set(combined_output "${standard_output}\n${standard_error}")
string(FIND "${combined_output}" "${EXPECTED}" expected_offset)
if(expected_offset EQUAL -1)
    message(FATAL_ERROR
        "command failed without expected text '${EXPECTED}':\n"
        "${combined_output}")
endif()
