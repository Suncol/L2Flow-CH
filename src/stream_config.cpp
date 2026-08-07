#include "l2flow/ingest/message.h"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingest {
namespace {

void SetError(std::string* error, std::string value) {
    if (error != nullptr) {
        *error = std::move(value);
    }
}

[[nodiscard]] std::string_view Trim(std::string_view value) noexcept {
    constexpr std::string_view whitespace = " \t";
    const std::size_t begin = value.find_first_not_of(whitespace);
    if (begin == std::string_view::npos) {
        return {};
    }
    const std::size_t end = value.find_last_not_of(whitespace);
    return value.substr(begin, end - begin + 1U);
}

template <typename Unsigned>
[[nodiscard]] bool ParseUnsigned(std::string_view value,
                                 Unsigned* output) noexcept {
    if (output == nullptr || value.empty()) {
        return false;
    }
    std::uint32_t parsed = 0U;
    const auto result = std::from_chars(
        value.data(), value.data() + value.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != value.data() + value.size() ||
        parsed > static_cast<std::uint32_t>(
                     std::numeric_limits<Unsigned>::max())) {
        return false;
    }
    *output = static_cast<Unsigned>(parsed);
    return true;
}

[[nodiscard]] bool ParseTuple(std::string_view line,
                              MessageKey* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const std::size_t first_dot = line.find('.');
    if (first_dot == std::string_view::npos) {
        return false;
    }
    const std::size_t second_dot = line.find('.', first_dot + 1U);
    if (second_dot == std::string_view::npos ||
        line.find('.', second_dot + 1U) != std::string_view::npos) {
        return false;
    }
    MessageKey key{};
    if (!ParseUnsigned(line.substr(0U, first_dot), &key.service_id) ||
        !ParseUnsigned(
            line.substr(first_dot + 1U, second_dot - first_dot - 1U),
            &key.service_version) ||
        !ParseUnsigned(line.substr(second_dot + 1U), &key.message_id)) {
        return false;
    }
    *output = key;
    return true;
}

}  // namespace

bool LoadStreamConfig(const std::filesystem::path& path,
                      StreamMask* output,
                      std::string* error) {
    if (output == nullptr) {
        SetError(error, "stream config output is null");
        return false;
    }
    try {
        std::ifstream stream(path);
        if (!stream) {
            SetError(error, "cannot open stream config: " + path.string());
            return false;
        }
        StreamMask mask = 0U;
        std::string raw_line;
        std::size_t line_number = 0U;
        while (std::getline(stream, raw_line)) {
            ++line_number;
            if (!raw_line.empty() && raw_line.back() == '\r') {
                raw_line.pop_back();
            }
            const std::string_view line = Trim(raw_line);
            if (line.empty() || line.front() == '#') {
                continue;
            }
            MessageKey key{};
            if (!ParseTuple(line, &key)) {
                SetError(error, "invalid stream tuple at line " +
                                    std::to_string(line_number));
                return false;
            }
            const StreamMask bit = StreamBit(key);
            if (bit == 0U) {
                SetError(error,
                         "stream tuple has no decoder in this build at line " +
                             std::to_string(line_number));
                return false;
            }
            if ((mask & bit) != 0U) {
                SetError(error, "duplicate stream tuple at line " +
                                    std::to_string(line_number));
                return false;
            }
            mask |= bit;
        }
        if (!stream.eof()) {
            SetError(error, "stream config read failed");
            return false;
        }
        if (mask == 0U) {
            SetError(error, "stream config selects no data streams");
            return false;
        }
        *output = mask;
        SetError(error, {});
        return true;
    } catch (const std::exception& exception) {
        SetError(error, std::string("stream config load failed: ") +
                            exception.what());
        return false;
    }
}

}  // namespace l2flow::ingest
