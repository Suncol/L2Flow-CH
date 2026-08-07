#include "l2flow/ingest/catalog.h"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <exception>
#include <limits>
#include <new>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace l2flow::ingest {
namespace {

[[nodiscard]] std::uint64_t Mix(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

[[nodiscard]] std::uint64_t HashIdentity(
    Market market,
    std::span<const std::byte> source,
    std::span<const std::byte> security) noexcept {
    std::uint64_t hash = UINT64_C(1469598103934665603);
    const auto append = [&hash](std::byte value) {
        hash ^= static_cast<std::uint64_t>(
            std::to_integer<unsigned char>(value));
        hash *= UINT64_C(1099511628211);
    };
    append(static_cast<std::byte>(market));
    append(static_cast<std::byte>(source.size()));
    for (const std::byte value : source) {
        append(value);
    }
    append(static_cast<std::byte>(security.size()));
    for (const std::byte value : security) {
        append(value);
    }
    return Mix(hash);
}

[[nodiscard]] bool IsPrintableAscii(std::string_view value) noexcept {
    return std::all_of(value.begin(), value.end(), [](char character) {
        const auto octet = static_cast<unsigned char>(character);
        return octet >= 0x20U && octet <= 0x7eU;
    });
}

void SetError(std::string* error, std::string value) {
    if (error != nullptr) {
        *error = std::move(value);
    }
}

[[nodiscard]] std::vector<std::string_view> SplitCsvLine(
    const std::string& line) {
    std::vector<std::string_view> fields;
    std::size_t start = 0U;
    for (std::size_t index = 0U; index <= line.size(); ++index) {
        if (index == line.size() || line[index] == ',') {
            fields.emplace_back(line.data() + start, index - start);
            start = index + 1U;
        }
    }
    return fields;
}

}  // namespace

class InstrumentCatalog::Impl final {
public:
    struct Entry final {
        InstrumentMatch match{};
        ExactIdentity identity{};
        std::uint64_t hash = 0U;
    };

    struct Slot final {
        std::uint32_t entry_plus_one = 0U;
    };

    std::vector<Entry> entries;
    std::vector<Slot> table;
};

InstrumentCatalog::InstrumentCatalog() : impl_(std::make_unique<Impl>()) {}
InstrumentCatalog::~InstrumentCatalog() = default;
InstrumentCatalog::InstrumentCatalog(InstrumentCatalog&&) noexcept = default;
InstrumentCatalog& InstrumentCatalog::operator=(
    InstrumentCatalog&&) noexcept = default;

bool InstrumentCatalog::Build(
    std::vector<InstrumentDefinition> definitions,
    InstrumentCatalog* output,
    std::string* error) {
    if (output == nullptr) {
        SetError(error, "catalog output is null");
        return false;
    }
    if (definitions.empty()) {
        SetError(error, "catalog must contain at least one instrument");
        return false;
    }
    if (definitions.size() >=
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        SetError(error, "catalog contains too many instruments");
        return false;
    }

    try {
        InstrumentCatalog built;
        std::size_t table_size = 1U;
        while (table_size < definitions.size() * 2U) {
            table_size *= 2U;
        }
        built.impl_->entries.reserve(definitions.size());
        built.impl_->table.resize(table_size);
        std::unordered_set<std::uint32_t> ids;
        ids.reserve(definitions.size());

        for (std::size_t index = 0U; index < definitions.size(); ++index) {
            const InstrumentDefinition& definition = definitions[index];
            if (definition.instrument_id == 0U ||
                !ids.insert(definition.instrument_id).second) {
                SetError(error, "instrument_id must be nonzero and unique");
                return false;
            }
            if (definition.market != Market::kShanghai &&
                definition.market != Market::kShenzhen) {
                SetError(error, "catalog market must be SH or SZ");
                return false;
            }
            if (definition.security_id.empty() ||
                definition.security_id.size() > kMaximumIdentityBytes ||
                definition.security_id_source.size() >
                    kMaximumIdentityBytes ||
                !IsPrintableAscii(definition.security_id) ||
                !IsPrintableAscii(definition.security_id_source)) {
                SetError(error,
                         "catalog identity must be nonempty printable ASCII "
                         "within the fixed identity bound");
                return false;
            }
            if (definition.market == Market::kShanghai &&
                !definition.security_id_source.empty()) {
                SetError(error,
                         "Shanghai catalog identities require an empty "
                         "security_id_source");
                return false;
            }
            if (definition.market == Market::kShenzhen &&
                definition.security_id_source.empty()) {
                SetError(error,
                         "Shenzhen catalog identities require an exact "
                         "nonempty security_id_source");
                return false;
            }

            Impl::Entry entry{};
            entry.match.instrument_id = definition.instrument_id;
            entry.match.ordinal = static_cast<std::uint32_t>(index);
            entry.identity.market = definition.market;
            entry.identity.security_id_source_size =
                static_cast<std::uint8_t>(
                    definition.security_id_source.size());
            entry.identity.security_id_size = static_cast<std::uint8_t>(
                definition.security_id.size());
            std::copy(definition.security_id_source.begin(),
                      definition.security_id_source.end(),
                      reinterpret_cast<char*>(
                          entry.identity.security_id_source.data()));
            std::copy(definition.security_id.begin(),
                      definition.security_id.end(),
                      reinterpret_cast<char*>(
                          entry.identity.security_id.data()));
            const auto source = std::span<const std::byte>(
                entry.identity.security_id_source.data(),
                entry.identity.security_id_source_size);
            const auto security = std::span<const std::byte>(
                entry.identity.security_id.data(),
                entry.identity.security_id_size);
            entry.hash = HashIdentity(definition.market, source, security);

            const std::size_t mask = table_size - 1U;
            std::size_t slot = static_cast<std::size_t>(entry.hash) & mask;
            for (;;) {
                Impl::Slot& target = built.impl_->table[slot];
                if (target.entry_plus_one == 0U) {
                    target.entry_plus_one =
                        static_cast<std::uint32_t>(index + 1U);
                    break;
                }
                const Impl::Entry& existing = built.impl_->entries[
                    static_cast<std::size_t>(target.entry_plus_one - 1U)];
                if (existing.hash == entry.hash &&
                    existing.identity.market == entry.identity.market &&
                    existing.identity.security_id_source_size ==
                        entry.identity.security_id_source_size &&
                    existing.identity.security_id_size ==
                        entry.identity.security_id_size &&
                    std::equal(
                        existing.identity.security_id_source.begin(),
                        existing.identity.security_id_source.begin() +
                            existing.identity.security_id_source_size,
                        entry.identity.security_id_source.begin()) &&
                    std::equal(
                        existing.identity.security_id.begin(),
                        existing.identity.security_id.begin() +
                            existing.identity.security_id_size,
                        entry.identity.security_id.begin())) {
                    SetError(error, "duplicate exact instrument identity");
                    return false;
                }
                slot = (slot + 1U) & mask;
            }
            built.impl_->entries.push_back(entry);
        }

        *output = std::move(built);
        SetError(error, {});
        return true;
    } catch (const std::bad_alloc&) {
        SetError(error, "catalog allocation failed");
        return false;
    } catch (const std::exception& exception) {
        SetError(error, std::string("catalog build failed: ") +
                            exception.what());
        return false;
    }
}

bool InstrumentCatalog::LoadCsv(
    const std::filesystem::path& path,
    InstrumentCatalog* output,
    std::string* error) {
    try {
        std::ifstream stream(path);
        if (!stream) {
            SetError(error, "cannot open catalog CSV: " + path.string());
            return false;
        }
        std::string line;
        if (!std::getline(stream, line)) {
            SetError(error, "catalog CSV is empty");
            return false;
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line !=
            "instrument_id,market,security_id_source,security_id") {
            SetError(error, "catalog CSV header is not the required schema");
            return false;
        }

        std::vector<InstrumentDefinition> definitions;
        std::size_t line_number = 1U;
        while (std::getline(stream, line)) {
            ++line_number;
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (line.empty()) {
                SetError(error, "empty catalog row at line " +
                                    std::to_string(line_number));
                return false;
            }
            const std::vector<std::string_view> fields = SplitCsvLine(line);
            if (fields.size() != 4U) {
                SetError(error, "catalog row must contain four columns at "
                                "line " + std::to_string(line_number));
                return false;
            }
            InstrumentDefinition definition{};
            const char* const begin = fields[0].data();
            const char* const end = begin + fields[0].size();
            const auto parsed = std::from_chars(
                begin, end, definition.instrument_id);
            if (parsed.ec != std::errc{} || parsed.ptr != end) {
                SetError(error, "invalid instrument_id at line " +
                                    std::to_string(line_number));
                return false;
            }
            if (fields[1] == "SH") {
                definition.market = Market::kShanghai;
            } else if (fields[1] == "SZ") {
                definition.market = Market::kShenzhen;
            } else {
                SetError(error, "market must be SH or SZ at line " +
                                    std::to_string(line_number));
                return false;
            }
            definition.security_id_source = fields[2];
            definition.security_id = fields[3];
            definitions.push_back(std::move(definition));
        }
        if (!stream.eof()) {
            SetError(error, "catalog CSV read failed");
            return false;
        }
        return Build(std::move(definitions), output, error);
    } catch (const std::exception& exception) {
        SetError(error, std::string("catalog CSV load failed: ") +
                            exception.what());
        return false;
    }
}

bool InstrumentCatalog::Find(
    Market market,
    std::span<const std::byte> security_id_source,
    std::span<const std::byte> security_id,
    InstrumentMatch* output) const noexcept {
    if (output == nullptr || impl_ == nullptr || impl_->table.empty() ||
        security_id.empty() ||
        security_id_source.size() > kMaximumIdentityBytes ||
        security_id.size() > kMaximumIdentityBytes) {
        return false;
    }
    const std::uint64_t hash =
        HashIdentity(market, security_id_source, security_id);
    const std::size_t mask = impl_->table.size() - 1U;
    std::size_t slot = static_cast<std::size_t>(hash) & mask;
    for (std::size_t probe = 0U; probe < impl_->table.size(); ++probe) {
        const std::uint32_t entry_plus_one =
            impl_->table[slot].entry_plus_one;
        if (entry_plus_one == 0U) {
            return false;
        }
        const Impl::Entry& entry = impl_->entries[
            static_cast<std::size_t>(entry_plus_one - 1U)];
        if (entry.hash == hash && entry.identity.market == market &&
            entry.identity.security_id_source_size ==
                security_id_source.size() &&
            entry.identity.security_id_size == security_id.size() &&
            std::equal(security_id_source.begin(),
                       security_id_source.end(),
                       entry.identity.security_id_source.begin()) &&
            std::equal(security_id.begin(), security_id.end(),
                       entry.identity.security_id.begin())) {
            *output = entry.match;
            return true;
        }
        slot = (slot + 1U) & mask;
    }
    return false;
}

std::size_t InstrumentCatalog::size() const noexcept {
    return impl_ == nullptr ? 0U : impl_->entries.size();
}

}  // namespace l2flow::ingest
