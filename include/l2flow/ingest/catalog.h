#pragma once

#include "l2flow/ingest/canonical.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace l2flow::ingest {

struct InstrumentDefinition final {
    std::uint32_t instrument_id = 0U;
    Market market = Market::kUnknown;
    std::string security_id_source;
    std::string security_id;
};

struct InstrumentMatch final {
    std::uint32_t instrument_id = 0U;
    std::uint32_t ordinal = kInvalidInstrumentOrdinal;
};

class InstrumentCatalog final {
public:
    InstrumentCatalog();
    ~InstrumentCatalog();
    InstrumentCatalog(InstrumentCatalog&&) noexcept;
    InstrumentCatalog& operator=(InstrumentCatalog&&) noexcept;

    InstrumentCatalog(const InstrumentCatalog&) = delete;
    InstrumentCatalog& operator=(const InstrumentCatalog&) = delete;

    [[nodiscard]] static bool Build(
        std::vector<InstrumentDefinition> definitions,
        InstrumentCatalog* output,
        std::string* error);

    // CSV columns are: instrument_id,market,security_id_source,security_id.
    // market is SH or SZ. Fields are exact opaque ASCII keys; whitespace is
    // never silently trimmed. A single header row is required.
    [[nodiscard]] static bool LoadCsv(
        const std::filesystem::path& path,
        InstrumentCatalog* output,
        std::string* error);

    [[nodiscard]] bool Find(
        Market market,
        std::span<const std::byte> security_id_source,
        std::span<const std::byte> security_id,
        InstrumentMatch* output) const noexcept;

    [[nodiscard]] std::size_t size() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ingest
