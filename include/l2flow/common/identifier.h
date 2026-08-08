#pragma once

#include <array>
#include <cstddef>

namespace l2flow::common {

struct Identifier128 final {
    std::array<std::byte, 16U> bytes{};

    friend constexpr bool operator==(const Identifier128&,
                                     const Identifier128&) = default;
};

}  // namespace l2flow::common
