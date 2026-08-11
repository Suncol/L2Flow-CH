#include "l2flow/arrow/schemas.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/table_builder.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace l2flow::arrow_hot {
namespace {

enum CommonField : int {
    kFeedSessionEpoch = 0,
    kIngressSequence,
    kVendorSequenceId,
    kReceiveMonotonicNs,
    kNativeSequence,
    kExchangeTimeNs,
    kVendorLocalTimeNs,
    kQualityFlags,
    kGapEpoch,
    kGapBeforeFirst,
    kGapBeforeLast,
    kTradeDate,
    kInstrumentId,
    kInstrumentOrdinal,
    kChannel,
    kExchangeTimeRaw,
    kVendorLocalTimeRaw,
    kServiceId,
    kServiceVersion,
    kMessageId,
    kCanonicalKind,
    kMarket,
    kSecurityIdSource,
    kSecurityId,
    kMdStreamId,
    kCommonFieldCount,
};

enum TickField : int {
    kTickStreamRole = kCommonFieldCount,
    kTickPriceRaw,
    kTickPriceP6,
    kTickPriceScale,
    kTickAmountRaw,
    kTickAmountP6,
    kTickAmountScale,
    kTickQuantityRaw,
    kTickQuantityScale,
    kTickPrimaryOrderId,
    kTickBuyOrderId,
    kTickSellOrderId,
    kTickMatchedQuantityRaw,
    kTickValidity,
    kTickRawType,
    kTickRawSide,
    kTickAction,
    kTickSide,
    kTickAggressor,
    kTickOrderType,
    kTickPhase,
    kTickExpectedSequence,
    kTickAdmissionFloor,
    kTickGeneration,
    kTickEvictBefore,
    kTickCatalogMatch,
};

enum SnapshotField : int {
    kSnapshotPreviousCloseRaw = kCommonFieldCount,
    kSnapshotPreviousCloseP6,
    kSnapshotPreviousCloseScale,
    kSnapshotOpenRaw,
    kSnapshotOpenP6,
    kSnapshotOpenScale,
    kSnapshotHighRaw,
    kSnapshotHighP6,
    kSnapshotHighScale,
    kSnapshotLowRaw,
    kSnapshotLowP6,
    kSnapshotLowScale,
    kSnapshotLastRaw,
    kSnapshotLastP6,
    kSnapshotLastScale,
    kSnapshotCloseRaw,
    kSnapshotCloseP6,
    kSnapshotCloseScale,
    kSnapshotTurnoverRaw,
    kSnapshotTurnoverP6,
    kSnapshotTurnoverScale,
    kSnapshotVolumeRaw,
    kSnapshotVolumeScale,
    kSnapshotTotalBidQuantityRaw,
    kSnapshotTotalBidQuantityScale,
    kSnapshotTotalAskQuantityRaw,
    kSnapshotTotalAskQuantityScale,
    kSnapshotWeightedAverageBidRaw,
    kSnapshotWeightedAverageBidP6,
    kSnapshotWeightedAverageBidScale,
    kSnapshotWeightedAverageAskRaw,
    kSnapshotWeightedAverageAskP6,
    kSnapshotWeightedAverageAskScale,
    kSnapshotTradeCount,
    kSnapshotImageStatus,
    kSnapshotInstrumentStatusCode,
    kSnapshotTradingPhaseCode,
    kSnapshotSourceBidDepth,
    kSnapshotSourceAskDepth,
    kSnapshotRetainedBidDepth,
    kSnapshotRetainedAskDepth,
    kSnapshotBids,
    kSnapshotAsks,
};

enum BookLevelField : int {
    kLevelPriceRaw = 0,
    kLevelPriceP6,
    kLevelPriceScale,
    kLevelQuantityRaw,
    kLevelQuantityScale,
    kLevelSourceOrderCount,
};

enum ControlField : int {
    kControlFeedSessionEpoch = 0,
    kControlProducerInstanceHigh,
    kControlProducerInstanceLow,
    kControlKind,
    kControlObservedMonotonicNs,
    kControlMarket,
    kControlChannel,
    kControlFirstMissing,
    kControlLastMissing,
    kControlFirstPresentAfterGap,
    kControlGapEpoch,
    kControlCumulativeMissing,
    kControlExpectedSequence,
    kControlObservedSequence,
    kControlReason,
    kControlAffectedStream,
    kControlAffectedShard,
    kControlDroppedRows,
};

[[nodiscard]] std::shared_ptr<arrow::KeyValueMetadata> SchemaMetadata(
    std::string name,
    std::uint32_t version) {
    return arrow::key_value_metadata(
        {"l2flow.schema_name", "l2flow.schema_version",
         "l2flow.transport", "l2flow.durability",
         "l2flow.ordering"},
        {std::move(name), std::to_string(version),
         "arrow-ipc-record-batch-message", "volatile",
         "per-shard-fifo-no-global-order"});
}

void AppendCommonFields(arrow::FieldVector* fields) {
    fields->push_back(arrow::field("feed_session_epoch", arrow::uint64(), false));
    fields->push_back(arrow::field("ingress_sequence", arrow::uint64(), false));
    fields->push_back(arrow::field("vendor_sequence_id", arrow::uint64(), false));
    fields->push_back(arrow::field("receive_monotonic_ns", arrow::uint64(), false));
    fields->push_back(arrow::field("native_sequence", arrow::uint64()));
    fields->push_back(arrow::field("exchange_time_ns_from_midnight", arrow::uint64()));
    fields->push_back(arrow::field("vendor_local_time_ns_from_midnight", arrow::uint64()));
    fields->push_back(arrow::field("quality_flags", arrow::uint64(), false));
    fields->push_back(arrow::field("gap_epoch", arrow::uint64(), false));
    fields->push_back(arrow::field("gap_before_first", arrow::uint64()));
    fields->push_back(arrow::field("gap_before_last", arrow::uint64()));
    fields->push_back(arrow::field("trade_date", arrow::uint32(), false));
    fields->push_back(arrow::field("instrument_id", arrow::uint32()));
    fields->push_back(arrow::field("instrument_ordinal", arrow::uint32()));
    fields->push_back(arrow::field("channel", arrow::uint32(), false));
    fields->push_back(arrow::field("exchange_time_raw", arrow::uint32(), false));
    fields->push_back(arrow::field("vendor_local_time_raw", arrow::uint32(), false));
    fields->push_back(arrow::field("service_id", arrow::uint8(), false));
    fields->push_back(arrow::field("service_version", arrow::uint16(), false));
    fields->push_back(arrow::field("message_id", arrow::uint16(), false));
    fields->push_back(arrow::field("canonical_kind", arrow::uint8(), false));
    fields->push_back(arrow::field("market", arrow::uint8(), false));
    fields->push_back(arrow::field("security_id_source", arrow::binary(), false));
    fields->push_back(arrow::field("security_id", arrow::binary(), false));
    fields->push_back(arrow::field("md_stream_id", arrow::binary(), false));
}

[[nodiscard]] arrow::FieldVector TickFields() {
    arrow::FieldVector fields;
    fields.reserve(50U);
    AppendCommonFields(&fields);
    fields.push_back(arrow::field("stream_role", arrow::uint8(), false));
    fields.push_back(arrow::field("price_raw", arrow::int64()));
    fields.push_back(arrow::field("price_p6", arrow::int64()));
    fields.push_back(arrow::field("price_source_scale", arrow::uint8(), false));
    fields.push_back(arrow::field("amount_raw", arrow::int64()));
    fields.push_back(arrow::field("amount_p6", arrow::int64()));
    fields.push_back(arrow::field("amount_source_scale", arrow::uint8(), false));
    fields.push_back(arrow::field("quantity_raw", arrow::int64()));
    fields.push_back(arrow::field("quantity_scale", arrow::uint8(), false));
    fields.push_back(arrow::field("primary_order_id", arrow::int64()));
    fields.push_back(arrow::field("buy_order_id", arrow::int64()));
    fields.push_back(arrow::field("sell_order_id", arrow::int64()));
    fields.push_back(arrow::field("sh_add_matched_quantity_raw", arrow::int64()));
    fields.push_back(arrow::field("validity", arrow::uint64(), false));
    fields.push_back(arrow::field("raw_type", arrow::int32(), false));
    fields.push_back(arrow::field("raw_side", arrow::int32(), false));
    fields.push_back(arrow::field("action", arrow::uint8(), false));
    fields.push_back(arrow::field("side", arrow::uint8(), false));
    fields.push_back(arrow::field("aggressor", arrow::uint8(), false));
    fields.push_back(arrow::field("order_type", arrow::uint8(), false));
    fields.push_back(arrow::field("phase", arrow::uint8(), false));
    fields.push_back(arrow::field("expected_sequence", arrow::uint64()));
    fields.push_back(arrow::field("admission_floor", arrow::uint64()));
    fields.push_back(arrow::field("generation", arrow::uint64()));
    fields.push_back(arrow::field("evict_before", arrow::uint64()));
    fields.push_back(arrow::field("catalog_match", arrow::boolean()));
    return fields;
}

[[nodiscard]] std::shared_ptr<arrow::DataType> BookLevelType() {
    return arrow::struct_({
        arrow::field("price_raw", arrow::int64()),
        arrow::field("price_p6", arrow::int64()),
        arrow::field("price_source_scale", arrow::uint8(), false),
        arrow::field("quantity_raw", arrow::int64()),
        arrow::field("quantity_scale", arrow::uint8(), false),
        arrow::field("source_order_count", arrow::uint32()),
    });
}

[[nodiscard]] arrow::FieldVector SnapshotFields() {
    arrow::FieldVector fields;
    fields.reserve(68U);
    AppendCommonFields(&fields);
    const auto append_decimal = [&fields](std::string prefix) {
        fields.push_back(arrow::field(prefix + "_raw", arrow::int64()));
        fields.push_back(arrow::field(prefix + "_p6", arrow::int64()));
        fields.push_back(arrow::field(prefix + "_source_scale", arrow::uint8(), false));
    };
    const auto append_scaled = [&fields](std::string prefix) {
        fields.push_back(arrow::field(prefix + "_raw", arrow::int64()));
        fields.push_back(arrow::field(prefix + "_scale", arrow::uint8(), false));
    };
    append_decimal("previous_close");
    append_decimal("open");
    append_decimal("high");
    append_decimal("low");
    append_decimal("last");
    append_decimal("close");
    append_decimal("turnover");
    append_scaled("volume");
    append_scaled("total_bid_quantity");
    append_scaled("total_ask_quantity");
    append_decimal("weighted_average_bid");
    append_decimal("weighted_average_ask");
    fields.push_back(arrow::field("trade_count", arrow::uint64()));
    fields.push_back(arrow::field("image_status", arrow::int32()));
    fields.push_back(arrow::field("instrument_status_code", arrow::binary()));
    fields.push_back(arrow::field("trading_phase_code", arrow::binary()));
    fields.push_back(arrow::field("source_bid_depth", arrow::uint32(), false));
    fields.push_back(arrow::field("source_ask_depth", arrow::uint32(), false));
    fields.push_back(arrow::field("retained_bid_depth", arrow::uint8(), false));
    fields.push_back(arrow::field("retained_ask_depth", arrow::uint8(), false));
    fields.push_back(arrow::field("bids", arrow::list(BookLevelType()), false));
    fields.push_back(arrow::field("asks", arrow::list(BookLevelType()), false));
    return fields;
}

[[nodiscard]] arrow::FieldVector ControlFields() {
    return {
        arrow::field("feed_session_epoch", arrow::uint64(), false),
        arrow::field("producer_instance_high", arrow::uint64(), false),
        arrow::field("producer_instance_low", arrow::uint64(), false),
        arrow::field("control_kind", arrow::uint8(), false),
        arrow::field("observed_monotonic_ns", arrow::uint64(), false),
        arrow::field("market", arrow::uint8()),
        arrow::field("channel", arrow::uint32()),
        arrow::field("first_missing", arrow::uint64()),
        arrow::field("last_missing", arrow::uint64()),
        arrow::field("first_present_after_gap", arrow::uint64()),
        arrow::field("gap_epoch", arrow::uint64()),
        arrow::field("cumulative_missing_sequences", arrow::uint64()),
        arrow::field("expected_sequence", arrow::uint64()),
        arrow::field("observed_sequence", arrow::uint64()),
        arrow::field("reason", arrow::uint8()),
        arrow::field("affected_stream", arrow::uint32()),
        arrow::field("affected_shard", arrow::uint32()),
        arrow::field("dropped_rows", arrow::uint64()),
    };
}

template <typename Builder, typename Value>
arrow::Status AppendNullable(Builder* builder,
                             bool valid,
                             Value value) {
    return valid ? builder->Append(value) : builder->AppendNull();
}

arrow::Status AppendBinary(arrow::BinaryBuilder* builder,
                           const std::byte* data,
                           std::size_t size) {
    if (size > static_cast<std::size_t>(
                   std::numeric_limits<std::int32_t>::max())) {
        return arrow::Status::Invalid("binary canonical field is too large");
    }
    return builder->Append(reinterpret_cast<const std::uint8_t*>(data),
                           static_cast<std::int32_t>(size));
}

arrow::Status AppendCommon(arrow::RecordBatchBuilder* builder,
                           const ingest::CanonicalCommon& common,
                           std::uint64_t feed_session_epoch) {
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kFeedSessionEpoch)->Append(feed_session_epoch));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kIngressSequence)->Append(common.ingress_sequence));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kVendorSequenceId)->Append(common.vendor_sequence_id));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kReceiveMonotonicNs)->Append(common.receive_monotonic_ns));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt64Builder>(kNativeSequence),
        common.native_sequence != 0U, common.native_sequence));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt64Builder>(kExchangeTimeNs),
        common.exchange_time_valid,
        common.exchange_time_ns_from_midnight));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt64Builder>(kVendorLocalTimeNs),
        common.vendor_local_time_valid,
        common.vendor_local_time_ns_from_midnight));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kQualityFlags)->Append(common.quality_flags));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt64Builder>(
        kGapEpoch)->Append(common.gap_epoch));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt64Builder>(kGapBeforeFirst),
        common.gap_before_first != 0U, common.gap_before_first));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt64Builder>(kGapBeforeLast),
        common.gap_before_last != 0U, common.gap_before_last));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt32Builder>(
        kTradeDate)->Append(common.trade_date));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt32Builder>(kInstrumentId),
        common.instrument_id != 0U, common.instrument_id));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::UInt32Builder>(kInstrumentOrdinal),
        common.instrument_ordinal != ingest::kInvalidInstrumentOrdinal,
        common.instrument_ordinal));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt32Builder>(
        kChannel)->Append(common.channel));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt32Builder>(
        kExchangeTimeRaw)->Append(common.exchange_time_raw));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt32Builder>(
        kVendorLocalTimeRaw)->Append(common.vendor_local_time_raw));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt8Builder>(
        kServiceId)->Append(common.message_key.service_id));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt16Builder>(
        kServiceVersion)->Append(common.message_key.service_version));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt16Builder>(
        kMessageId)->Append(common.message_key.message_id));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt8Builder>(
        kCanonicalKind)->Append(static_cast<std::uint8_t>(common.kind)));
    ARROW_RETURN_NOT_OK(builder->GetFieldAs<arrow::UInt8Builder>(
        kMarket)->Append(static_cast<std::uint8_t>(common.identity.market)));
    ARROW_RETURN_NOT_OK(AppendBinary(
        builder->GetFieldAs<arrow::BinaryBuilder>(kSecurityIdSource),
        common.identity.security_id_source.data(),
        common.identity.security_id_source_size));
    ARROW_RETURN_NOT_OK(AppendBinary(
        builder->GetFieldAs<arrow::BinaryBuilder>(kSecurityId),
        common.identity.security_id.data(), common.identity.security_id_size));
    return AppendBinary(
        builder->GetFieldAs<arrow::BinaryBuilder>(kMdStreamId),
        common.md_stream_id.data(), common.md_stream_id_size);
}

void UpdateBatchMetadata(const ingest::CanonicalCommon& common,
                         std::uint64_t feed_session_epoch,
                         std::size_t previous_rows,
                         bool* has_exchange_time,
                         BatchMetadata* metadata) noexcept {
    metadata->feed_session_epoch = feed_session_epoch;
    if (previous_rows == 0U) {
        metadata->first_ingress_sequence = common.ingress_sequence;
        metadata->last_ingress_sequence = common.ingress_sequence;
    } else {
        metadata->first_ingress_sequence = std::min(
            metadata->first_ingress_sequence, common.ingress_sequence);
        metadata->last_ingress_sequence = std::max(
            metadata->last_ingress_sequence, common.ingress_sequence);
    }
    if (common.exchange_time_valid) {
        if (!*has_exchange_time) {
            metadata->minimum_exchange_time_ns =
                common.exchange_time_ns_from_midnight;
            metadata->maximum_exchange_time_ns =
                common.exchange_time_ns_from_midnight;
            *has_exchange_time = true;
        } else {
            metadata->minimum_exchange_time_ns = std::min(
                metadata->minimum_exchange_time_ns,
                common.exchange_time_ns_from_midnight);
            metadata->maximum_exchange_time_ns = std::max(
                metadata->maximum_exchange_time_ns,
                common.exchange_time_ns_from_midnight);
        }
    }
}

arrow::Status AppendDecimal(arrow::RecordBatchBuilder* builder,
                            int raw_index,
                            int p6_index,
                            int scale_index,
                            const ingest::FixedDecimal& value) {
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::Int64Builder>(raw_index),
        value.raw_valid, value.raw));
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::Int64Builder>(p6_index),
        value.p6_valid, value.p6));
    return builder->GetFieldAs<arrow::UInt8Builder>(scale_index)->Append(
        value.source_scale);
}

arrow::Status AppendScaled(arrow::RecordBatchBuilder* builder,
                           int raw_index,
                           int scale_index,
                           const ingest::ScaledInteger& value) {
    ARROW_RETURN_NOT_OK(AppendNullable(
        builder->GetFieldAs<arrow::Int64Builder>(raw_index),
        value.valid, value.raw));
    return builder->GetFieldAs<arrow::UInt8Builder>(scale_index)->Append(
        value.scale);
}

arrow::Status AppendBookLevels(
    arrow::ListBuilder* list_builder,
    const std::array<ingest::CanonicalBookLevel,
                     ingest::kCanonicalBookDepth>& levels,
    std::uint8_t retained_depth) {
    if (retained_depth > ingest::kCanonicalBookDepth) {
        return arrow::Status::Invalid("retained book depth exceeds schema");
    }
    ARROW_RETURN_NOT_OK(list_builder->Append());
    auto* const structure =
        static_cast<arrow::StructBuilder*>(list_builder->value_builder());
    for (std::size_t index = 0U; index < retained_depth; ++index) {
        const ingest::CanonicalBookLevel& level = levels[index];
        ARROW_RETURN_NOT_OK(structure->Append());
        ARROW_RETURN_NOT_OK(AppendNullable(
            static_cast<arrow::Int64Builder*>(
                structure->field_builder(kLevelPriceRaw)),
            level.price.raw_valid, level.price.raw));
        ARROW_RETURN_NOT_OK(AppendNullable(
            static_cast<arrow::Int64Builder*>(
                structure->field_builder(kLevelPriceP6)),
            level.price.p6_valid, level.price.p6));
        ARROW_RETURN_NOT_OK(static_cast<arrow::UInt8Builder*>(
            structure->field_builder(kLevelPriceScale))->Append(
                level.price.source_scale));
        ARROW_RETURN_NOT_OK(AppendNullable(
            static_cast<arrow::Int64Builder*>(
                structure->field_builder(kLevelQuantityRaw)),
            level.quantity.valid, level.quantity.raw));
        ARROW_RETURN_NOT_OK(static_cast<arrow::UInt8Builder*>(
            structure->field_builder(kLevelQuantityScale))->Append(
                level.quantity.scale));
        ARROW_RETURN_NOT_OK(AppendNullable(
            static_cast<arrow::UInt32Builder*>(
                structure->field_builder(kLevelSourceOrderCount)),
            level.order_count_valid, level.source_order_count));
    }
    return arrow::Status::OK();
}

[[nodiscard]] bool CheckInitialCapacity(std::size_t capacity,
                                        std::string* error) noexcept {
    if (capacity == 0U ||
        capacity > static_cast<std::size_t>(
                       std::numeric_limits<std::int64_t>::max())) {
        if (error != nullptr) {
            *error = "Arrow batch initial capacity is invalid";
        }
        return false;
    }
    return true;
}

void SetError(std::string* error, std::string value) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(value);
    } catch (...) {
    }
}

[[nodiscard]] bool ValidateCommon(
    const ingest::CanonicalCommon& common,
    std::string* error) noexcept {
    if (common.identity.security_id_source_size >
            common.identity.security_id_source.size() ||
        common.identity.security_id_size >
            common.identity.security_id.size() ||
        common.md_stream_id_size > common.md_stream_id.size()) {
        SetError(error, "canonical identity/stream byte length is invalid");
        return false;
    }
    return true;
}

[[nodiscard]] bool IsValidLifecycle(ControlKind kind,
                                    ContinuityReason reason) noexcept {
    switch (kind) {
        case ControlKind::kProducerStarted:
            return reason == ContinuityReason::kProcessStart;
        case ControlKind::kFeedConnected:
            return reason == ContinuityReason::kMdlConnect;
        case ControlKind::kFeedDisconnected:
            return reason == ContinuityReason::kMdlDisconnect;
        case ControlKind::kProducerSealed:
            return reason == ContinuityReason::kProducerShutdown;
        case ControlKind::kFeedConnectError:
            return reason == ContinuityReason::kMdlConnectError;
        case ControlKind::kFeedServiceTimeout:
            return reason == ContinuityReason::kMdlServiceTimeout;
        case ControlKind::kFeedMessageDiscarded:
            return reason == ContinuityReason::kMdlMessageDiscarded;
        case ControlKind::kFeedSubscriptionRejected:
            return reason == ContinuityReason::kMdlSubscriptionRejected;
        case ControlKind::kFeedControlProtocolError:
            return reason == ContinuityReason::kMdlControlProtocolError;
        case ControlKind::kFeedReadyTimeout:
            return reason == ContinuityReason::kMdlReadyTimeout;
        case ControlKind::kChannelGap:
        case ControlKind::kChannelFault:
        case ControlKind::kHotPublishOverrun:
            return false;
    }
    return false;
}

[[nodiscard]] bool IsRingStreamKind(RingStreamKind kind) noexcept {
    switch (kind) {
        case RingStreamKind::kOrderedTick:
        case RingStreamKind::kSnapshot:
        case RingStreamKind::kControl:
        case RingStreamKind::kEvent:
        case RingStreamKind::kKline:
            return true;
    }
    return false;
}

[[nodiscard]] bool IsKnownMarket(ingest::Market market) noexcept {
    return market == ingest::Market::kShanghai ||
           market == ingest::Market::kShenzhen;
}

[[nodiscard]] bool IsChannelFaultReason(
    ingest::ChannelFaultReason reason) noexcept {
    switch (reason) {
        case ingest::ChannelFaultReason::kDecodeFailure:
            return true;
    }
    return false;
}

}  // namespace

std::shared_ptr<arrow::Schema> TickArrowSchema() {
    static const std::shared_ptr<arrow::Schema> schema = arrow::schema(
        TickFields(), SchemaMetadata("l2flow.tick_hot", kTickArrowSchemaVersion));
    return schema;
}

std::shared_ptr<arrow::Schema> SnapshotArrowSchema() {
    static const std::shared_ptr<arrow::Schema> schema = arrow::schema(
        SnapshotFields(),
        SchemaMetadata("l2flow.snapshot_hot", kSnapshotArrowSchemaVersion));
    return schema;
}

std::shared_ptr<arrow::Schema> ControlArrowSchema() {
    static const std::shared_ptr<arrow::Schema> schema = arrow::schema(
        ControlFields(),
        SchemaMetadata("l2flow.control_hot", kControlArrowSchemaVersion));
    return schema;
}

class TickRecordBatchBuilder::Impl final {
public:
    Impl(std::unique_ptr<arrow::RecordBatchBuilder> builder,
         std::uint64_t feed_session_epoch) noexcept
        : builder_(std::move(builder)),
          feed_session_epoch_(feed_session_epoch) {}

    arrow::Status Append(const ingest::CanonicalTick& tick,
                         TickStreamRole role,
                         const ingest::TickDispatch* hole_fill) {
        ARROW_RETURN_NOT_OK(
            AppendCommon(builder_.get(), tick.common, feed_session_epoch_));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickStreamRole)->Append(static_cast<std::uint8_t>(role)));
        ARROW_RETURN_NOT_OK(AppendDecimal(builder_.get(), kTickPriceRaw,
                                          kTickPriceP6, kTickPriceScale,
                                          tick.price));
        ARROW_RETURN_NOT_OK(AppendDecimal(builder_.get(), kTickAmountRaw,
                                          kTickAmountP6, kTickAmountScale,
                                          tick.amount));
        ARROW_RETURN_NOT_OK(AppendScaled(builder_.get(), kTickQuantityRaw,
                                         kTickQuantityScale, tick.quantity));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::Int64Builder>(kTickPrimaryOrderId),
            (tick.validity & ingest::kTickPrimaryOrderIdValid) != 0U,
            tick.primary_order_id));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::Int64Builder>(kTickBuyOrderId),
            (tick.validity & ingest::kTickBuyOrderIdValid) != 0U,
            tick.buy_order_id));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::Int64Builder>(kTickSellOrderId),
            (tick.validity & ingest::kTickSellOrderIdValid) != 0U,
            tick.sell_order_id));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::Int64Builder>(
                kTickMatchedQuantityRaw),
            (tick.validity & ingest::kTickMatchedQuantityValid) != 0U,
            tick.sh_add_matched_quantity_raw));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt64Builder>(
            kTickValidity)->Append(tick.validity));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::Int32Builder>(
            kTickRawType)->Append(tick.raw_type));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::Int32Builder>(
            kTickRawSide)->Append(tick.raw_side));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickAction)->Append(static_cast<std::uint8_t>(tick.action)));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickSide)->Append(static_cast<std::uint8_t>(tick.side)));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickAggressor)->Append(
                static_cast<std::uint8_t>(tick.aggressor)));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickOrderType)->Append(
                static_cast<std::uint8_t>(tick.order_type)));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kTickPhase)->Append(static_cast<std::uint8_t>(tick.phase)));
        const bool is_hole_fill = hole_fill != nullptr;
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::UInt64Builder>(
                kTickExpectedSequence),
            is_hole_fill,
            is_hole_fill ? hole_fill->expected_sequence : 0U));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::UInt64Builder>(
                kTickAdmissionFloor),
            is_hole_fill,
            is_hole_fill ? hole_fill->admission_floor : 0U));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::UInt64Builder>(kTickGeneration),
            is_hole_fill,
            is_hole_fill ? hole_fill->generation : 0U));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::UInt64Builder>(kTickEvictBefore),
            is_hole_fill,
            is_hole_fill ? hole_fill->evict_before : 0U));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::BooleanBuilder>(kTickCatalogMatch),
            is_hole_fill,
            is_hole_fill && hole_fill->catalog_match));
        UpdateBatchMetadata(tick.common, feed_session_epoch_, rows_,
                            &has_exchange_time_, &metadata_);
        ++rows_;
        return arrow::Status::OK();
    }

    std::unique_ptr<arrow::RecordBatchBuilder> builder_;
    std::uint64_t feed_session_epoch_ = 0U;
    std::size_t rows_ = 0U;
    bool has_exchange_time_ = false;
    BatchMetadata metadata_{};
};

std::unique_ptr<TickRecordBatchBuilder> TickRecordBatchBuilder::Create(
    std::size_t initial_capacity,
    std::uint64_t feed_session_epoch,
    std::string* error) {
    if (!CheckInitialCapacity(initial_capacity, error) ||
        feed_session_epoch == 0U) {
        if (feed_session_epoch == 0U) {
            SetError(error, "feed session epoch must be positive");
        }
        return nullptr;
    }
    auto result = arrow::RecordBatchBuilder::Make(
        TickArrowSchema(), arrow::default_memory_pool(),
        static_cast<std::int64_t>(initial_capacity));
    if (!result.ok()) {
        SetError(error, result.status().ToString());
        return nullptr;
    }
    return std::unique_ptr<TickRecordBatchBuilder>(
        new TickRecordBatchBuilder(std::make_unique<Impl>(
            std::move(*result), feed_session_epoch)));
}

TickRecordBatchBuilder::TickRecordBatchBuilder(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

TickRecordBatchBuilder::~TickRecordBatchBuilder() = default;

bool TickRecordBatchBuilder::AppendOrdered(
    const ingest::CanonicalTick& tick,
    std::string* error) noexcept {
    if (!ValidateCommon(tick.common, error)) {
        return false;
    }
    try {
        const arrow::Status status = impl_->Append(
            tick, TickStreamRole::kRealtimeOrdered, nullptr);
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

bool TickRecordBatchBuilder::AppendHoleFill(
    const ingest::TickDispatch& dispatch,
    std::string* error) noexcept {
    const std::uint64_t sequence = dispatch.tick.common.native_sequence;
    if (!ValidateCommon(dispatch.tick.common, error)) {
        return false;
    }
    if (dispatch.kind != ingest::TickDispatchKind::kProjectHoleFill ||
        dispatch.feed_session_epoch != impl_->feed_session_epoch_ ||
        dispatch.expected_sequence == 0U ||
        dispatch.admission_floor > sequence ||
        sequence >= dispatch.expected_sequence ||
        dispatch.generation == 0U || dispatch.evict_before == 0U) {
        SetError(error, "invalid hole-fill dispatch");
        return false;
    }
    try {
        const arrow::Status status = impl_->Append(
            dispatch.tick, TickStreamRole::kHoleFill, &dispatch);
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

BuiltRecordBatch TickRecordBatchBuilder::Finish(
    std::string* error) noexcept {
    BuiltRecordBatch output{};
    if (impl_->rows_ == 0U) {
        SetError(error, "cannot finish an empty Tick batch");
        return output;
    }
    try {
        auto result = impl_->builder_->Flush();
        if (!result.ok()) {
            SetError(error, result.status().ToString());
            return output;
        }
        output.batch = std::move(*result);
        output.metadata = impl_->metadata_;
        impl_->rows_ = 0U;
        impl_->has_exchange_time_ = false;
        impl_->metadata_ = {};
        SetError(error, {});
        return output;
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return output;
    }
}

std::size_t TickRecordBatchBuilder::rows() const noexcept {
    return impl_->rows_;
}

std::uint64_t TickRecordBatchBuilder::feed_session_epoch() const noexcept {
    return impl_->feed_session_epoch_;
}

class SnapshotRecordBatchBuilder::Impl final {
public:
    Impl(std::unique_ptr<arrow::RecordBatchBuilder> builder,
         std::uint64_t feed_session_epoch) noexcept
        : builder_(std::move(builder)),
          feed_session_epoch_(feed_session_epoch) {}

    arrow::Status Append(const ingest::CanonicalSnapshot& snapshot) {
        ARROW_RETURN_NOT_OK(AppendCommon(
            builder_.get(), snapshot.common, feed_session_epoch_));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotPreviousCloseRaw,
            kSnapshotPreviousCloseP6, kSnapshotPreviousCloseScale,
            snapshot.previous_close));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotOpenRaw, kSnapshotOpenP6,
            kSnapshotOpenScale, snapshot.open));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotHighRaw, kSnapshotHighP6,
            kSnapshotHighScale, snapshot.high));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotLowRaw, kSnapshotLowP6,
            kSnapshotLowScale, snapshot.low));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotLastRaw, kSnapshotLastP6,
            kSnapshotLastScale, snapshot.last));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotCloseRaw, kSnapshotCloseP6,
            kSnapshotCloseScale, snapshot.close));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotTurnoverRaw, kSnapshotTurnoverP6,
            kSnapshotTurnoverScale, snapshot.turnover));
        ARROW_RETURN_NOT_OK(AppendScaled(
            builder_.get(), kSnapshotVolumeRaw, kSnapshotVolumeScale,
            snapshot.volume));
        ARROW_RETURN_NOT_OK(AppendScaled(
            builder_.get(), kSnapshotTotalBidQuantityRaw,
            kSnapshotTotalBidQuantityScale, snapshot.total_bid_quantity));
        ARROW_RETURN_NOT_OK(AppendScaled(
            builder_.get(), kSnapshotTotalAskQuantityRaw,
            kSnapshotTotalAskQuantityScale, snapshot.total_ask_quantity));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotWeightedAverageBidRaw,
            kSnapshotWeightedAverageBidP6,
            kSnapshotWeightedAverageBidScale,
            snapshot.weighted_average_bid));
        ARROW_RETURN_NOT_OK(AppendDecimal(
            builder_.get(), kSnapshotWeightedAverageAskRaw,
            kSnapshotWeightedAverageAskP6,
            kSnapshotWeightedAverageAskScale,
            snapshot.weighted_average_ask));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::UInt64Builder>(kSnapshotTradeCount),
            snapshot.trade_count_valid, snapshot.trade_count));
        ARROW_RETURN_NOT_OK(AppendNullable(
            builder_->GetFieldAs<arrow::Int32Builder>(kSnapshotImageStatus),
            snapshot.image_status_valid, snapshot.image_status));
        auto* const status = builder_->GetFieldAs<arrow::BinaryBuilder>(
            kSnapshotInstrumentStatusCode);
        if (snapshot.instrument_status_code_size == 0U) {
            ARROW_RETURN_NOT_OK(status->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(AppendBinary(
                status, snapshot.instrument_status_code.data(),
                snapshot.instrument_status_code_size));
        }
        auto* const phase = builder_->GetFieldAs<arrow::BinaryBuilder>(
            kSnapshotTradingPhaseCode);
        if (snapshot.trading_phase_code_size == 0U) {
            ARROW_RETURN_NOT_OK(phase->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(AppendBinary(
                phase, snapshot.trading_phase_code.data(),
                snapshot.trading_phase_code_size));
        }
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt32Builder>(
            kSnapshotSourceBidDepth)->Append(snapshot.source_bid_depth));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt32Builder>(
            kSnapshotSourceAskDepth)->Append(snapshot.source_ask_depth));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kSnapshotRetainedBidDepth)->Append(snapshot.retained_bid_depth));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kSnapshotRetainedAskDepth)->Append(snapshot.retained_ask_depth));
        ARROW_RETURN_NOT_OK(AppendBookLevels(
            builder_->GetFieldAs<arrow::ListBuilder>(kSnapshotBids),
            snapshot.bids, snapshot.retained_bid_depth));
        ARROW_RETURN_NOT_OK(AppendBookLevels(
            builder_->GetFieldAs<arrow::ListBuilder>(kSnapshotAsks),
            snapshot.asks, snapshot.retained_ask_depth));
        UpdateBatchMetadata(snapshot.common, feed_session_epoch_, rows_,
                            &has_exchange_time_, &metadata_);
        ++rows_;
        return arrow::Status::OK();
    }

    std::unique_ptr<arrow::RecordBatchBuilder> builder_;
    std::uint64_t feed_session_epoch_ = 0U;
    std::size_t rows_ = 0U;
    bool has_exchange_time_ = false;
    BatchMetadata metadata_{};
};

std::unique_ptr<SnapshotRecordBatchBuilder>
SnapshotRecordBatchBuilder::Create(std::size_t initial_capacity,
                                   std::uint64_t feed_session_epoch,
                                   std::string* error) {
    if (!CheckInitialCapacity(initial_capacity, error) ||
        feed_session_epoch == 0U) {
        if (feed_session_epoch == 0U) {
            SetError(error, "feed session epoch must be positive");
        }
        return nullptr;
    }
    auto result = arrow::RecordBatchBuilder::Make(
        SnapshotArrowSchema(), arrow::default_memory_pool(),
        static_cast<std::int64_t>(initial_capacity));
    if (!result.ok()) {
        SetError(error, result.status().ToString());
        return nullptr;
    }
    return std::unique_ptr<SnapshotRecordBatchBuilder>(
        new SnapshotRecordBatchBuilder(std::make_unique<Impl>(
            std::move(*result), feed_session_epoch)));
}

SnapshotRecordBatchBuilder::SnapshotRecordBatchBuilder(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SnapshotRecordBatchBuilder::~SnapshotRecordBatchBuilder() = default;

bool SnapshotRecordBatchBuilder::Append(
    const ingest::CanonicalSnapshot& snapshot,
    std::string* error) noexcept {
    if (!ValidateCommon(snapshot.common, error) ||
        snapshot.instrument_status_code_size >
            snapshot.instrument_status_code.size() ||
        snapshot.trading_phase_code_size >
            snapshot.trading_phase_code.size() ||
        snapshot.retained_bid_depth > snapshot.bids.size() ||
        snapshot.retained_ask_depth > snapshot.asks.size()) {
        if (error != nullptr && error->empty()) {
            SetError(error, "invalid Snapshot byte length or retained depth");
        }
        return false;
    }
    try {
        const arrow::Status status = impl_->Append(snapshot);
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

BuiltRecordBatch SnapshotRecordBatchBuilder::Finish(
    std::string* error) noexcept {
    BuiltRecordBatch output{};
    if (impl_->rows_ == 0U) {
        SetError(error, "cannot finish an empty Snapshot batch");
        return output;
    }
    try {
        auto result = impl_->builder_->Flush();
        if (!result.ok()) {
            SetError(error, result.status().ToString());
            return output;
        }
        output.batch = std::move(*result);
        output.metadata = impl_->metadata_;
        impl_->rows_ = 0U;
        impl_->has_exchange_time_ = false;
        impl_->metadata_ = {};
        SetError(error, {});
        return output;
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return output;
    }
}

std::size_t SnapshotRecordBatchBuilder::rows() const noexcept {
    return impl_->rows_;
}

std::uint64_t SnapshotRecordBatchBuilder::feed_session_epoch() const noexcept {
    return impl_->feed_session_epoch_;
}

class ControlRecordBatchBuilder::Impl final {
public:
    Impl(std::unique_ptr<arrow::RecordBatchBuilder> builder,
         std::uint64_t feed_session_epoch,
         ProducerInstanceId producer_instance) noexcept
        : builder_(std::move(builder)),
          feed_session_epoch_(feed_session_epoch),
          producer_instance_(producer_instance) {}

    arrow::Status AppendBase(ControlKind kind,
                             std::uint64_t observed_monotonic_ns) {
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt64Builder>(
            kControlFeedSessionEpoch)->Append(feed_session_epoch_));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt64Builder>(
            kControlProducerInstanceHigh)->Append(producer_instance_.high));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt64Builder>(
            kControlProducerInstanceLow)->Append(producer_instance_.low));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt8Builder>(
            kControlKind)->Append(static_cast<std::uint8_t>(kind)));
        ARROW_RETURN_NOT_OK(builder_->GetFieldAs<arrow::UInt64Builder>(
            kControlObservedMonotonicNs)->Append(observed_monotonic_ns));
        if (rows_ == 0U) {
            metadata_.feed_session_epoch = feed_session_epoch_;
        }
        return arrow::Status::OK();
    }

    arrow::Status AppendNullRange(int first, int last) {
        for (int index = first; index <= last; ++index) {
            ARROW_RETURN_NOT_OK(builder_->GetField(index)->AppendNull());
        }
        return arrow::Status::OK();
    }

    std::unique_ptr<arrow::RecordBatchBuilder> builder_;
    std::uint64_t feed_session_epoch_ = 0U;
    ProducerInstanceId producer_instance_{};
    std::size_t rows_ = 0U;
    BatchMetadata metadata_{};
};

std::unique_ptr<ControlRecordBatchBuilder>
ControlRecordBatchBuilder::Create(std::size_t initial_capacity,
                                  std::uint64_t feed_session_epoch,
                                  ProducerInstanceId producer_instance,
                                  std::string* error) {
    if (!CheckInitialCapacity(initial_capacity, error) ||
        feed_session_epoch == 0U ||
        producer_instance == ProducerInstanceId{}) {
        SetError(error, "invalid control batch epoch or producer instance");
        return nullptr;
    }
    auto result = arrow::RecordBatchBuilder::Make(
        ControlArrowSchema(), arrow::default_memory_pool(),
        static_cast<std::int64_t>(initial_capacity));
    if (!result.ok()) {
        SetError(error, result.status().ToString());
        return nullptr;
    }
    return std::unique_ptr<ControlRecordBatchBuilder>(
        new ControlRecordBatchBuilder(std::make_unique<Impl>(
            std::move(*result), feed_session_epoch, producer_instance)));
}

ControlRecordBatchBuilder::ControlRecordBatchBuilder(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ControlRecordBatchBuilder::~ControlRecordBatchBuilder() = default;

bool ControlRecordBatchBuilder::AppendLifecycle(
    const LifecycleControl& record,
    std::string* error) noexcept {
    if (!IsValidLifecycle(record.kind, record.reason)) {
        SetError(error, "invalid lifecycle control kind or reason");
        return false;
    }
    try {
        arrow::Status status = impl_->AppendBase(
            record.kind, record.observed_monotonic_ns);
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlMarket,
                                            kControlObservedSequence);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt8Builder>(
                kControlReason)->Append(
                    static_cast<std::uint8_t>(record.reason));
        }
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlAffectedStream,
                                            kControlDroppedRows);
        }
        if (status.ok()) {
            ++impl_->rows_;
        }
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

bool ControlRecordBatchBuilder::AppendGap(
    const ingest::ChannelGap& record,
    std::string* error) noexcept {
    if (!IsKnownMarket(record.market) ||
        record.first_missing == 0U ||
        record.last_missing < record.first_missing ||
        record.first_present_after_gap <= record.last_missing ||
        record.gap_epoch == 0U) {
        SetError(error, "invalid ChannelGap range or epoch");
        return false;
    }
    try {
        arrow::Status status = impl_->AppendBase(
            ControlKind::kChannelGap, record.detected_monotonic_ns);
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt8Builder>(
                kControlMarket)->Append(static_cast<std::uint8_t>(record.market));
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt32Builder>(
                kControlChannel)->Append(record.channel);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlFirstMissing)->Append(record.first_missing);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlLastMissing)->Append(record.last_missing);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlFirstPresentAfterGap)->Append(
                    record.first_present_after_gap);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlGapEpoch)->Append(record.gap_epoch);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlCumulativeMissing)->Append(
                    record.cumulative_missing_sequences);
        }
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlExpectedSequence,
                                            kControlDroppedRows);
        }
        if (status.ok()) {
            ++impl_->rows_;
        }
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

bool ControlRecordBatchBuilder::AppendFault(
    const ingest::ChannelFault& record,
    std::string* error) noexcept {
    if (!IsKnownMarket(record.market) ||
        !IsChannelFaultReason(record.reason)) {
        SetError(error, "invalid ChannelFault market or reason");
        return false;
    }
    try {
        arrow::Status status = impl_->AppendBase(
            ControlKind::kChannelFault, record.detected_monotonic_ns);
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt8Builder>(
                kControlMarket)->Append(static_cast<std::uint8_t>(record.market));
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt32Builder>(
                kControlChannel)->Append(record.channel);
        }
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlFirstMissing,
                                            kControlCumulativeMissing);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlExpectedSequence)->Append(record.expected_sequence);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlObservedSequence)->Append(record.observed_sequence);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt8Builder>(
                kControlReason)->Append(
                    static_cast<std::uint8_t>(record.reason));
        }
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlAffectedStream,
                                            kControlDroppedRows);
        }
        if (status.ok()) {
            ++impl_->rows_;
        }
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

bool ControlRecordBatchBuilder::AppendOverrun(
    const HotPublishOverrun& record,
    std::string* error) noexcept {
    if (!IsRingStreamKind(record.affected_stream) ||
        record.dropped_rows == 0U) {
        SetError(error, "invalid hot-publish overrun record");
        return false;
    }
    try {
        arrow::Status status = impl_->AppendBase(
            ControlKind::kHotPublishOverrun,
            record.observed_monotonic_ns);
        if (status.ok()) {
            status = impl_->AppendNullRange(kControlMarket,
                                            kControlReason);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt32Builder>(
                kControlAffectedStream)->Append(
                    static_cast<std::uint32_t>(record.affected_stream));
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt32Builder>(
                kControlAffectedShard)->Append(record.affected_shard);
        }
        if (status.ok()) {
            status = impl_->builder_->GetFieldAs<arrow::UInt64Builder>(
                kControlDroppedRows)->Append(record.dropped_rows);
        }
        if (status.ok()) {
            ++impl_->rows_;
        }
        SetError(error, status.ok() ? std::string{} : status.ToString());
        return status.ok();
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return false;
    }
}

BuiltRecordBatch ControlRecordBatchBuilder::Finish(
    std::string* error) noexcept {
    BuiltRecordBatch output{};
    if (impl_->rows_ == 0U) {
        SetError(error, "cannot finish an empty Control batch");
        return output;
    }
    try {
        auto result = impl_->builder_->Flush();
        if (!result.ok()) {
            SetError(error, result.status().ToString());
            return output;
        }
        output.batch = std::move(*result);
        output.metadata = impl_->metadata_;
        impl_->rows_ = 0U;
        impl_->metadata_ = {};
        SetError(error, {});
        return output;
    } catch (const std::exception& exception) {
        SetError(error, exception.what());
        return output;
    }
}

std::size_t ControlRecordBatchBuilder::rows() const noexcept {
    return impl_->rows_;
}

}  // namespace l2flow::arrow_hot
