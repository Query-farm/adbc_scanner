#include "adbc_arrow_types.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/main/query_result.hpp"
#include <array>
#include <utility>

namespace adbc_scanner {
using namespace duckdb;

// Arrow allows decimal scales up to the maximum Decimal256 precision.
static constexpr idx_t MAX_WIDE_DECIMAL_SCALE = 76;

// Convert one wide decimal, stored as a little-endian two's-complement integer of
// blob.GetSize() bytes (16 or 32), to a double. The unscaled integer is rendered as
// decimal digits and parsed as "<digits>e-<scale>", so the result is correctly
// rounded (1.5 stays 1.5) -- the same as the postgres/mysql scanners, which parse
// the numeric's text form.
static double WideDecimalToDouble(string_t blob, idx_t scale) {
	const auto size = blob.GetSize();
	if (size != 16 && size != 32) {
		throw InvalidInputException("adbc: unexpected wide decimal byte width %d", size);
	}
	const idx_t n_words = size / sizeof(uint64_t);
	uint64_t words[4];
	memcpy(words, blob.GetData(), size);

	const bool negative = (words[n_words - 1] >> 63) != 0;
	if (negative) {
		// Two's-complement negate to get the magnitude.
		uint64_t carry = 1;
		for (idx_t i = 0; i < n_words; i++) {
			words[i] = ~words[i] + carry;
			carry = (carry && words[i] == 0) ? 1 : 0;
		}
	}

	// Split into 32-bit limbs, most significant first, so we can divide by 10^9
	// using only 64-bit arithmetic.
	uint32_t limbs[8];
	const idx_t n_limbs = n_words * 2;
	for (idx_t i = 0; i < n_words; i++) {
		limbs[n_limbs - 1 - 2 * i] = static_cast<uint32_t>(words[i]);
		limbs[n_limbs - 2 - 2 * i] = static_cast<uint32_t>(words[i] >> 32);
	}

	// Repeatedly divide by 10^9, collecting 9-digit chunks from least significant.
	// 2^256 has 78 digits, so 9 chunks suffice.
	uint32_t chunks[9];
	idx_t n_chunks = 0;
	idx_t first = 0;
	while (first < n_limbs) {
		uint64_t rem = 0;
		for (idx_t i = first; i < n_limbs; i++) {
			const uint64_t cur = (rem << 32) | limbs[i];
			limbs[i] = static_cast<uint32_t>(cur / 1000000000ULL);
			rem = cur % 1000000000ULL;
		}
		chunks[n_chunks++] = static_cast<uint32_t>(rem);
		while (first < n_limbs && limbs[first] == 0) {
			first++;
		}
	}

	char buf[128];
	int len = 0;
	if (negative) {
		buf[len++] = '-';
	}
	if (n_chunks == 0) {
		buf[len++] = '0';
	} else {
		len += snprintf(buf + len, sizeof(buf) - len, "%u", chunks[n_chunks - 1]);
		for (idx_t i = n_chunks - 1; i > 0; i--) {
			len += snprintf(buf + len, sizeof(buf) - len, "%09u", chunks[i - 1]);
		}
	}
	len += snprintf(buf + len, sizeof(buf) - len, "e-%llu", static_cast<unsigned long long>(scale));

	double result;
	if (!TryCast::Operation<string_t, double>(string_t(buf, static_cast<uint32_t>(len)), result, true)) {
		throw InvalidInputException("adbc: could not convert wide decimal %s to DOUBLE", string(buf, len));
	}
	return result;
}

// cast_arrow_duck_t carries no user data, so the scale is baked in per instantiation.
template <idx_t SCALE>
static void WideDecimalCast(ClientContext &, Vector &source, Vector &result, idx_t count) {
	UnaryExecutor::Execute<string_t, double>(source, result, count,
	                                         [](string_t blob) { return WideDecimalToDouble(blob, SCALE); });
}

template <std::size_t... SCALES>
static constexpr std::array<cast_arrow_duck_t, sizeof...(SCALES)> MakeWideDecimalCasts(std::index_sequence<SCALES...>) {
	return {{&WideDecimalCast<SCALES>...}};
}

static constexpr auto WIDE_DECIMAL_CASTS = MakeWideDecimalCasts(std::make_index_sequence<MAX_WIDE_DECIMAL_SCALE + 1>());

// Parse an Arrow decimal format "d:precision,scale[,bitwidth]". Returns false if the
// format is not a decimal or cannot be parsed; DuckDB then reports the error itself.
static bool ParseDecimalFormat(const char *format, int64_t &precision, int64_t &scale, int64_t &bitwidth) {
	if (!format || format[0] != 'd' || format[1] != ':') {
		return false;
	}
	bitwidth = 128;
	int consumed = 0;
	long long p = 0, s = 0, b = 0;
	if (sscanf(format + 2, "%lld,%lld%n", &p, &s, &consumed) != 2) {
		return false;
	}
	const char *rest = format + 2 + consumed;
	if (*rest == ',') {
		if (sscanf(rest + 1, "%lld", &b) != 1) {
			return false;
		}
		bitwidth = b;
	} else if (*rest != '\0') {
		return false;
	}
	precision = p;
	scale = s;
	return true;
}

shared_ptr<duckdb::ArrowType> AdbcGetArrowType(ClientContext &context, ArrowSchema &schema) {
	int64_t precision, scale, bitwidth;
	if (ParseDecimalFormat(schema.format, precision, scale, bitwidth) && precision > Decimal::MAX_WIDTH_DECIMAL &&
	    (bitwidth == 128 || bitwidth == 256) && scale >= 0 && scale <= static_cast<int64_t>(MAX_WIDE_DECIMAL_SCALE)) {
		// DuckDB's DECIMAL tops out at 38 digits. Decimal128/256 share the physical layout of
		// fixed-size binary, so read the values as fixed-size blobs and convert them to DOUBLE.
		auto fixed_size = static_cast<idx_t>(bitwidth / 8);
		auto arrow_type = make_shared_ptr<duckdb::ArrowType>(LogicalType::DOUBLE, make_uniq<ArrowStringInfo>(fixed_size));
		arrow_type->extension_data = make_shared_ptr<ArrowTypeExtensionData>(
		    LogicalType::DOUBLE, LogicalType::BLOB, WIDE_DECIMAL_CASTS[static_cast<idx_t>(scale)]);
		return arrow_type;
	}
	return duckdb::ArrowType::GetArrowLogicalType(context, schema);
}

void AdbcPopulateArrowTableSchema(ClientContext &context, ArrowTableSchema &arrow_table,
                                  const ArrowSchema &arrow_schema) {
	// Mirrors ArrowTableFunction::PopulateArrowTableSchema, swapping in AdbcGetArrowType.
	vector<string> names;
	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(arrow_schema.n_children); col_idx++) {
		const auto &schema = *arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("adbc: released schema passed");
		}
		auto name = schema.name ? string(schema.name) : string();
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	QueryResult::DeduplicateColumns(names);

	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(arrow_schema.n_children); col_idx++) {
		auto &schema = *arrow_schema.children[col_idx];
		arrow_table.AddColumn(col_idx, AdbcGetArrowType(context, schema), names[col_idx]);
	}
}

} // namespace adbc_scanner
