//! Arbitrary-precision decimal support for `type_overrides`-driven exact
//! NUMERIC/DECIMAL decoding.
//!
//! Pure, engine-agnostic arithmetic and the `type_overrides` string grammar
//! only — the wire-format-specific bits (Postgres's binary NUMERIC digit
//! groups; MySQL/BigQuery's shared plain-decimal-text) live in each
//! decoder, which calls into this module for the shared rescale/overflow
//! logic. Scoped to Arrow `Decimal128` (precision <= 38) only; `Decimal256`
//! is not yet supported (see `transform::plan`, which rejects a `Decimal(P,
//! S)` override with `P > 38` as a config error rather than silently
//! falling back to the lossy `Float64` decode this module exists to fix).

use crate::error::{EtlError, Result};

/// Parse the literal ClickHouse `"Decimal(P, S)"` `type_overrides` syntax
/// (whitespace around the comma/parens tolerated). Returns `None` for
/// anything else — a bare `"Decimal"`, the `DecimalNN(S)` shorthand aliases
/// (not recognized here), or an unrelated override string (e.g. a BigQuery
/// type name like `"NUMERIC"`, which never has parens/digits and so can
/// never collide with this grammar — see `transform::plan`'s call site for
/// why that matters for a non-ClickHouse destination reusing the same
/// `type_overrides` map). `None` is not an error: it means "no Arrow-level
/// override applies to this column", not "the user made a mistake".
pub(crate) fn parse_decimal_override(s: &str) -> Option<(u8, i8)> {
    let rest = s.trim().strip_prefix("Decimal(")?;
    let rest = rest.strip_suffix(')')?;
    let (p_str, s_str) = rest.split_once(',')?;
    let precision: u8 = p_str.trim().parse().ok()?;
    let scale: i8 = s_str.trim().parse().ok()?;
    Some((precision, scale))
}

/// Render an Arrow `Decimal128` mantissa `value` at the given `scale` as its
/// canonical plain-decimal text (e.g. `12345` @ scale 2 -> `"123.45"`), for
/// delivering an exact BigQuery `NUMERIC` over both write paths (Storage Write
/// proto `string`, and insertAll JSON string). No exponent, no thousands
/// separators — the form BigQuery parses into NUMERIC. `scale <= 0` appends
/// zeros (or is a plain integer at scale 0). Uses `unsigned_abs()` so
/// `i128::MIN` can't overflow (unreachable anyway: `P <= 38` bounds
/// `|value| < 10^38 < i128::MAX`).
pub(crate) fn decimal128_to_string(value: i128, scale: i8) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let body = if scale <= 0 {
        if scale == 0 {
            digits
        } else {
            let mut d = digits;
            d.push_str(&"0".repeat((-(scale as i32)) as usize));
            d
        }
    } else {
        let scale = scale as usize;
        if digits.len() > scale {
            let point = digits.len() - scale;
            format!("{}.{}", &digits[..point], &digits[point..])
        } else {
            format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
        }
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}

/// Rescale a non-negative base-10 mantissa from `from_scale` to `to_scale`
/// (either may be negative — Postgres's own native encoding scale is a
/// multiple of 4 and can be negative for large whole numbers), rounding
/// half-away-from-zero on narrowing (matches PostgreSQL's own numeric-cast
/// rounding, rather than round-half-to-even). Returns `None` on arithmetic
/// overflow (caller coerces the value to NULL and counts it).
///
/// Always returns `Some(0)` when `magnitude == 0`, regardless of how large
/// `to_scale - from_scale` is — a required short-circuit, not an
/// optimization: Postgres wire-encodes a literal zero as `ndigits=0,
/// weight=0`, which works out to `native_scale = -4`; rescaling that to a
/// wide target scale (e.g. 38) would otherwise need `10^42`, which overflows
/// i128 and would wrongly NULL out a literal zero.
pub(crate) fn rescale_mantissa(magnitude: i128, from_scale: i32, to_scale: i32) -> Option<i128> {
    if magnitude == 0 {
        return Some(0);
    }
    let diff = to_scale.checked_sub(from_scale)?;
    if diff >= 0 {
        let factor = 10i128.checked_pow(u32::try_from(diff).ok()?)?;
        magnitude.checked_mul(factor)
    } else {
        let shift = diff.checked_neg()?;
        let divisor = 10i128.checked_pow(u32::try_from(shift).ok()?)?;
        let quotient = magnitude / divisor;
        let remainder = magnitude % divisor;
        // `remainder >= divisor / 2` rather than `remainder * 2 >= divisor` —
        // `remainder * 2` can overflow i128 when `divisor` is close to
        // i128::MAX, while `divisor / 2` never can.
        if remainder >= divisor / 2 {
            Some(quotient + 1)
        } else {
            Some(quotient)
        }
    }
}

/// Result of parsing plain decimal text into its sign/magnitude/scale parts.
/// `MagnitudeOverflow` (more digits than fit in an i128, before any
/// rescaling is even attempted) is deliberately not an `Err` — it's the same
/// "value too large for any Decimal128" category as a post-rescale precision
/// overflow, and both coerce to NULL upstream rather than aborting the
/// transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecimalText {
    Ok {
        negative: bool,
        magnitude: i128,
        scale: i32,
    },
    MagnitudeOverflow,
}

/// Parse plain ASCII decimal text — MySQL DECIMAL/NEWDECIMAL and BigQuery
/// NUMERIC/BIGNUMERIC's shared wire shape: optional leading `-` or `+`,
/// digits, optional `.` + digits, no exponent — into `DecimalText`. `Err` is
/// reserved for text that isn't valid decimal syntax at all — a
/// decoder/driver mismatch, mirroring today's `s.parse::<f64>()` failure
/// being a hard error rather than a coercion.
pub(crate) fn parse_decimal_text(s: &str) -> Result<DecimalText> {
    let trimmed = s.trim();
    let (negative, rest) = match trimmed.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    let all_digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    let valid = match (int_part.is_empty(), frac_part.is_empty()) {
        (true, true) => false,
        (false, true) => all_digits(int_part),
        (true, false) => all_digits(frac_part),
        (false, false) => all_digits(int_part) && all_digits(frac_part),
    };
    if !valid {
        return Err(EtlError::decode(format!("invalid decimal text '{s}'")));
    }

    let scale = frac_part.len() as i32;
    let mut magnitude: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        let digit = (b - b'0') as i128;
        magnitude = match magnitude.checked_mul(10).and_then(|m| m.checked_add(digit)) {
            Some(m) => m,
            None => return Ok(DecimalText::MagnitudeOverflow),
        };
    }
    Ok(DecimalText::Ok {
        negative,
        magnitude,
        scale,
    })
}

/// Which kind of otherwise-valid value a decoder coerced to NULL instead of
/// erroring or corrupting the whole transfer — returned by each decoder's
/// per-value append method instead of a bare bool, so the caller bumps the
/// counter (and logs the message) matching the actual reason, instead of
/// conflating date-range and decimal-precision coercions under one count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Coercion {
    None,
    DateRange,
    DecimalOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal128_to_string_renders_canonical_numeric_text() {
        assert_eq!(decimal128_to_string(12345, 2), "123.45");
        assert_eq!(decimal128_to_string(-12345, 2), "-123.45");
        // Fewer integer digits than the scale -> leading "0." with padding.
        assert_eq!(decimal128_to_string(5, 3), "0.005");
        assert_eq!(decimal128_to_string(-5, 3), "-0.005");
        // Scale 0 -> plain integer; negative scale -> trailing zeros.
        assert_eq!(decimal128_to_string(42, 0), "42");
        assert_eq!(decimal128_to_string(42, -2), "4200");
        assert_eq!(decimal128_to_string(0, 9), "0.000000000");
        // A full NUMERIC(38,9)-scale value round-trips exactly (no f64 loss).
        assert_eq!(decimal128_to_string(32_899_999_999, 9), "32.899999999");
    }

    #[test]
    fn rescale_identity_when_scales_match() {
        assert_eq!(rescale_mantissa(12345, 2, 2), Some(12345));
    }

    #[test]
    fn rescale_widens_by_exact_multiplication() {
        assert_eq!(rescale_mantissa(123, 0, 2), Some(12300));
    }

    #[test]
    fn rescale_narrows_without_rounding_when_exact() {
        assert_eq!(rescale_mantissa(1200, 2, 0), Some(12));
    }

    #[test]
    fn rescale_narrows_with_half_away_from_zero_rounding() {
        // 1.5 -> 2 (not banker's rounding, which would give 2 here too by
        // coincidence — the next case pins the actual distinguishing behavior).
        assert_eq!(rescale_mantissa(15, 1, 0), Some(2));
        // 2.5 -> 3 under round-half-away-from-zero; round-half-to-even would
        // give 2. This is the case that actually distinguishes the two rules.
        assert_eq!(rescale_mantissa(25, 1, 0), Some(3));
        // 2.4 -> 2 (rounds down, below the halfway point).
        assert_eq!(rescale_mantissa(24, 1, 0), Some(2));
    }

    #[test]
    fn rescale_zero_short_circuits_regardless_of_scale_delta() {
        // Regression test: Postgres encodes a literal zero at native_scale
        // -4; without the short-circuit, rescaling to scale 38 needs
        // 10^42, which overflows i128 and would wrongly null out a zero.
        assert_eq!(rescale_mantissa(0, -4, 38), Some(0));
        assert_eq!(rescale_mantissa(0, 0, 0), Some(0));
    }

    #[test]
    fn rescale_overflows_to_none_for_large_nonzero_widen() {
        assert_eq!(rescale_mantissa(1, 0, 40), None);
    }

    #[test]
    fn parse_decimal_override_accepts_valid_shapes() {
        assert_eq!(parse_decimal_override("Decimal(18, 2)"), Some((18, 2)));
        // The bug report's own repro spells it with no space.
        assert_eq!(parse_decimal_override("Decimal(30,10)"), Some((30, 10)));
    }

    #[test]
    fn parse_decimal_override_rejects_non_decimal_strings() {
        assert_eq!(parse_decimal_override("Float64"), None);
        assert_eq!(parse_decimal_override("NUMERIC"), None);
        // Shorthand alias, not the "Decimal(P, S)" grammar this parses.
        assert_eq!(parse_decimal_override("Decimal128(10)"), None);
    }

    #[test]
    fn parse_decimal_text_handles_common_shapes() {
        assert_eq!(
            parse_decimal_text("123.456").unwrap(),
            DecimalText::Ok {
                negative: false,
                magnitude: 123456,
                scale: 3
            }
        );
        assert_eq!(
            parse_decimal_text("-42").unwrap(),
            DecimalText::Ok {
                negative: true,
                magnitude: 42,
                scale: 0
            }
        );
        assert_eq!(
            parse_decimal_text("42").unwrap(),
            DecimalText::Ok {
                negative: false,
                magnitude: 42,
                scale: 0
            }
        );
        assert_eq!(
            parse_decimal_text("0.00").unwrap(),
            DecimalText::Ok {
                negative: false,
                magnitude: 0,
                scale: 2
            }
        );
    }

    #[test]
    fn parse_decimal_text_reports_magnitude_overflow_not_an_error() {
        let forty_nines = "9".repeat(40);
        assert_eq!(
            parse_decimal_text(&forty_nines).unwrap(),
            DecimalText::MagnitudeOverflow
        );
    }

    #[test]
    fn parse_decimal_text_errors_on_invalid_syntax() {
        assert!(parse_decimal_text("not_a_number").is_err());
        assert!(parse_decimal_text("-").is_err());
        assert!(parse_decimal_text(".").is_err());
    }
}
