use thiserror::Error;

/// Identifies one order across its lifecycle. A monotonic counter in the
/// backtest; the exchange's order hash in a future live adapter — the engine
/// stays mode-agnostic, like [`MarketId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OrderId(u64);

impl OrderId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw counter — for serializing an order's events so they can be
    /// joined back into one order's history.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(i64);

impl Price {
    /// Micro-ticks per unit probability — a $1 (probability 1.0) share is this
    /// many ticks. Valid prices live in `[0, TICKS_PER_UNIT]`.
    pub const TICKS_PER_UNIT: i64 = 1_000_000;

    /// Build a price from raw micro-ticks, validating it into `[0, TICKS_PER_UNIT]`.
    /// This is the single checked door for prices entering from outside this module.
    pub fn new(ticks: i64) -> Result<Price, PriceError> {
        if (0..=Self::TICKS_PER_UNIT).contains(&ticks) {
            Ok(Price(ticks))
        } else {
            Err(PriceError::OutOfRange(ticks))
        }
    }

    /// Build a price from a probability / dollar amount in `[0.0, 1.0]` (how the
    /// data stores prices), rounding to the nearest micro-tick.
    pub fn from_probability(probability: f64) -> Result<Price, PriceError> {
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(PriceError::InvalidProbability(probability));
        }
        Price::new((probability * Self::TICKS_PER_UNIT as f64).round() as i64)
    }

    /// Build a price from a decimal string, the wire format shared by the
    /// Telonex at-rest data and Polymarket's CLOB feeds: prices arrive as
    /// strings like `"0.085"` on the micro-tick grid in `[0, 1]`.
    ///
    /// The grammar is deliberately strict — digits, at most one point, at most
    /// six fractional digits (the tick grid), no signs/exponents/whitespace.
    /// Anything off-grid is a corruption signal and fails fast rather than
    /// being silently rounded to the nearest tick.
    ///
    /// After the grammar check the value goes through the `f64` path, which is
    /// exact here: a double carries ~16 significant digits, so for ≤6 decimals
    /// in `[0, 1]` the representation error (~1e-16) scaled to ticks (~1e-10)
    /// can never move the rounded result off the true tick.
    pub fn parse_decimal(s: &str) -> Result<Price, PriceError> {
        let invalid = || PriceError::InvalidDecimal(s.to_string());
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());

        let (integer, fraction) = match s.split_once('.') {
            Some((integer, fraction)) => (integer, Some(fraction)),
            None => (s, None),
        };
        if !digits(integer) || !fraction.is_none_or(|f| digits(f) && f.len() <= 6) {
            return Err(invalid());
        }

        Price::from_probability(s.parse().map_err(|_| invalid())?)
    }

    /// Render as the canonical wire decimal string — the exact inverse of
    /// [`Price::parse_decimal`] up to normalization (trailing zeros trimmed:
    /// parsing `"0.0850"` renders back as `"0.085"`). Pure integer formatting,
    /// so exactness needs no floating-point reasoning.
    pub fn to_decimal_string(self) -> String {
        let integer = self.0 / Self::TICKS_PER_UNIT;
        let fraction = self.0 % Self::TICKS_PER_UNIT;
        if fraction == 0 {
            integer.to_string()
        } else {
            let digits = format!("{fraction:06}");
            format!("{integer}.{}", digits.trim_end_matches('0'))
        }
    }

    /// Midpoint of two prices. Floors on an odd tick sum; with grid-aligned
    /// inputs that can't happen.
    pub fn midpoint(self, other: Price) -> Price {
        Price((self.0 + other.0) / 2)
    }

    /// Signed difference `self - other`, in micro-ticks.
    pub fn difference(self, other: Price) -> i64 {
        self.0 - other.0
    }

    /// Value as a probability / dollar amount in `[0.0, 1.0]`.
    pub fn as_f64(self) -> f64 {
        self.0 as f64 / Self::TICKS_PER_UNIT as f64
    }

    /// Raw micro-ticks — the inverse of [`new`](Self::new). Lets callers do
    /// exact tick arithmetic (a quoter stepping a price by one cent) and come
    /// back through the checked door, with no float round-tripping.
    pub fn ticks(self) -> i64 {
        self.0
    }
}

/// Why a `Price` couldn't be built — the `[0,1]` invariant was violated.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum PriceError {
    #[error("price {0} micro-ticks is outside [0, 1_000_000]")]
    OutOfRange(i64),
    #[error("probability {0} is not finite / not in [0.0, 1.0]")]
    InvalidProbability(f64),
    #[error("{0:?} is not a decimal price on the micro-tick grid")]
    InvalidDecimal(String),
}

/// Identifies a market. Backtest uses synthetic ids (`"0"`, `"1"`, …); the live
/// adapter uses Polymarket's real `condition_id`/`token_id`. Same type either way
/// so the engine/portfolio/strategy stay mode-agnostic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId(String);

impl MarketId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_decimal_lands_on_exact_ticks() {
        for (input, ticks) in [
            ("0.085", 85_000),
            ("0.5", 500_000),
            ("0.000001", 1),
            ("0", 0),
            ("1", 1_000_000),
            ("1.000000", 1_000_000),
        ] {
            assert_eq!(Price::parse_decimal(input), Price::new(ticks), "{input:?}");
        }
    }

    #[test]
    fn parse_decimal_rejects_off_grammar_strings() {
        for input in [
            "",
            ".5",
            "0.",
            "+0.5",
            "-0.1",
            "8.5e-2",
            " 0.5",
            "0.5 ",
            "0..5",
            "0.0850001",
            "NaN",
        ] {
            assert!(
                matches!(
                    Price::parse_decimal(input),
                    Err(PriceError::InvalidDecimal(_))
                ),
                "{input:?} should fail the grammar"
            );
        }
    }

    #[test]
    fn to_decimal_string_round_trips_parse_decimal() {
        for (input, canonical) in [
            ("0.085", "0.085"),
            ("0.0850", "0.085"), // normalization: trailing zeros trimmed
            ("0.5", "0.5"),
            ("0.000001", "0.000001"),
            ("0", "0"),
            ("1", "1"),
            ("1.000000", "1"),
        ] {
            assert_eq!(
                Price::parse_decimal(input).unwrap().to_decimal_string(),
                canonical,
                "{input:?}"
            );
        }
    }

    #[test]
    fn parse_decimal_rejects_out_of_range_values() {
        assert!(matches!(
            Price::parse_decimal("1.5"),
            Err(PriceError::InvalidProbability(_))
        ));
    }
}
