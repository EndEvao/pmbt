//! Taker fees: Polymarket charges the taker (never the maker) a fee in USDC
//! on each fill, `qty × rate × p × (1−p)` — peaking at p=0.5 and symmetric
//! around it, so a trade at 30¢ costs the same as one at 70¢ and trades near
//! the extremes are nearly free.
//!
//! Like the taker delay, the rate is a function of the market's category, so
//! it keys off the `Category` the universe layer already carries. The
//! historical per-market rate lives in the CLOB `fd` field; we model it as
//! config rather than looked-up data (the exact value for a past day is not
//! recoverable). The engine consumes only the resolved `f64` rate — the
//! `Category`-dependent lookup stays here, out of the engine.

use crate::domain::types::Price;
use crate::universe::Category;

/// Polymarket's taker fee rate by category (makers never pay). Our variants
/// map onto the published tiers: crypto 0.07; sports and the "…/other" group
/// (culture, science, other) 0.05; finance/politics/tech 0.04; geopolitics
/// (world events) fee-free.
pub fn fee_rate(category: Category) -> f64 {
    match category {
        Category::Crypto => 0.07,
        Category::Sports | Category::Culture | Category::Science | Category::Other => 0.05,
        Category::Finance | Category::Politics | Category::Tech => 0.04,
        Category::Geopolitics => 0.0,
    }
}

/// One taker fill's fee in USDC: `qty × rate × p × (1−p)`. A `rate` of 0
/// (fees disabled, or a fee-free market) yields 0, exactly the pre-fee
/// behavior.
pub fn taker_fee(rate: f64, price: Price, quantity: f64) -> f64 {
    let p = price.as_f64();
    quantity * rate * p * (1.0 - p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_table_matches_the_published_tiers() {
        assert_eq!(fee_rate(Category::Crypto), 0.07);
        for c in [
            Category::Sports,
            Category::Culture,
            Category::Science,
            Category::Other,
        ] {
            assert_eq!(fee_rate(c), 0.05);
        }
        for c in [Category::Finance, Category::Politics, Category::Tech] {
            assert_eq!(fee_rate(c), 0.04);
        }
        assert_eq!(fee_rate(Category::Geopolitics), 0.0);
    }

    fn fee_at(price: f64) -> f64 {
        taker_fee(0.05, Price::from_probability(price).unwrap(), 100.0)
    }

    #[test]
    fn fee_peaks_at_one_half_and_is_symmetric() {
        // Symmetric around 0.5: 0.3 and 0.7 cost the same.
        assert!((fee_at(0.3) - fee_at(0.7)).abs() < 1e-12);
        // Peak at 0.5 (100 × 0.05 × 0.25 = 1.25), higher than either wing.
        assert!((fee_at(0.5) - 1.25).abs() < 1e-12);
        assert!(fee_at(0.5) > fee_at(0.3));
        // Nearly free at the extremes.
        assert!(fee_at(0.01) < 0.06);
    }

    #[test]
    fn zero_rate_is_free() {
        assert_eq!(
            taker_fee(0.0, Price::from_probability(0.5).unwrap(), 1_000.0),
            0.0
        );
    }
}
