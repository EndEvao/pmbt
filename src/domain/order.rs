use crate::domain::types::{MarketId, OrderId, Price};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good-Till-Cancelled: rests on the book until filled or cancelled.
    Gtc,
    /// Good-Till-Date: rests like GTC, but the exchange retires it on a timer.
    ///
    /// `expires_at` is the **stated** expiry the order carries to the exchange,
    /// in exchange-time microseconds — deliberately the raw number a real
    /// request would send, not a pre-adjusted one, because the exchange applies
    /// its own two rules to it (see [`TimeInForce::GTD_SECURITY_BUFFER_US`] and
    /// [`TimeInForce::GTD_MIN_HORIZON_US`]). Playing those rules is the engine's
    /// job; a strategy that pre-compensated for them would be encoding exchange
    /// policy in the wrong layer.
    Gtd { expires_at: i64 },
    /// Fill-Or-Kill: fill the whole size immediately (as taker) or nothing.
    Fok,
    /// Fill-And-Kill: fill what's immediately available, drop the remainder.
    Fak,
}

impl TimeInForce {
    /// Polymarket retires a GTD order **one minute before** its stated
    /// expiration, as a security threshold. So the life a strategy actually
    /// gets is always a minute shorter than the timestamp it sent.
    pub const GTD_SECURITY_BUFFER_US: i64 = 60 * 1_000_000;

    /// A stated expiration must be at least **three minutes** out; the exchange
    /// rejects anything sooner. With the buffer above, the shortest GTD life
    /// obtainable is 180 - 60 = **120 seconds** — which is why GTD is a
    /// stale-order guard for slower strategies, not a substitute for
    /// cancel/replace at quoting frequency.
    pub const GTD_MIN_HORIZON_US: i64 = 180 * 1_000_000;

    /// The shortest life the exchange actually permits — the floor less the
    /// buffer. Derived, not chosen: 180 - 60 = 120 s.
    pub const GTD_MIN_LIFE_US: i64 = Self::GTD_MIN_HORIZON_US - Self::GTD_SECURITY_BUFFER_US;

    /// A GTD that will really live `life_us` from `now`. This is the docs'
    /// `now + 60 + N` formula, kept here so callers say how long they want the
    /// order to last and the buffer arithmetic happens once, in the one place
    /// that already owns the rule.
    ///
    /// A `life_us` under [`TimeInForce::GTD_MIN_LIFE_US`] produces a stated
    /// expiry the exchange will reject — deliberately not clamped, because
    /// silently stretching a caller's timer would hide the constraint instead
    /// of surfacing it as the rejection it really is.
    pub fn gtd_after(now: i64, life_us: i64) -> Self {
        TimeInForce::Gtd {
            expires_at: now + Self::GTD_SECURITY_BUFFER_US + life_us,
        }
    }

    /// Whether an unmatched remainder joins the book instead of being dropped.
    /// The one predicate the engine should ask — asking `== Gtc` is what
    /// silently excludes GTD from every resting path.
    pub fn rests(self) -> bool {
        matches!(self, TimeInForce::Gtc | TimeInForce::Gtd { .. })
    }

    /// When the exchange will actually pull this order, in exchange-time
    /// microseconds: the stated expiry less the security buffer. `None` for
    /// every other time-in-force — nothing else is on a timer.
    pub fn effective_expiry(self) -> Option<i64> {
        match self {
            TimeInForce::Gtd { expires_at } => Some(expires_at - Self::GTD_SECURITY_BUFFER_US),
            _ => None,
        }
    }

    /// Whether a stated expiry clears the exchange's three-minute floor, given
    /// the moment the order is submitted.
    pub fn horizon_ok(self, now: i64) -> bool {
        match self {
            TimeInForce::Gtd { expires_at } => expires_at - now >= Self::GTD_MIN_HORIZON_US,
            _ => true,
        }
    }
}

/// Pure intent from a strategy: which market, which way, how much, and — for a
/// limit order — at what price and under which time-in-force. Carries no id,
/// timestamp, or status; the engine stamps those when it accepts the request
/// and owns the resulting `Order`'s lifecycle.
///
/// Fields are private so the two constructors are the only doors, which makes
/// the illegal states unconstructable: a `market` (price-less) order can never
/// carry a resting time-in-force, and a `limit` order's price is on-grid *by
/// construction* — it takes a [`Price`], which has no off-grid constructor.
///
/// Not yet enforced: Polymarket also imposes a per-market `minimum_order_size`
/// and a per-market tick (0.01–0.0001). Both are exchange config we don't carry
/// yet, so the only size floor here is `quantity > 0`, and the price grid is
/// `Price`'s 6-decimal grid (finer than any market tick). These tighten once
/// per-market metadata is plumbed in.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRequest {
    market_id: MarketId,
    side: OrderSide,
    quantity: f64,
    limit_price: Option<Price>,
    tif: TimeInForce,
}

impl OrderRequest {
    /// A marketable order — no price cap, so it can never rest. Restricted to
    /// FOK/FAK: a price-less order that asked to rest would be meaningless.
    pub fn market(market_id: MarketId, side: OrderSide, quantity: f64, tif: TimeInForce) -> Self {
        debug_assert!(quantity.is_finite() && quantity > 0.0);
        debug_assert!(
            matches!(tif, TimeInForce::Fok | TimeInForce::Fak),
            "a market (price-less) order can't rest — use FOK or FAK"
        );
        Self {
            market_id,
            side,
            quantity,
            limit_price: None,
            tif,
        }
    }

    /// A limit order at `price`. The price is on-grid by construction, so there
    /// is nothing to validate here.
    pub fn limit(
        market_id: MarketId,
        side: OrderSide,
        quantity: f64,
        price: Price,
        tif: TimeInForce,
    ) -> Self {
        debug_assert!(quantity.is_finite() && quantity > 0.0);
        Self {
            market_id,
            side,
            quantity,
            limit_price: Some(price),
            tif,
        }
    }

    pub fn market_id(&self) -> &MarketId {
        &self.market_id
    }

    pub fn side(&self) -> OrderSide {
        self.side
    }

    pub fn quantity(&self) -> f64 {
        self.quantity
    }

    pub fn limit_price(&self) -> Option<Price> {
        self.limit_price
    }

    pub fn tif(&self) -> TimeInForce {
        self.tif
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderStatus {
    Pending,
    Resting,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
}

pub struct Order {
    order_id: OrderId,
    timestamp: i64,
    quantity: f64,
    status: OrderStatus,
    side: OrderSide,
    /// The worst price this order will trade at, if any. `None` is a market
    /// order — it accepts whatever the book offers.
    limit_price: Option<Price>,
}

impl Order {
    pub fn new(
        order_id: OrderId,
        timestamp: i64,
        quantity: f64,
        side: OrderSide,
        limit_price: Option<Price>,
    ) -> Self {
        debug_assert!(quantity.is_finite() && quantity > 0.0);
        Self {
            order_id,
            timestamp,
            quantity,
            status: OrderStatus::Pending,
            side,
            limit_price,
        }
    }

    /// Would this order trade at `price`? A market order accepts anything; a
    /// limit buy refuses to pay above its price, a limit sell to accept below.
    /// The single rule every fill model consults to bound its walk.
    pub fn accepts_price(&self, price: Price) -> bool {
        match self.limit_price {
            None => true,
            Some(limit) => match self.side {
                OrderSide::Buy => price <= limit,
                OrderSide::Sell => price >= limit,
            },
        }
    }

    pub fn limit_price(&self) -> Option<Price> {
        self.limit_price
    }

    pub fn side(&self) -> OrderSide {
        self.side
    }

    pub fn quantity(&self) -> f64 {
        self.quantity
    }

    pub fn status(&self) -> OrderStatus {
        self.status
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn order_id(&self) -> OrderId {
        self.order_id
    }

    pub fn cancel(&mut self) {
        if self.status == OrderStatus::Filled {
            panic!("Cannot cancel a filled order");
        }
        self.status = OrderStatus::Canceled;
    }
    pub fn fill(&mut self) {
        if self.status == OrderStatus::Canceled {
            panic!("Cannot fill a canceled order");
        }
        self.status = OrderStatus::Filled;
    }
}
