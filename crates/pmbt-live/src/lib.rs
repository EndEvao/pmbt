//! Paper trading transport, capture and replay. No order-submission API.
pub mod record;
pub mod transport;

use pmbt::{
    clob::{
        book::BookBuilder,
        decode::{decode_frame, ClobEvent, Side},
    },
    domain::{
        marketstate::MarketState,
        order::OrderSide,
        trade::Trade,
        types::{MarketId, Price},
    },
    engine::{
        paper::{PaperRecorder, PaperSession, PaperStatus},
        Simulation,
    },
    feed::Event,
    fill::sweep::SweepFillModel,
    latency::LatencyProfile,
    strategy::Strategy,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
pub struct ResyncRequired;
impl std::fmt::Display for ResyncRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "book integrity failure; resynchronization required")
    }
}
impl std::error::Error for ResyncRequired {}

/// All parameters affecting results are captured in the session manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub yes_asset: String,
    pub no_asset: String,
    pub strategy: String,
    pub capital: f64,
    pub size: f64,
    pub max_inventory: f64,
    pub fee_rate: f64,
    pub transit_ms: u64,
    pub taker_delay_ms: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.yes_asset.is_empty() || self.no_asset.is_empty() || self.yes_asset == self.no_asset
        {
            return Err("distinct nonempty YES and NO asset IDs are required".into());
        }
        if !matches!(self.strategy.as_str(), "buyandhold" | "quoter") {
            return Err("paper strategy must be buyandhold or quoter".into());
        }
        if [self.capital, self.size, self.max_inventory]
            .iter()
            .any(|x| !x.is_finite() || *x <= 0.)
            || self.max_inventory < self.size
            || !self.fee_rate.is_finite()
            || !(0.0..=1.0).contains(&self.fee_rate)
            || self.transit_ms > 60_000
            || self.taker_delay_ms > 60_000
        {
            return Err(
                "invalid capital, size, inventory, fee rate or latency (maximum 60s)".into(),
            );
        }
        Ok(())
    }
}

/// Capture the normalized clock and raw frame together. Replaying these inputs
/// includes explicit timer and gap decisions, not a new wall-clock schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    Connected {
        at_us: i64,
    },
    Frame {
        at_us: i64,
        received_us: i64,
        raw: String,
    },
    Gap {
        at_us: i64,
        reason: String,
    },
    Tick {
        at_us: i64,
    },
    Stop {
        at_us: i64,
    },
}
impl Input {
    pub fn at_us(&self) -> i64 {
        match self {
            Self::Connected { at_us }
            | Self::Frame { at_us, .. }
            | Self::Gap { at_us, .. }
            | Self::Tick { at_us }
            | Self::Stop { at_us } => *at_us,
        }
    }
}

/// Owns feed synchronization. Both outcome books must be fresh before execution.
pub struct Driver {
    config: Config,
    session: PaperSession,
    builder: BookBuilder,
    books: [Option<MarketState>; 2],
    conditions: [Option<String>; 2],
    synchronized: bool,
    now: i64,
}

impl Driver {
    pub fn new(config: Config, strategies: [Box<dyn Strategy>; 2]) -> Result<Self> {
        config.validate()?;
        let [yes, no] = strategies;
        let mut sim = Simulation::with_latency(LatencyProfile {
            feed: Duration::ZERO,
            transit: Duration::from_millis(config.transit_ms),
            taker_delay: Duration::from_millis(config.taker_delay_ms),
        });
        sim.fee_rate = config.fee_rate;
        let session = PaperSession::new(
            config.capital,
            Box::new(SweepFillModel),
            sim,
            vec![
                (MarketId::new(config.yes_asset.clone()), yes),
                (MarketId::new(config.no_asset.clone()), no),
            ],
        )?;
        Ok(Self {
            config,
            session,
            builder: BookBuilder::new(),
            books: [None, None],
            conditions: [None, None],
            synchronized: false,
            now: 0,
        })
    }

    pub fn status(&self) -> PaperStatus {
        self.session.status()
    }
    pub fn next_deadline(&mut self) -> Option<i64> {
        self.session.next_deadline()
    }

    fn index(&self, asset: &str) -> Option<usize> {
        if asset == self.config.yes_asset {
            Some(0)
        } else if asset == self.config.no_asset {
            Some(1)
        } else {
            None
        }
    }

    fn reset(&mut self, at: i64) -> Result<()> {
        self.session.suspend(at)?;
        self.builder = BookBuilder::new();
        self.books = [None, None];
        self.conditions = [None, None];
        self.synchronized = false;
        Ok(())
    }

    pub fn apply(&mut self, input: &Input, recorder: &mut dyn PaperRecorder) -> Result<()> {
        let at = input.at_us();
        if at < self.now || !(0..=i64::MAX - 120_000_000).contains(&at) {
            return Err("invalid or backwards input time".into());
        }
        self.now = at;
        match input {
            Input::Connected { .. } | Input::Gap { .. } => self.reset(at),
            Input::Stop { .. } => {
                self.synchronized = false;
                Ok(self.session.stop(at)?)
            }
            Input::Tick { .. } => Ok(self.session.advance_to(at, recorder)?),
            Input::Frame { raw, .. } => self.frame(raw, at, recorder),
        }
    }

    fn frame(&mut self, raw: &str, at: i64, recorder: &mut dyn PaperRecorder) -> Result<()> {
        let mut events = decode_frame(raw)?;
        // Keep only the requested outcomes; deltas can contain several assets.
        events.retain_mut(|event| match event {
            ClobEvent::Book(b) => self.index(&b.asset_id).is_some(),
            ClobEvent::PriceChange(p) => {
                p.price_changes
                    .retain(|c| self.index(&c.asset_id).is_some());
                !p.price_changes.is_empty()
            }
            ClobEvent::BestBidAsk(b) => self.index(&b.asset_id).is_some(),
            ClobEvent::LastTradePrice(t) => self.index(&t.asset_id).is_some(),
            ClobEvent::TickSizeChange(t) => self.index(&t.asset_id).is_some(),
        });
        // Do not carry on with a rule change this prototype cannot enforce.
        if events
            .iter()
            .any(|e| matches!(e, ClobEvent::TickSizeChange(_)))
        {
            self.reset(at)?;
            return Err(
                "market tick size changed; restart with reviewed strategy/rule configuration"
                    .into(),
            );
        }
        let snapshots = self.builder.apply_frame(&events, at)?;
        let mut changed = [false, false];
        for b in snapshots {
            let i = self.index(&b.asset_id).ok_or("unexpected book asset")?;
            self.conditions[i] = Some(b.market);
            self.books[i] = Some(MarketState::new(
                MarketId::new(b.asset_id),
                at,
                b.bids.into_boxed_slice(),
                b.asks.into_boxed_slice(),
            )?);
            changed[i] = true;
        }
        if self.conditions[0].is_some()
            && self.conditions[1].is_some()
            && self.conditions[0] != self.conditions[1]
        {
            self.reset(at)?;
            return Err("YES and NO assets belong to different conditions".into());
        }
        let fresh = [&self.config.yes_asset, &self.config.no_asset]
            .iter()
            .all(|id| self.builder.is_fresh(id) == Some(true));
        if self.synchronized && !fresh {
            self.reset(at)?;
            return Err(ResyncRequired.into());
        }
        if !self.synchronized {
            if fresh {
                let books = [
                    self.books[0].as_ref().ok_or("missing YES book")?.at(at),
                    self.books[1].as_ref().ok_or("missing NO book")?.at(at),
                ];
                self.session.synchronize(books, recorder)?;
                self.synchronized = true;
            }
            return Ok(());
        }
        // Trades before resulting snapshots within a batch. Frames retain receive
        // order, even when exchange timestamps are late or share millisecond time.
        for event in &events {
            if let ClobEvent::LastTradePrice(t) = event {
                let side = match t.side {
                    Side::Buy => OrderSide::Buy,
                    Side::Sell => OrderSide::Sell,
                };
                let trade = Trade::try_new(
                    at,
                    Price::parse_decimal(&t.price)?,
                    t.size.parse()?,
                    side,
                    MarketId::new(t.asset_id.clone()),
                )?;
                self.session.process(Event::Trade(trade), recorder)?;
            }
        }
        for (i, did_change) in changed.into_iter().enumerate() {
            if did_change {
                self.session.process(
                    Event::Book(
                        self.books[i]
                            .as_ref()
                            .ok_or("missing changed book")?
                            .clone(),
                    ),
                    recorder,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
