use std::collections::{BTreeSet, VecDeque};

use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;
use crate::domain::order::{Order, OrderRequest, OrderSide, TimeInForce};
use crate::domain::position::Position;
use crate::domain::trade::Trade;
use crate::domain::types::{MarketId, OrderId, Price};
use crate::feed::{Event, EventSource};
use crate::fill::fillmodel::FillModel;
use crate::latency::LatencyProfile;
use crate::portfolio::Portfolio;
use crate::report::record::{NullRecorder, OrderEvent, OrderEventKind, Quotes, Recorder};
use crate::reservation::ReservationLedger;
use crate::residual::ResidualBook;
use crate::resting::{OpenOrders, QueueModel, RestingOrder};
use crate::strategy::{Action, OpenOrder, OrderState, Strategy};

/// Drives one backtest: streams the feed, lets the strategy react to each
/// snapshot, and plays the exchange for whatever it asks — validating requests
/// against the ledger (Polymarket rules: no spending cash you don't have, no
/// shorting), executing accepted ones through the fill model, and booking the
/// fills into the portfolio.
///
/// Assumes a single-market feed: a request is priced and filled against the
/// snapshot that triggered it.
pub struct Engine {
    portfolio: Portfolio,
    fill_model: Box<dyn FillModel>,
    sim: Simulation,
    next_order_id: u64,
    /// Our own maker orders sitting on the book, and what they still hold.
    /// Empty for every marketable-only run, which is why those stay identical.
    resting: OpenOrders,
    reservations: ReservationLedger,
    /// Cancels travelling to the exchange: `(order, when it lands)`. Until a
    /// cancel lands the order is still live and can fill — that gap is a
    /// maker's central risk, so it is modelled rather than assumed away.
    pending_cancels: VecDeque<(OrderId, i64)>,
    /// Callbacks the strategy asked for, earliest first and de-duplicated —
    /// appointments it created rather than ones the order lifecycle implies.
    wakeups: BTreeSet<i64>,
}

/// What the simulation models beyond the raw replay. Future realism toggles
/// (jitter seed) land here instead of growing `Engine::new` forever.
/// `zero()` is the pre-latency, fee-free engine, byte for byte.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Simulation {
    pub latency: LatencyProfile,
    /// Deliver snapshots to the strategy only once the simulation clock
    /// reaches `snapshot.timestamp + latency.feed` — the strategy sees the
    /// world as late as a real observer would, which closes the lookahead
    /// channel for stateful/time-aware strategies (a signal over "the last
    /// 2 s of updates" must not include updates the observer hadn't received
    /// yet). Off = snapshots are seen instantly; execution latency still
    /// applies either way — the exchange always trades on the true book.
    pub perception: bool,
    /// Polymarket's taker fee rate, already resolved from the market's
    /// category (`fees::fee_rate`) by the caller — the engine stays category
    /// agnostic and only multiplies. `0.0` = fees disabled or a fee-free
    /// market, reproducing the pre-fee behavior.
    pub fee_rate: f64,
    /// How much of the line ahead of a resting order to model. See
    /// [`QueueModel`] — this is the one genuinely unobservable quantity in a
    /// maker backtest, so it is a knob rather than a constant.
    pub queue: QueueModel,
}

impl Simulation {
    pub fn zero() -> Self {
        Self::with_latency(LatencyProfile::zero())
    }

    pub fn with_latency(latency: LatencyProfile) -> Self {
        Self {
            latency,
            perception: false,
            fee_rate: 0.0,
            queue: QueueModel::default(),
        }
    }
}

/// What one loop iteration is reacting to.
enum Step {
    /// The market republished its book.
    Book,
    /// A print crossed the tape.
    Trade(Trade),
    /// An appointment came due with the market silent.
    Time,
}

/// The running P&L and counters a run folds into. Grouped so the many methods
/// that accumulate into them take one `&mut Tally` rather than a fistful of
/// `&mut u64` — the clutter that was tripping `too_many_arguments` at six sites.
/// Every field maps straight onto the matching `BacktestResult` field, built
/// once when the feed drains. (`expired`/`expired_resting` are computed at feed
/// end, not folded here, so they stay out.)
#[derive(Default)]
struct Tally {
    realized_pnl: f64,
    fees: f64,
    fills: u64,
    maker_fills: u64,
    taker_fills: u64,
    rejections: u64,
    cancels: u64,
    cancels_missed: u64,
    expired_gtd: u64,
}

/// An accepted order waiting out its latency window. Pending orders are
/// sorted by execution deadline, with submission order breaking ties.
struct Pending {
    order: Order,
    request: OrderRequest,
    execute_at: i64,
}

/// What happened to the position when the feed drained.
///
/// Resolution is pure post-run accounting: the strategy never observes it
/// (knowing the outcome during the run would be lookahead), and redemption
/// never passes through the order pipeline or fill model — it is not a trade.
#[derive(Debug, Clone, PartialEq)]
pub enum Settlement {
    /// No resolution supplied — the market is still open. Any holding is
    /// reported in `BacktestResult::open_position`, unmarked.
    Unresolved,
    /// A resolution was supplied but the run ended flat: nothing to redeem.
    NothingHeld,
    /// The holding redeemed at the resolution price.
    Settled {
        quantity: f64,
        redemption: Price,
        pnl: f64,
    },
}

/// Where a run ended up. `realized_pnl` is gross trading P&L; settlement and
/// fees are broken out separately, with `total_pnl` netting all three, so the
/// breakdown stays legible. Open positions are not marked to market — an
/// unresolved run reports its holding as-is in `open_position`.
#[derive(Debug, Clone, PartialEq)]
pub struct BacktestResult {
    pub initial_capital: f64,
    pub final_cash: f64,
    /// Gross P&L realized by trading during the run (excludes settlement and
    /// fees).
    pub realized_pnl: f64,
    /// Net: `realized_pnl + settlement pnl − fees`.
    pub total_pnl: f64,
    /// Total taker fees paid over the run (always ≥ 0). Charged per fill on
    /// both buys and sells; already deducted from `final_cash`.
    pub fees: f64,
    pub settlement: Settlement,
    /// The holding left when an *unresolved* run ends: quantity + cost basis,
    /// unmarked. `None` when flat or settled. (Single-market feed: at most
    /// one position exists.)
    pub open_position: Option<Position>,
    /// Executions: one per marketable order (however many levels it swept),
    /// plus one per resting-order *match event* — a maker order hit by five
    /// trades counts five, because each is a discrete execution.
    pub fills: u64,
    /// The maker/taker split of `fills`. Maker fills pay no fee, so `fees`
    /// must be 0 whenever `taker_fills` is 0.
    pub maker_fills: u64,
    pub taker_fills: u64,
    pub rejections: u64,
    /// Orders the strategy successfully withdrew. Counts effective cancels
    /// only: cancelling an id that already filled or expired is a normal race
    /// against the previous tick's view, so it is ignored and uncounted.
    pub cancels: u64,
    /// Cancels that arrived too late — the order had already filled. The direct
    /// measure of being picked off: the strategy tried to pull a quote and the
    /// market got there first. Zero under zero latency, where cancels are
    /// instantaneous.
    pub cancels_missed: u64,
    /// Orders still inside their latency window when the feed ended — killed,
    /// never executed. Kept SEPARATE from `rejections` on purpose: a
    /// rejection is the exchange saying no (meaningful live), an expiry is a
    /// data-boundary artifact whose count scales with the latency setting —
    /// folding them together would corrupt cross-profile comparisons.
    /// Revisit at live trading, where "feed end" becomes "market close".
    pub expired: u64,
    /// Resting (maker) orders still on the book when the feed ended. Separate
    /// from `expired`, which counts orders that died inside their *latency*
    /// window: that count scales with the latency preset, this one with the
    /// strategy's patience, and merging them would blur both.
    pub expired_resting: u64,
    /// GTD orders the exchange retired on their timer. A *modelled exchange
    /// action*, so it is kept apart from both `expired` counts above — those
    /// are artifacts of the data running out, this one would happen identically
    /// in live trading.
    pub expired_gtd: u64,
    /// Unfilled size still resting at feed end, and the cash/shares it held —
    /// so a maker strategy that never filled is visible rather than silent.
    pub resting_quantity: f64,
    pub resting_reserved: f64,
}

impl Engine {
    pub fn new(initial_capital: f64, fill_model: Box<dyn FillModel>, sim: Simulation) -> Self {
        Self {
            portfolio: Portfolio::new(initial_capital),
            fill_model,
            sim,
            next_order_id: 0,
            resting: OpenOrders::new(),
            reservations: ReservationLedger::new(),
            pending_cancels: VecDeque::new(),
            wakeups: BTreeSet::new(),
        }
    }

    /// Run the strategy over the feed to completion, then settle if the
    /// market's resolution is known. Consumes the engine — a run leaves the
    /// ledger dirty, so one engine is one backtest.
    ///
    /// `resolution` is **token-relative**: the redemption value of the traded
    /// token ($0 lost / $1 won), not a market outcome index. `None` means the
    /// market is unresolved — no settlement, and any holding is reported
    /// unmarked in the result.
    ///
    /// Fails fast: the first bad data line aborts the run, because a P&L over
    /// silently skipped data isn't a result.
    pub fn run<S: EventSource>(
        self,
        feed: S,
        strategy: &mut dyn Strategy,
        resolution: Option<Price>,
    ) -> Result<BacktestResult, S::Error> {
        // A plain run is a recorded run with a recorder that keeps nothing —
        // the no-op default methods inline away, so this is byte-identical to
        // the pre-recorder engine. Callers that want the time series call
        // `run_recorded` with a real recorder.
        self.run_recorded(feed, strategy, resolution, &mut NullRecorder)
    }

    /// Like [`run`](Self::run), but drives `recorder` through the run so a
    /// caller can capture the price/equity/depth time series. The recorder is a
    /// read-only observer: it sees each snapshot and each fill but cannot alter
    /// the run, so recording never changes a result.
    pub fn run_recorded<S: EventSource>(
        mut self,
        mut feed: S,
        strategy: &mut dyn Strategy,
        resolution: Option<Price>,
        recorder: &mut dyn Recorder,
    ) -> Result<BacktestResult, S::Error> {
        let mut tally = Tally::default();
        let feed_us = self.sim.latency.feed.as_micros() as i64;
        // Orders waiting out their latency window. Validation runs twice,
        // mirroring the exchange's documented taker-delay lifecycle: a cheap
        // sanity gate at submission, then the full gates + FOK fill against
        // the snapshot the order actually executes on.
        let mut pending: VecDeque<Pending> = VecDeque::new();
        // Under perception delay, snapshots the observer hasn't received yet.
        // Whatever is still here when the data ends was simply never seen —
        // dropped, not counted: it is data the observer's feed hadn't
        // delivered, not orders (those are `expired`).
        let mut unseen: VecDeque<MarketState> = VecDeque::new();
        // The feed's final instant — when anything still open expires.
        let mut last_ts = 0i64;

        // The last book the market showed. On a tick it is carried forward,
        // restamped, so `state.timestamp` is always "now".
        let mut last_book: Option<MarketState> = None;
        // Execution depth survives clock/trade events until the next real book.
        let mut depleted: Option<MarketState> = None;
        // Under perception, the last book the STRATEGY was shown — the delayed
        // one, not the newer one being withheld. Handing it the current book on
        // a tick would leak exactly what perception exists to prevent. Without
        // perception the two are the same book, so `last_book` is used directly
        // and nothing is cloned.
        let mut last_seen: Option<MarketState> = None;
        loop {
            let deadline = self.next_deadline(&pending, &unseen, feed_us, last_ts);
            let Some(event) = feed.next_event(deadline) else {
                break;
            };
            // Fold the event into "now" plus what it carries. A book replaces
            // the market's last word; a trade and a time event both leave it
            // standing, restamped, so `state.timestamp` is always now.
            let (now, step) = match event? {
                Event::Book(state) => {
                    let now = state.timestamp;
                    last_book = Some(state);
                    depleted = None;
                    (now, Step::Book)
                }
                Event::Trade(trade) => (trade.timestamp, Step::Trade(trade)),
                Event::Time(now) => (now, Step::Time),
            };
            // Anything before the first book has no market to act against. A
            // source is free to emit one; the engine simply has nothing to do.
            let Some(book) = last_book.as_ref() else {
                continue;
            };
            // Only a carried-forward book needs restamping — a book event
            // already bears its own instant, so the common path never clones.
            let carried = match step {
                Step::Book => None,
                _ => Some(book.at(now)),
            };
            let state = carried.as_ref().unwrap_or(book);
            let trades: &[Trade] = match &step {
                Step::Trade(trade) => std::slice::from_ref(trade),
                _ => &[],
            };
            let is_tick = !matches!(step, Step::Book);
            last_ts = now;
            // Trades in this window are executions the exchange already
            // processed, so resting orders they filled are advanced before
            // anything else looks at the new book. An order placed during this
            // snapshot therefore can't fill on trades that predate it.
            self.advance_resting(trades, &mut tally, recorder);
            // Only a book the market printed can be reconciled against. A tick
            // carries the previous one forward unchanged, so there is no
            // shrinkage to explain — and reconciling there would discard the
            // trade volume that explains the *next* real book, handing our
            // queue a free advance it never earned.
            // Unconditional on a real book, even with nothing resting: this is
            // also what clears the trade tally, and skipping it would leave
            // stale volume to be subtracted from some later, unrelated book.
            if !is_tick {
                self.resting.observe(state, self.sim.queue);
            }
            // GTD orders the exchange has retired by now leave next: they are
            // gone before the strategy can act on this snapshot, and before any
            // cancel aimed at them lands (such a cancel then correctly counts
            // as missed — there was nothing left to pull).
            self.expire_gtd(state.timestamp, &mut tally, recorder);
            // Cancels land only AFTER this snapshot's trades have had their
            // chance. Reverse the two and a maker could always pull in time,
            // which is precisely the risk being modelled.
            self.drain_cancels(state.timestamp, &mut pending, &mut tally, recorder);

            // One residual per snapshot, shared by both drains below, so every
            // order executing at this instant walks the depth its predecessors
            // left rather than a fresh full book.
            let mut book = ResidualBook::with_depleted(state, depleted.take());

            // Orders from earlier snapshots whose time has come execute
            // BEFORE the strategy sees this snapshot (they were on the
            // exchange first). Execution always runs on the true book.
            self.execute_due(&mut pending, &mut book, &mut tally, recorder);

            // A tick is time passing, not the market speaking. The strategy's
            // `on_market_state` is a reaction to a *book*, so it never fires on
            // the carried-forward one — that would have it react again to a
            // snapshot it has already seen. Time-triggered behaviour gets its
            // own hook instead.
            //
            // A perception delivery is not an exception to that rule: what gets
            // handed over is a real book the observer only *now* received. The
            // tick is how it arrives on time rather than whenever the market
            // next happens to move.
            if self.sim.perception {
                if !is_tick {
                    unseen.push_back(state.clone());
                }
                // Everything whose delivery time has come, oldest first — on a
                // tick that is precisely the appointment we woke for.
                while unseen
                    .front()
                    .is_some_and(|s| s.timestamp + feed_us <= state.timestamp)
                {
                    let seen = unseen.pop_front().expect("front checked above");
                    self.decide(
                        &seen,
                        state.timestamp,
                        strategy,
                        &mut pending,
                        &mut tally,
                        recorder,
                    );
                    last_seen = Some(seen);
                }
            } else if !is_tick {
                self.decide(
                    state,
                    state.timestamp,
                    strategy,
                    &mut pending,
                    &mut tally,
                    recorder,
                );
            }

            // Time passed with the market silent. The strategy hears about it
            // through its own hook, on the last book it was shown — never the
            // carried-forward one, which under perception it is not entitled to
            // see yet.
            let wake_due = self.wakeups.first().is_some_and(|&at| at <= now);
            if is_tick || wake_due {
                let shown = if self.sim.perception {
                    last_seen.as_ref()
                } else {
                    last_book.as_ref()
                };
                if let Some(seen) = shown.map(|s| s.at(state.timestamp)) {
                    self.wake(&seen, strategy, &mut pending, &mut tally, recorder);
                }
            }

            // With zero latency `execute_at == state.timestamp`, so orders
            // execute against their own triggering snapshot — the pre-latency
            // semantics, exactly. With a real delay this drain is a no-op.
            self.execute_due(&mut pending, &mut book, &mut tally, recorder);

            depleted = book.into_depleted();

            // Every event can change the portfolio. Keep the book/depth
            // notification separate from carried-mark portfolio samples.
            let quotes = Quotes {
                bid: self.resting.best(OrderSide::Buy).map(|p| p.as_f64()),
                ask: self.resting.best(OrderSide::Sell).map(|p| p.as_f64()),
            };
            if is_tick {
                recorder.on_event(state, &self.portfolio, quotes);
            } else {
                recorder.on_snapshot(state, &self.portfolio, quotes);
            }
        }

        // Orders still in flight when the data ends are killed, not judged —
        // see the `expired` field docs for why they are counted separately.
        let expired = pending.len() as u64;
        // Same for orders still resting: whatever the feed didn't fill dies
        // here, releasing its hold. Snapshot the size first, for the result.
        let resting_quantity = self.resting.total_remaining();
        let resting_reserved = self.resting.total_reserved();
        let expired_resting = self.expire_resting(last_ts, recorder);

        let (settlement, open_position) = self.settle(resolution);
        let settlement_pnl = match &settlement {
            Settlement::Settled { pnl, .. } => *pnl,
            Settlement::Unresolved | Settlement::NothingHeld => 0.0,
        };

        Ok(BacktestResult {
            initial_capital: self.portfolio.initial_capital(),
            final_cash: self.portfolio.cash(),
            realized_pnl: tally.realized_pnl,
            total_pnl: tally.realized_pnl + settlement_pnl - tally.fees,
            fees: tally.fees,
            settlement,
            open_position,
            fills: tally.fills,
            maker_fills: tally.maker_fills,
            taker_fills: tally.taker_fills,
            rejections: tally.rejections,
            cancels: tally.cancels,
            cancels_missed: tally.cancels_missed,
            expired,
            expired_resting,
            expired_gtd: tally.expired_gtd,
            resting_quantity,
            resting_reserved,
        })
    }

    /// One strategy decision on a (possibly late-delivered) snapshot: the
    /// strategy sees `seen` and the *current* portfolio; submission sanity
    /// runs against `seen` (the book the order was conceived against — the
    /// authoritative checks re-run at execution); the order's execution time
    /// is anchored to the decision snapshot's exchange timestamp, so the
    /// formula is identical with perception on or off.
    fn decide(
        &mut self,
        seen: &MarketState,
        now: i64,
        strategy: &mut dyn Strategy,
        pending: &mut VecDeque<Pending>,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        let view = self.open_orders(pending);
        let actions = strategy.on_market_state(seen, &self.portfolio, &view);
        self.apply(actions, seen, now, pending, tally, recorder);
    }

    /// Our live orders, in flight and resting. Built fresh each call so it
    /// always reflects true order state — unlike the market snapshot, this
    /// never lags under perception delay: order acknowledgements come over the
    /// private channel, not the public market feed.
    fn open_orders(&self, pending: &VecDeque<Pending>) -> Vec<OpenOrder> {
        pending
            .iter()
            .map(|p| OpenOrder {
                id: p.order.order_id(),
                side: p.request.side(),
                quantity: p.request.quantity(),
                limit_price: p.request.limit_price(),
                state: OrderState::Pending,
            })
            .chain(self.resting.iter().map(|o| OpenOrder {
                id: o.id,
                side: o.side,
                quantity: o.remaining,
                limit_price: Some(o.price),
                state: OrderState::Resting,
            }))
            .collect()
    }

    /// Call the strategy's time hook and apply whatever it asks for. `seen` is
    /// the last book *this strategy* was shown, restamped to now — see
    /// [`Strategy::on_time`].
    fn wake(
        &mut self,
        seen: &MarketState,
        strategy: &mut dyn Strategy,
        pending: &mut VecDeque<Pending>,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        // Requests that have come due are spent, whether or not this wake-up
        // was theirs — a callback that already fired must not fire again.
        self.wakeups.retain(|&at| at > seen.timestamp);
        let view = self.open_orders(pending);
        let actions = strategy.on_time(seen, &self.portfolio, &view);
        // On a wake-up the book handed over IS restamped to now, so they agree.
        let now = seen.timestamp;
        self.apply(actions, seen, now, pending, tally, recorder);
    }

    /// Apply an ordered list of strategy actions.
    fn apply(
        &mut self,
        actions: Vec<Action>,
        seen: &MarketState,
        now: i64,
        pending: &mut VecDeque<Pending>,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        for action in actions {
            match action {
                Action::WakeAt(at) => {
                    // Checked against the CLOCK, not against `seen` — under
                    // perception those differ, and a strategy reasoning from
                    // the book it was shown ("a second from this") routinely
                    // names an instant already gone. Such a request can never
                    // be honoured on time, so it is dropped here rather than
                    // sitting in the set forever, unfireable and unpruned.
                    if at > now {
                        self.wakeups.insert(at);
                    }
                }
                Action::Cancel(id) => {
                    // A cancel is a message to the exchange, so it travels for
                    // feed+transit (no taker hold — it aggresses nothing).
                    let lands_at = seen.timestamp + self.sim.latency.placement_delay_us();
                    self.pending_cancels.push_back((id, lands_at));
                    // Anything already due lands now. With zero latency that is
                    // this very cancel, so a cancel-then-place pair still frees
                    // its hold before the replacement asks for one.
                    self.drain_cancels(seen.timestamp, pending, tally, recorder);
                }
                Action::Place(request) => {
                    self.place(request, seen, pending, tally, recorder);
                }
            }
        }
    }

    /// The engine's next appointment after `now`, if any: when an order's
    /// latency window elapses, when a cancel lands, when a GTD expires.
    ///
    /// This is what turns the feed from a data stream into a clock. Anything at
    /// or before `now` was already handled by this iteration's drains, so only
    /// strictly-future appointments count — otherwise the source would tick
    /// forever on an instant that is already past.
    fn next_deadline(
        &self,
        pending: &VecDeque<Pending>,
        unseen: &VecDeque<MarketState>,
        feed_us: i64,
        now: i64,
    ) -> Option<i64> {
        // Both queues are ordered by their deadline,
        // so the front is the earliest.
        let candidates = [
            pending.front().map(|p| p.execute_at),
            self.pending_cancels.front().map(|&(_, at)| at),
            self.resting.iter().filter_map(|o| o.expires_at).min(),
            // A snapshot the observer has not received yet arrives on its own
            // schedule too. Without this the delay is not `feed` at all — it is
            // "feed, rounded up to whenever the market next moves", which on a
            // quiet book is unboundedly worse than the latency being modelled.
            unseen
                .front()
                .filter(|_| self.sim.perception)
                .map(|s| s.timestamp + feed_us),
            // Callbacks the strategy asked for.
            self.wakeups.first().copied(),
        ];
        candidates
            .into_iter()
            .flatten()
            .filter(|&at| at > now)
            .min()
    }

    /// Retire every resting GTD order whose time has come, releasing what it
    /// held. The exchange does this on its own schedule — no message travels,
    /// so unlike a cancel it cannot lose a race to a trade.
    ///
    /// Swept right after the trades, for the same reason cancels are: prints in
    /// this snapshot's window are executions the exchange already made, and an
    /// order the exchange filled cannot then expire unfilled. See
    /// [`OpenOrders::expire`] for the timing approximation, which is small on an
    /// active book and emphatically not on a sparse one.
    fn expire_gtd(&mut self, now: i64, tally: &mut Tally, recorder: &mut dyn Recorder) {
        for order in self.resting.expire(now) {
            if order.reserved > 0.0 {
                match order.side {
                    OrderSide::Buy => self.reservations.release_cash(order.reserved),
                    OrderSide::Sell => self
                        .reservations
                        .release_shares(&order.market_id, order.reserved),
                }
            }
            Self::note_order(
                recorder,
                now,
                order.id,
                OrderEventKind::Expired,
                order.side,
                Some(order.price),
                order.remaining,
            );
            tally.expired_gtd += 1;
        }
    }

    /// Apply every cancel that has reached the exchange by `now`.
    ///
    /// A cancel that finds nothing has **lost the race**: the order it meant to
    /// pull already filled (or expired). That is not an error — it is exactly
    /// the adverse selection a maker suffers — so it is counted separately in
    /// `missed` rather than silently dropped.
    ///
    /// Approximation, stated: a cancel takes effect at the *end* of the snapshot
    /// in which it becomes due, so it loses to every trade in that snapshot's
    /// window — even to prints that arrived after it technically landed. It
    /// biases toward *more* adverse selection, the safe direction for a
    /// backtest. Exact interleaving would need trades and cancel-landings
    /// merged by timestamp within the window.
    ///
    /// The error is one snapshot interval, which is 4 ms at the median on the
    /// dev data (against a 13 ms dublin cancel) but has a long tail on a quiet
    /// book — see [`OpenOrders::expire`], which measures it. That tail matters
    /// less here than for expiries: a snapshot gap means no book updates, and a
    /// cancel that arrives late into a market where nothing printed had nothing
    /// to be picked off by.
    fn drain_cancels(
        &mut self,
        now: i64,
        pending: &mut VecDeque<Pending>,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        while let Some(&(id, lands_at)) = self.pending_cancels.front() {
            if lands_at > now {
                break; // still in flight — and the order is still live
            }
            self.pending_cancels.pop_front();
            if self.cancel(id, pending, now, recorder) {
                tally.cancels += 1;
            } else {
                tally.cancels_missed += 1;
            }
        }
    }

    /// Withdraw one of our live orders, in flight or resting, releasing
    /// whatever it still held. Returns whether anything was actually removed:
    /// cancelling an order that has since filled or expired is a normal race
    /// (the strategy acted on the previous tick's view), not an error.
    ///
    /// Cancels take effect **immediately**. Real cancels travel to the exchange
    /// and can lose the race against an incoming fill — the adverse-selection
    /// case worth modelling once cancellation windows land; until then a
    /// strategy here can always pull a quote in time, which flatters it.
    fn cancel(
        &mut self,
        id: OrderId,
        pending: &mut VecDeque<Pending>,
        ts: i64,
        recorder: &mut dyn Recorder,
    ) -> bool {
        if let Some(index) = pending.iter().position(|p| p.order.order_id() == id) {
            let cancelled = pending.remove(index).expect("index just found");
            self.release(Self::commitment(&cancelled.request), &cancelled.request);
            Self::note_order(
                recorder,
                ts,
                id,
                OrderEventKind::Cancelled,
                cancelled.request.side(),
                cancelled.request.limit_price(),
                cancelled.request.quantity(),
            );
            return true;
        }
        if let Some(order) = self.resting.remove(id) {
            if order.reserved > 0.0 {
                match order.side {
                    OrderSide::Buy => self.reservations.release_cash(order.reserved),
                    OrderSide::Sell => self
                        .reservations
                        .release_shares(&order.market_id, order.reserved),
                }
            }
            Self::note_order(
                recorder,
                ts,
                id,
                OrderEventKind::Cancelled,
                order.side,
                Some(order.price),
                order.remaining,
            );
            return true;
        }
        false
    }

    /// Report one moment in an order's life to the recorder. Kept in one place
    /// so every lifecycle point reports the same shape.
    fn note_order(
        recorder: &mut dyn Recorder,
        ts: i64,
        id: OrderId,
        kind: OrderEventKind,
        side: OrderSide,
        price: Option<Price>,
        quantity: f64,
    ) {
        recorder.on_order(&OrderEvent {
            ts,
            id: id.value(),
            kind,
            side: match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            price: price.map(|p| p.as_f64()),
            quantity,
        });
    }

    /// Release a placement hold of `amount` for `request`'s side/market.
    fn release(&mut self, amount: f64, request: &OrderRequest) {
        if amount <= 0.0 {
            return;
        }
        match request.side() {
            OrderSide::Buy => self.reservations.release_cash(amount),
            OrderSide::Sell => self
                .reservations
                .release_shares(request.market_id(), amount),
        }
    }

    /// Gate a new request and, if it passes, hold its resources and queue it
    /// for its latency window.
    fn place(
        &mut self,
        request: OrderRequest,
        seen: &MarketState,
        pending: &mut VecDeque<Pending>,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        if !self.can_execute(&request, seen) {
            tally.rejections += 1;
            return;
        }
        // A GTD expiring too soon is refused by the exchange outright, so it
        // never reaches the book — a rejection, not an expiry.
        if !request.tif().horizon_ok(seen.timestamp) {
            tally.rejections += 1;
            return;
        }
        // A limit order commits its resources here, at placement, so a burst of
        // orders can't collectively overdraw during the latency window: each is
        // gated against what the previous ones already hold.
        let commitment = Self::commitment(&request);
        if commitment > 0.0 {
            let affordable = match request.side() {
                OrderSide::Buy => {
                    self.portfolio.cash() - self.reservations.reserved_cash() >= commitment
                }
                // Free-inventory cover was already checked by `can_execute`.
                OrderSide::Sell => true,
            };
            if !affordable {
                tally.rejections += 1;
                return;
            }
            match request.side() {
                OrderSide::Buy => self.reservations.reserve_cash(commitment),
                OrderSide::Sell => self
                    .reservations
                    .reserve_shares(request.market_id(), commitment),
            }
        }
        let order = self.accept(&request, seen);
        Self::note_order(
            recorder,
            seen.timestamp,
            order.order_id(),
            OrderEventKind::Placed,
            request.side(),
            request.limit_price(),
            request.quantity(),
        );
        // A limit order only waits to *reach* the book; the exchange's taker
        // hold brakes aggressing orders, and a resting order adds liquidity
        // instead of taking it. A market order always aggresses, so it serves
        // the full hold.
        let delay = if request.limit_price().is_some() {
            self.sim.latency.placement_delay_us()
        } else {
            self.sim.latency.execution_delay_us()
        };
        let execute_at = seen.timestamp + delay;
        // Limits and market orders have different delays. Insert after equal
        // deadlines so submission order remains the tie-breaker.
        let index = pending.partition_point(|p| p.execute_at <= execute_at);
        pending.insert(
            index,
            Pending {
                order,
                request,
                execute_at,
            },
        );
    }

    /// Execute every pending order whose latency window has elapsed at this
    /// snapshot, oldest first. This is the authoritative validation pass —
    /// two-phase, mirroring a real FOK lifecycle: gates that don't need a
    /// price (`can_execute`: side present, inventory *as of now*), then the
    /// fill model computes what would fill (`None` = killed on insufficient
    /// depth), then the fund check against the actual walked cost — a sweep
    /// costs more than touch × quantity, so affordability can only be judged
    /// after the walk.
    fn execute_due(
        &mut self,
        pending: &mut VecDeque<Pending>,
        book: &mut ResidualBook,
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        // Timestamps come from the pristine snapshot: consuming depth does not
        // move the clock.
        let state = book.pristine();
        while pending
            .front()
            .is_some_and(|p| p.execute_at <= state.timestamp)
        {
            let due = pending.pop_front().expect("front checked above");
            // The order's own hold is released before its gates run, so the
            // money it reserved at placement is available to the fill it was
            // reserved for — and never counts against itself in `can_afford`.
            let commitment = Self::commitment(&due.request);
            if commitment > 0.0 {
                match due.request.side() {
                    OrderSide::Buy => self.reservations.release_cash(commitment),
                    OrderSide::Sell => self
                        .reservations
                        .release_shares(due.request.market_id(), commitment),
                }
            }

            // Both gates read the residual: if an earlier order this instant
            // already ate a side, there is nothing there for this one.
            if !self.can_execute(&due.request, book.current()) {
                tally.rejections += 1;
                continue;
            }
            // What the book will match right now, bounded by any limit price.
            let prospective = self.fill_model.fill(&due.order, book.current());
            let matched: f64 = prospective.iter().map(|f| f.quantity).sum();

            // Time-in-force decides the unmatched remainder's fate. FOK is
            // all-or-nothing; FAK takes what it can and drops the rest; GTC
            // rests it as a maker order.
            let unmatched = due.request.quantity() - matched;
            let kill_whole = due.request.tif() == TimeInForce::Fok && unmatched > 0.0;
            if kill_whole || (matched == 0.0 && !due.request.tif().rests()) {
                tally.rejections += 1;
                continue;
            }
            if !self.can_afford(&due.request, &prospective) {
                tally.rejections += 1;
                continue;
            }

            for fill in prospective.iter() {
                tally.realized_pnl += self.portfolio.apply(due.request.market_id(), fill);
                recorder.on_fill(state.timestamp, due.request.side(), fill);
                // Taker fee on every taker fill, buy or sell — the taker always
                // pays. Symmetric in side, so it depends only on price/size.
                let fee = crate::fees::taker_fee(self.sim.fee_rate, fill.price, fill.quantity);
                self.portfolio.charge_fee(fee);
                tally.fees += fee;
            }
            if matched > 0.0 {
                tally.fills += 1; // executed orders: a multi-level sweep counts once
                tally.taker_fills += 1;
                // The depth this order took is gone for the rest of the instant.
                book.consume(&prospective);
            }

            // GTC/GTD: whatever didn't cross joins the book as a maker order,
            // and re-holds the resources for the size still outstanding.
            if due.request.tif().rests() && unmatched > 0.0 {
                if let Some(limit) = due.request.limit_price() {
                    let reserved = match due.request.side() {
                        OrderSide::Buy => {
                            let cash = limit.as_f64() * unmatched;
                            self.reservations.reserve_cash(cash);
                            cash
                        }
                        OrderSide::Sell => {
                            self.reservations
                                .reserve_shares(due.request.market_id(), unmatched);
                            unmatched
                        }
                    };
                    Self::note_order(
                        recorder,
                        state.timestamp,
                        due.order.order_id(),
                        OrderEventKind::Rested,
                        due.request.side(),
                        Some(limit),
                        unmatched,
                    );
                    // Seed the line ahead of us from the ladder: the displayed
                    // size at our price IS the queue we just joined the back
                    // of. A price nobody occupies seeds 0 — we are genuinely
                    // first there.
                    let displayed = book.current().size_at(due.request.side(), limit);
                    let queue_ahead = match self.sim.queue {
                        QueueModel::Front => 0.0,
                        QueueModel::Proportional | QueueModel::TradesOnly => displayed,
                    };
                    self.resting.insert(RestingOrder {
                        // Net of the exchange's security buffer: what the
                        // strategy stated is not when the order actually goes.
                        expires_at: due.request.tif().effective_expiry(),
                        queue_ahead,
                        last_displayed: displayed,
                        id: due.order.order_id(),
                        market_id: due.request.market_id().clone(),
                        side: due.request.side(),
                        price: limit,
                        remaining: unmatched,
                        placed_at: state.timestamp,
                        reserved,
                    });
                }
            }
        }
    }

    /// Fill resting (maker) orders against the trades that printed since the
    /// previous snapshot: a trade reaching a resting order's price is what
    /// fills it. See [`crate::resting`] for the matching rule and, importantly,
    /// the front-of-queue optimism it assumes.
    ///
    /// Maker fills pay **no fee** — Polymarket charges takers only.
    fn advance_resting(
        &mut self,
        trades: &[Trade],
        tally: &mut Tally,
        recorder: &mut dyn Recorder,
    ) {
        if self.resting.is_empty() {
            return;
        }
        // Trades drain the queue and fill. Reconciling against the book is a
        // separate step, because it may only happen on a book the market
        // actually printed — see `OpenOrders::observe`.
        for maker in self.resting.fill_from(trades) {
            tally.realized_pnl += self.portfolio.apply(&maker.market_id, &maker.fill);
            recorder.on_fill(maker.fill.timestamp, maker.fill.side, &maker.fill);
            // Release the hold this fill consumed; the cash actually spent moved
            // in `apply`.
            if maker.released > 0.0 {
                match maker.fill.side {
                    OrderSide::Buy => self.reservations.release_cash(maker.released),
                    OrderSide::Sell => self
                        .reservations
                        .release_shares(&maker.market_id, maker.released),
                }
            }
            // Each match is its own execution, so resting fills count per event
            // (a marketable order still counts once, however many levels it swept).
            tally.fills += 1;
            tally.maker_fills += 1;
        }
    }

    /// Kill every still-resting order at feed end, releasing what they held.
    /// Returns how many died unfilled.
    fn expire_resting(&mut self, ts: i64, recorder: &mut dyn Recorder) -> u64 {
        let orders = self.resting.drain();
        for order in &orders {
            if order.reserved > 0.0 {
                match order.side {
                    OrderSide::Buy => self.reservations.release_cash(order.reserved),
                    OrderSide::Sell => self
                        .reservations
                        .release_shares(&order.market_id, order.reserved),
                }
            }
            Self::note_order(
                recorder,
                ts,
                order.id,
                OrderEventKind::Expired,
                order.side,
                Some(order.price),
                order.remaining,
            );
        }
        orders.len() as u64
    }

    /// Post-run settlement: redeem whatever is still held at the resolution
    /// price, or report the unmarked holding when the market is unresolved.
    fn settle(&mut self, resolution: Option<Price>) -> (Settlement, Option<Position>) {
        let held: Vec<(MarketId, Position)> = self
            .portfolio
            .open_positions()
            .map(|(id, position)| (id.clone(), *position))
            .collect();

        let Some(redemption) = resolution else {
            // Single-market feed: at most one open position exists.
            return (Settlement::Unresolved, held.first().map(|(_, p)| *p));
        };
        if held.is_empty() {
            return (Settlement::NothingHeld, None);
        }

        let mut quantity = 0.0;
        let mut pnl = 0.0;
        for (market_id, position) in &held {
            quantity += position.quantity();
            pnl += self.portfolio.settle(market_id, redemption);
        }
        (
            Settlement::Settled {
                quantity,
                redemption,
                pnl,
            },
            None,
        )
    }

    /// The price-independent gates: an order whose required side is empty is
    /// unpriceable and rejected here (which lets fill models assume the side
    /// they price against exists), and sells can't exceed the current long —
    /// there is no shorting. Buy *funding* is deliberately not checked here:
    /// the true cost of a depth-walking fill is only known after the fill
    /// model runs, so cash is verified against the prospective fills in
    /// `can_afford`.
    fn can_execute(&self, request: &OrderRequest, state: &MarketState) -> bool {
        // A limit order does not need the opposite side to exist: with nothing
        // to cross it simply rests. Only a *marketable* order is unpriceable
        // without a touch.
        let side_ok = request.limit_price().is_some()
            || match request.side() {
                OrderSide::Buy => state.ask().is_some(),
                OrderSide::Sell => state.bid().is_some(),
            };
        // Sells are covered against *free* inventory — shares already promised
        // to a resting sell can't be sold twice.
        let inventory_ok = match request.side() {
            OrderSide::Buy => true,
            OrderSide::Sell => {
                let held = self.portfolio.position(request.market_id()).quantity();
                held - self.reservations.reserved_shares(request.market_id()) >= request.quantity()
            }
        };
        side_ok && inventory_ok
    }

    /// The funding gate, phase two: would the prospective fills' actual cost
    /// — swept price **plus taker fee** — clear the cash balance? Buys must be
    /// fully funded (no spending cash you don't have); sells only bring cash
    /// in (the fee is at most `rate·proceeds`, so a sell never fails here).
    fn can_afford(&self, request: &OrderRequest, fills: &[Fill]) -> bool {
        match request.side() {
            OrderSide::Buy => {
                let cost: f64 = fills
                    .iter()
                    .map(|fill| {
                        fill.quantity * fill.price.as_f64()
                            + crate::fees::taker_fee(self.sim.fee_rate, fill.price, fill.quantity)
                    })
                    .sum();
                // Against *free* cash: what resting buys already hold is
                // spoken for. (This order's own hold is released first, so it
                // never counts against itself.)
                let remaining =
                    (request.quantity() - fills.iter().map(|f| f.quantity).sum::<f64>()).max(0.0);
                let rest_cost = if request.tif().rests() {
                    request
                        .limit_price()
                        .map_or(0.0, |p| p.as_f64() * remaining)
                } else {
                    0.0
                };
                self.portfolio.cash() - self.reservations.reserved_cash() >= cost + rest_cost
            }
            OrderSide::Sell => true,
        }
    }

    /// What a request commits while it waits and rests: a buy holds cash at its
    /// limit (the worst it could pay), a sell holds the shares it promises.
    /// A market order commits nothing — it executes or dies on arrival, and its
    /// cost is checked against the walked fills instead.
    fn commitment(request: &OrderRequest) -> f64 {
        match (request.limit_price(), request.side()) {
            (Some(limit), OrderSide::Buy) => limit.as_f64() * request.quantity(),
            (Some(_), OrderSide::Sell) => request.quantity(),
            (None, _) => 0.0,
        }
    }

    /// Promote an accepted request to a live `Order`: the engine stamps
    /// identity (id, submission time); the strategy only ever supplies intent.
    fn accept(&mut self, request: &OrderRequest, state: &MarketState) -> Order {
        self.next_order_id += 1;
        Order::new(
            OrderId::new(self.next_order_id),
            state.timestamp,
            request.quantity(),
            request.side(),
            request.limit_price(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buyandhold::BuyAndHold;
    use crate::buyandsell::BuyAndSell;
    use crate::domain::marketstate::Level;
    use crate::domain::order::TimeInForce;
    use crate::feed::Snapshot;
    use crate::fill::fillmodel::NaiveFillModel;
    use crate::fill::sweep::SweepFillModel;
    use crate::io;
    use crate::io::LoadError;

    /// A one-level book with chosen touch prices, plenty of size.
    fn snapshot_at(timestamp: i64, bid: f64, ask: f64) -> MarketState {
        let level = |price: f64| {
            Box::from([Level {
                price: Price::from_probability(price).unwrap(),
                size: 1_000.0,
            }])
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            timestamp,
            level(bid),
            level(ask),
        )
        .unwrap()
    }

    /// The standard book: bid 0.08 / ask 0.085.
    fn snapshot(timestamp: i64) -> MarketState {
        snapshot_at(timestamp, 0.08, 0.085)
    }

    fn run(strategy: &mut dyn Strategy, resolution: Option<f64>) -> BacktestResult {
        let feed = io::fixture(vec![snapshot(1), snapshot(2)]);
        Engine::new(10_000.0, Box::new(NaiveFillModel), Simulation::zero())
            .run(
                feed,
                strategy,
                resolution.map(|r| Price::from_probability(r).unwrap()),
            )
            .unwrap()
    }

    /// Like `run` but with a taker fee rate applied (0.0 = fee-free).
    fn run_fee(
        strategy: &mut dyn Strategy,
        resolution: Option<f64>,
        fee_rate: f64,
    ) -> BacktestResult {
        let mut sim = Simulation::zero();
        sim.fee_rate = fee_rate;
        let feed = io::fixture(vec![snapshot(1), snapshot(2)]);
        Engine::new(10_000.0, Box::new(NaiveFillModel), sim)
            .run(
                feed,
                strategy,
                resolution.map(|r| Price::from_probability(r).unwrap()),
            )
            .unwrap()
    }

    /// Plays back a fixed script of requests, one per snapshot. `None`
    /// entries (and snapshots past the script's end) do nothing.
    struct Script(Vec<Option<OrderRequest>>, usize);

    impl Script {
        fn new(steps: Vec<Option<OrderRequest>>) -> Self {
            Self(steps, 0)
        }
    }

    impl Strategy for Script {
        fn on_market_state(
            &mut self,
            _state: &MarketState,
            _portfolio: &Portfolio,
            _orders: &[OpenOrder],
        ) -> Vec<Action> {
            let Some(slot) = self.0.get_mut(self.1) else {
                return Vec::new();
            };
            let step = slot.take();
            self.1 += 1;
            step.into_iter().map(Action::Place).collect()
        }
    }

    /// A book whose ask side holds exactly `size` shares at 0.50 — the depth
    /// two orders in the same instant will compete for.
    fn thin_ask(timestamp: i64, size: f64) -> MarketState {
        let level = |price: f64, size: f64| {
            Box::from([Level {
                price: Price::from_probability(price).unwrap(),
                size,
            }])
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            timestamp,
            level(0.40, 1_000.0),
            level(0.50, size),
        )
        .unwrap()
    }

    fn take(quantity: f64, tif: TimeInForce) -> Action {
        Action::Place(OrderRequest::market(
            MarketId::new("token".to_string()),
            OrderSide::Buy,
            quantity,
            tif,
        ))
    }

    /// Two marketable orders at one instant share the depth — they do not each
    /// get a fresh book. Without the residual both would fill 100, buying 200
    /// shares out of a level that only ever held 150.
    #[test]
    fn two_orders_at_one_instant_cannot_spend_the_same_depth() {
        let mut strategy = ActionScript(
            vec![vec![
                take(100.0, TimeInForce::Fak),
                take(100.0, TimeInForce::Fak),
            ]],
            0,
        );
        let feed = io::fixture(vec![thin_ask(1, 150.0), thin_ask(2, 150.0)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        // The second order takes the 50 that were left, not another 100.
        assert_eq!(result.open_position.as_ref().unwrap().quantity(), 150.0);
        assert_eq!(result.fills, 2);
    }

    /// When the first order clears the side outright, the second has nothing to
    /// execute against and is rejected rather than filled from thin air.
    #[test]
    fn depth_exhausted_by_the_first_order_rejects_the_second() {
        let mut strategy = ActionScript(
            vec![vec![
                take(100.0, TimeInForce::Fok),
                take(100.0, TimeInForce::Fok),
            ]],
            0,
        );
        let feed = io::fixture(vec![thin_ask(1, 100.0), thin_ask(2, 100.0)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        assert_eq!(result.open_position.as_ref().unwrap().quantity(), 100.0);
        assert_eq!(result.fills, 1);
        assert_eq!(result.rejections, 1);
    }

    /// The residual is scoped to one snapshot: the next book from the feed is
    /// ground truth again, so the same strategy fills afresh. (Persisting our
    /// depletion would double-count against a feed that already records what
    /// the real market did.)
    #[test]
    fn the_next_snapshot_restores_full_depth() {
        let mut strategy = ActionScript(
            vec![
                vec![take(100.0, TimeInForce::Fok)],
                vec![take(100.0, TimeInForce::Fok)],
            ],
            0,
        );
        let feed = io::fixture(vec![thin_ask(1, 100.0), thin_ask(2, 100.0)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        assert_eq!(result.open_position.as_ref().unwrap().quantity(), 200.0);
        assert_eq!(result.fills, 2);
        assert_eq!(result.rejections, 0);
    }

    /// Plays a script of full action lists — for cancels, or several orders on
    /// one snapshot.
    struct ActionScript(Vec<Vec<Action>>, usize);

    impl Strategy for ActionScript {
        fn on_market_state(
            &mut self,
            _state: &MarketState,
            _portfolio: &Portfolio,
            _orders: &[OpenOrder],
        ) -> Vec<Action> {
            let step = self.0.get(self.1).cloned().unwrap_or_default();
            self.1 += 1;
            step
        }
    }

    fn buy(quantity: f64) -> Option<OrderRequest> {
        Some(OrderRequest::market(
            MarketId::new("token".to_string()),
            OrderSide::Buy,
            quantity,
            TimeInForce::Fok,
        ))
    }

    fn sell(quantity: f64) -> Option<OrderRequest> {
        Some(OrderRequest::market(
            MarketId::new("token".to_string()),
            OrderSide::Sell,
            quantity,
            TimeInForce::Fok,
        ))
    }

    /// A latency profile of exactly `us` microseconds, all in one component.
    fn delay_us(us: u64) -> LatencyProfile {
        LatencyProfile::zero().with_taker_delay(std::time::Duration::from_micros(us))
    }

    fn run_latency(
        snapshots: Vec<MarketState>,
        strategy: &mut dyn Strategy,
        latency: LatencyProfile,
    ) -> BacktestResult {
        Engine::new(
            10_000.0,
            Box::new(NaiveFillModel),
            Simulation::with_latency(latency),
        )
        .run(io::fixture(snapshots), strategy, None)
        .unwrap()
    }

    /// A feed-only latency profile with perception delivery enabled.
    fn perception_sim(feed_us: u64) -> Simulation {
        let mut sim = Simulation::with_latency(LatencyProfile {
            feed: std::time::Duration::from_micros(feed_us),
            transit: std::time::Duration::ZERO,
            taker_delay: std::time::Duration::ZERO,
        });
        sim.perception = true;
        sim
    }

    fn run_sim(
        snapshots: Vec<MarketState>,
        strategy: &mut dyn Strategy,
        sim: Simulation,
    ) -> BacktestResult {
        Engine::new(10_000.0, Box::new(NaiveFillModel), sim)
            .run(io::fixture(snapshots), strategy, None)
            .unwrap()
    }

    #[test]
    fn perception_shows_snapshots_late_and_fills_on_the_true_book() {
        // Feed = 1s: snapshot 1 (t=1s) is delivered when the clock reaches
        // 2s, i.e. on snapshot 2's arrival. The buy decided on snapshot 1's
        // book executes at 1s + 1s = 2s — against snapshot 2's TRUE book
        // (ask 0.10), not the stale 0.085 the strategy saw.
        let snaps = vec![
            snapshot_at(1_000_000, 0.08, 0.085),
            snapshot_at(2_000_000, 0.09, 0.10),
            snapshot_at(3_000_000, 0.19, 0.20),
        ];
        let result = run_sim(
            snaps,
            &mut BuyAndHold::new(100.0),
            perception_sim(1_000_000),
        );

        assert_eq!(result.fills, 1);
        assert!((result.open_position.unwrap().average_price() - 0.10).abs() < 1e-9);
        assert!((result.final_cash - (10_000.0 - 10.0)).abs() < 1e-9);
    }

    #[test]
    fn perception_tail_is_never_seen() {
        // Feed = 10s over a 3s file: nothing is ever delivered, the strategy
        // never decides, and the portfolio is untouched.
        let snaps = vec![
            snapshot(1_000_000),
            snapshot(2_000_000),
            snapshot(3_000_000),
        ];
        let result = run_sim(
            snaps,
            &mut BuyAndHold::new(100.0),
            perception_sim(10_000_000),
        );

        assert_eq!(result.fills, 0);
        assert_eq!(result.rejections, 0);
        assert_eq!(result.expired, 0);
        assert_eq!(result.final_cash, 10_000.0);
        assert_eq!(result.open_position, None);
    }

    #[test]
    fn perception_with_zero_feed_is_identical_to_perception_off() {
        let snaps = || vec![snapshot(1_000_000), snapshot(2_000_000)];
        let on = run_sim(snaps(), &mut BuyAndHold::new(100.0), perception_sim(0));
        let off = run_sim(snaps(), &mut BuyAndHold::new(100.0), Simulation::zero());
        assert_eq!(on, off);
    }

    #[test]
    fn perception_delivers_each_snapshot_at_its_own_arrival_time() {
        use crate::report::record::{RecorderConfig, TimeSeriesRecorder};

        // Feed = 1.5 s. The snapshot from 1 s reaches the observer at 2.5 s and
        // the one from 2 s at 3.5 s — each at its own instant, not both at once
        // when the 4 s snapshot happens to arrive.
        //
        // Before the event clock they were delivered together at 4 s and both
        // orders filled on the 4 s book at 0.20. That was lookahead of the same
        // kind the latency path had: an order whose execution time had already
        // passed filled on a book update that had not happened yet. Each now
        // fills on the book in force when it executed — the 2 s ask, 0.10.
        let snaps = vec![
            snapshot_at(1_000_000, 0.08, 0.085),
            snapshot_at(2_000_000, 0.09, 0.10),
            snapshot_at(4_000_000, 0.19, 0.20),
        ];
        let script = vec![buy(10.0), buy(20.0)];
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let result = Engine::new(
            10_000.0,
            Box::new(NaiveFillModel),
            perception_sim(1_500_000),
        )
        .run_recorded(
            io::fixture(snaps),
            &mut Script::new(script),
            None,
            &mut recorder,
        )
        .unwrap();

        assert_eq!(result.fills, 2);
        let position = result.open_position.unwrap();
        assert_eq!(position.quantity(), 30.0);
        assert!((position.average_price() - 0.10).abs() < 1e-9);

        // The delivery times themselves: 1 s + 1.5 s and 2 s + 1.5 s. Orders are
        // stamped from the decision snapshot, so these are also their execution
        // instants — and neither waited for the 4 s book.
        let times: Vec<i64> = recorder.fills().iter().map(|f| f.ts).collect();
        assert_eq!(times, vec![2_500_000, 3_500_000]);
    }

    #[test]
    fn delayed_order_executes_at_the_window_close_on_the_last_known_book() {
        // Decided at t=1.0 s with Δ=1.5 s → due at exactly 2.5 s. No book
        // printed between 2.0 s and 3.0 s, so the book in force at 2.5 s is the
        // 2.0 s one (ask 0.10), carried forward.
        //
        // Before the event clock this executed on the 3.0 s snapshot (ask 0.20)
        // — the first one *past* the window. That was lookahead: it filled an
        // order that reached the exchange at 2.5 s using a book update that had
        // not happened yet, information no live system could have had. The
        // clock removes it by stopping at the appointment instead of skipping
        // to the next data.
        let snaps = vec![
            snapshot_at(1_000_000, 0.08, 0.085),
            snapshot_at(2_000_000, 0.09, 0.10),
            snapshot_at(3_000_000, 0.19, 0.20),
        ];
        let result = run_latency(
            snaps,
            &mut Script::new(vec![buy(100.0)]),
            delay_us(1_500_000),
        );

        assert_eq!(result.fills, 1);
        assert_eq!(result.expired, 0);
        // 100 x 0.10 — the book known at 2.5 s, not the one that arrived later.
        assert!((result.final_cash - (10_000.0 - 10.0)).abs() < 1e-9);
        let position = result.open_position.unwrap();
        assert!((position.average_price() - 0.10).abs() < 1e-9);
    }

    #[test]
    fn snapshot_exactly_at_the_window_boundary_executes_there() {
        let snaps = vec![
            snapshot_at(1_000_000, 0.08, 0.085),
            snapshot_at(2_000_000, 0.09, 0.10),
        ];
        // Δ=1s: due at exactly 2_000_000 → executes on that snapshot (≤).
        let result = run_latency(
            snaps,
            &mut Script::new(vec![buy(100.0)]),
            delay_us(1_000_000),
        );

        assert_eq!(result.fills, 1);
        assert!((result.open_position.unwrap().average_price() - 0.10).abs() < 1e-9);
    }

    #[test]
    fn order_in_flight_at_feed_end_expires() {
        let snaps = vec![snapshot(1_000_000), snapshot(2_000_000)];
        let result = run_latency(
            snaps,
            &mut Script::new(vec![buy(100.0)]),
            delay_us(10_000_000), // far beyond the feed
        );

        assert_eq!(result.expired, 1);
        assert_eq!(result.fills, 0);
        assert_eq!(result.rejections, 0);
        assert_eq!(result.final_cash, 10_000.0); // never executed
        assert_eq!(result.open_position, None);
    }

    #[test]
    fn execution_time_revalidation_catches_overselling_in_flight() {
        // Two sells of the full position queue up before either executes;
        // both pass the submission check (position is 100 at both decision
        // times), but only one survives execution-time re-validation.
        let snaps = vec![
            snapshot(1_000_000), // buy decided
            snapshot(2_000_000),
            snapshot(3_000_000), // buy executed (due 2.5s); sell #1 decided
            snapshot(4_000_000), // sell #1 still pending (due 4.5s); sell #2 decided
            snapshot(5_000_000), // sell #1 executes; position now 0
            snapshot(6_000_000), // sell #2 due (5.5s): inventory re-check fails
        ];
        let script = vec![buy(100.0), None, sell(100.0), sell(100.0)];
        let result = run_latency(snaps, &mut Script::new(script), delay_us(1_500_000));

        assert_eq!(result.fills, 2); // the buy and the first sell
        assert_eq!(result.rejections, 1); // the second sell, at execution time
        assert_eq!(result.expired, 0);
        assert_eq!(result.open_position, None); // flat: bought 100, sold 100
    }

    #[test]
    fn queued_orders_stack_and_execute_in_order() {
        let snaps = vec![
            snapshot(1_000_000),
            snapshot(2_000_000),
            snapshot(3_000_000),
            snapshot(4_000_000),
        ];
        // Buys decided on the first two snapshots, each spanning ~1.5s.
        let script = vec![buy(10.0), buy(20.0)];
        let result = run_latency(snaps, &mut Script::new(script), delay_us(1_500_000));

        assert_eq!(result.fills, 2);
        assert_eq!(result.expired, 0);
        assert_eq!(result.open_position.unwrap().quantity(), 30.0);
    }

    #[test]
    fn taker_fee_is_charged_on_a_buy() {
        // Buy 100 @ 0.085 ask, rate 0.05: fee = 100 × 0.05 × 0.085 × 0.915.
        let fee = 100.0 * 0.05 * 0.085 * 0.915;
        let result = run_fee(&mut BuyAndHold::new(100.0), None, 0.05);

        assert!((result.fees - fee).abs() < 1e-9);
        assert!((result.final_cash - (10_000.0 - 8.5 - fee)).abs() < 1e-9);
        // realized_pnl stays gross (the buy realizes nothing); fees net it.
        assert_eq!(result.realized_pnl, 0.0);
        assert!((result.total_pnl - (-fee)).abs() < 1e-9);
    }

    #[test]
    fn zero_fee_rate_matches_the_fee_free_run() {
        let with_zero = run_fee(&mut BuyAndHold::new(100.0), Some(0.0), 0.0);
        let fee_free = run(&mut BuyAndHold::new(100.0), Some(0.0));
        assert_eq!(with_zero, fee_free);
    }

    #[test]
    fn taker_fee_hits_both_sides_of_a_round_trip() {
        // BuyAndSell buys @ 0.085 then sells @ 0.08, both taker fills.
        let buy_fee = 100.0 * 0.05 * 0.085 * 0.915;
        let sell_fee = 100.0 * 0.05 * 0.08 * 0.92;
        let result = run_fee(&mut BuyAndSell::new(100.0), Some(1.0), 0.05);

        assert!((result.fees - (buy_fee + sell_fee)).abs() < 1e-9);
        assert_eq!(result.settlement, Settlement::NothingHeld); // ended flat
    }

    #[test]
    fn funding_gate_includes_the_fee() {
        // 100 @ 0.085 = $8.50 principal; give exactly that, so the fee
        // (~$0.39) can't be covered → rejected before any cash moves.
        let mut sim = Simulation::zero();
        sim.fee_rate = 0.05;
        let feed = io::fixture(vec![snapshot(1)]);
        let result = Engine::new(8.5, Box::new(NaiveFillModel), sim)
            .run(feed, &mut BuyAndHold::new(100.0), None)
            .unwrap();

        assert_eq!(result.rejections, 1);
        assert_eq!(result.fills, 0);
        assert_eq!(result.final_cash, 8.5); // untouched
    }

    #[test]
    fn money_flow_invariant_holds_with_fee_and_settlement() {
        // Buy 100 @ 0.085 with a fee, settle the winner at $1 → nothing left
        // open, so final_cash − initial must equal total_pnl exactly.
        let result = run_fee(&mut BuyAndHold::new(100.0), Some(1.0), 0.05);
        assert!(matches!(result.settlement, Settlement::Settled { .. }));
        assert!((result.final_cash - result.initial_capital - result.total_pnl).abs() < 1e-9);
    }

    #[test]
    fn winning_resolution_redeems_at_one() {
        // BuyAndHold: 100 shares lifted at the 0.085 ask, held to the end.
        let result = run(&mut BuyAndHold::new(100.0), Some(1.0));

        assert!(matches!(
            result.settlement,
            Settlement::Settled { quantity, pnl, .. }
                if quantity == 100.0 && (pnl - 91.5).abs() < 1e-9
        ));
        assert!((result.final_cash - 10_091.5).abs() < 1e-9);
        assert_eq!(result.realized_pnl, 0.0); // no trading P&L, only settlement
        assert!((result.total_pnl - 91.5).abs() < 1e-9);
        assert_eq!(result.open_position, None);
    }

    #[test]
    fn losing_resolution_redeems_at_zero() {
        let result = run(&mut BuyAndHold::new(100.0), Some(0.0));

        assert!(matches!(
            result.settlement,
            Settlement::Settled { pnl, .. } if (pnl + 8.5).abs() < 1e-9
        ));
        assert!((result.final_cash - 9_991.5).abs() < 1e-9);
        assert!((result.total_pnl + 8.5).abs() < 1e-9);
    }

    #[test]
    fn unresolved_reports_the_open_position_unmarked() {
        let result = run(&mut BuyAndHold::new(100.0), None);

        assert_eq!(result.settlement, Settlement::Unresolved);
        let position = result.open_position.expect("holding survives the run");
        assert_eq!(position.quantity(), 100.0);
        assert!((position.average_price() - 0.085).abs() < 1e-9);
        // Cash reflects the purchase only; the holding is not marked.
        assert!((result.final_cash - 9_991.5).abs() < 1e-9);
        assert_eq!(result.total_pnl, result.realized_pnl);
    }

    /// Asks 0.085×100, 0.090×150, 0.100×500; bids 0.080×20, 0.079×50.
    fn deep_snapshot(timestamp: i64) -> MarketState {
        let ladder = |levels: &[(f64, f64)]| -> Box<[Level]> {
            levels
                .iter()
                .map(|&(price, size)| Level {
                    price: Price::from_probability(price).unwrap(),
                    size,
                })
                .collect()
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            timestamp,
            ladder(&[(0.08, 20.0), (0.079, 50.0)]),
            ladder(&[(0.085, 100.0), (0.09, 150.0), (0.1, 500.0)]),
        )
        .unwrap()
    }

    fn run_sweep(capital: f64, size: f64, resolution: Option<f64>) -> BacktestResult {
        let feed = io::fixture(vec![deep_snapshot(1)]);
        Engine::new(
            capital,
            Box::new(crate::fill::sweep::SweepFillModel),
            Simulation::zero(),
        )
        .run(
            feed,
            &mut BuyAndHold::new(size),
            resolution.map(|r| Price::from_probability(r).unwrap()),
        )
        .unwrap()
    }

    #[test]
    fn sweep_books_exact_cost_across_levels() {
        // 300 shares sweep 100@0.085 + 150@0.090 + 50@0.100 = $27.00.
        let result = run_sweep(10_000.0, 300.0, None);

        assert_eq!(result.fills, 1); // one executed order, not three
        assert!((result.final_cash - 9_973.0).abs() < 1e-9);
        let position = result.open_position.expect("holding 300");
        assert_eq!(position.quantity(), 300.0);
        assert!((position.average_price() - 0.09).abs() < 1e-9); // 27/300
    }

    #[test]
    fn sweep_beyond_total_depth_is_killed() {
        // Asks hold 750 in total; ask for more.
        let result = run_sweep(10_000.0, 1_000.0, None);

        assert_eq!(result.rejections, 1);
        assert_eq!(result.fills, 0);
        assert_eq!(result.final_cash, 10_000.0); // portfolio untouched
    }

    #[test]
    fn funding_is_checked_against_walked_cost_not_touch() {
        // 300 × touch(0.085) = $25.50 fits in $26; the true sweep cost is
        // $27.00 and must be the number that gates.
        let result = run_sweep(26.0, 300.0, None);

        assert_eq!(result.rejections, 1);
        assert_eq!(result.fills, 0);
        assert_eq!(result.final_cash, 26.0);
    }

    #[test]
    fn sweep_bought_position_settles_correctly() {
        // 300 shares at blended 0.09 redeem at $1: (1 − 0.09) × 300 = 273.
        let result = run_sweep(10_000.0, 300.0, Some(1.0));

        assert!(matches!(
            result.settlement,
            Settlement::Settled { quantity, pnl, .. }
                if quantity == 300.0 && (pnl - 273.0).abs() < 1e-9
        ));
        assert!((result.final_cash - 10_273.0).abs() < 1e-9);
        assert!((result.total_pnl - 273.0).abs() < 1e-9);
    }

    #[test]
    fn unaffordable_buy_is_rejected() {
        // 100 shares at the 0.085 ask costs 8.50; give the engine less.
        let feed = io::fixture(vec![snapshot(1)]);
        let result = Engine::new(5.0, Box::new(NaiveFillModel), Simulation::zero())
            .run(feed, &mut BuyAndHold::new(100.0), None)
            .unwrap();

        assert_eq!(result.rejections, 1);
        assert_eq!(result.fills, 0);
        assert_eq!(result.final_cash, 5.0); // untouched
    }

    #[test]
    fn resolution_with_flat_book_settles_nothing() {
        // BuyAndSell buys on snapshot 1 and sells on snapshot 2: ends flat.
        let result = run(&mut BuyAndSell::new(100.0), Some(1.0));

        assert_eq!(result.settlement, Settlement::NothingHeld);
        assert_eq!(result.open_position, None);
        // The round trip crossed the spread: bought 0.085, sold 0.08.
        assert!((result.realized_pnl + 0.5).abs() < 1e-9);
        assert_eq!(result.total_pnl, result.realized_pnl);
    }

    // --- limit orders: resting, maker fills, holds -------------------------

    fn price(p: f64) -> Price {
        Price::from_probability(p).unwrap()
    }

    fn limit(side: OrderSide, quantity: f64, at: f64) -> Option<OrderRequest> {
        Some(OrderRequest::limit(
            MarketId::new("token".to_string()),
            side,
            quantity,
            price(at),
            TimeInForce::Gtc,
        ))
    }

    fn trade_at(ts: i64, side: OrderSide, p: f64, size: f64) -> Trade {
        Trade::new(
            ts,
            price(p),
            size,
            side,
            MarketId::new("origin".to_string()),
        )
    }

    /// A feed whose snapshots carry trades, for exercising maker fills.
    fn feed_with_trades(steps: Vec<(MarketState, Vec<Trade>)>) -> Vec<Result<Snapshot, LoadError>> {
        steps
            .into_iter()
            .map(|(state, trades)| {
                Ok(Snapshot {
                    state,
                    trades: trades.into_boxed_slice(),
                })
            })
            .collect()
    }

    fn run_limit(
        strategy: &mut dyn Strategy,
        steps: Vec<(MarketState, Vec<Trade>)>,
        fee_rate: f64,
    ) -> BacktestResult {
        let mut sim = Simulation::zero();
        sim.fee_rate = fee_rate;
        Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(io::replay(feed_with_trades(steps)), strategy, None)
            .unwrap()
    }

    #[test]
    fn a_non_marketable_limit_rests_and_fills_on_a_trade() {
        // Bid 0.07 while the book asks 0.085: nothing crosses, so it rests.
        // The sell trade at 0.07 on the NEXT snapshot lifts it.
        let mut script = Script::new(vec![limit(OrderSide::Buy, 100.0, 0.07)]);
        let result = run_limit(
            &mut script,
            vec![
                (snapshot(1), vec![]),
                (snapshot(2), vec![trade_at(2, OrderSide::Sell, 0.07, 100.0)]),
            ],
            0.05,
        );

        assert_eq!(result.maker_fills, 1);
        assert_eq!(result.taker_fills, 0);
        assert_eq!(result.rejections, 0);
        // Filled at OUR price, and a maker pays no fee even with a fee rate set.
        assert_eq!(result.fees, 0.0);
        assert!((result.final_cash - (10_000.0 - 7.0)).abs() < 1e-9);
        assert_eq!(result.expired_resting, 0);
    }

    #[test]
    fn a_resting_order_is_not_filled_by_trades_that_predate_it() {
        // The trade prints in the same snapshot the order is placed on: it
        // happened before the order reached the book, so it cannot fill it.
        let mut script = Script::new(vec![limit(OrderSide::Buy, 100.0, 0.07)]);
        let result = run_limit(
            &mut script,
            vec![(snapshot(1), vec![trade_at(1, OrderSide::Sell, 0.07, 100.0)])],
            0.0,
        );

        assert_eq!(result.maker_fills, 0);
        assert_eq!(result.expired_resting, 1, "it outlived the feed unfilled");
        assert_eq!(result.resting_quantity, 100.0);
    }

    #[test]
    fn a_marketable_limit_takes_what_crosses_and_rests_the_rest() {
        // Buy 1_200 @ 0.085 against an ask of 0.085 x 1_000: the book covers
        // 1_000 as taker, the other 200 rests as a maker order at 0.085.
        let mut script = Script::new(vec![limit(OrderSide::Buy, 1_200.0, 0.085)]);
        let result = run_limit(&mut script, vec![(snapshot(1), vec![])], 0.0);

        assert_eq!(result.taker_fills, 1);
        assert_eq!(result.maker_fills, 0);
        assert_eq!(result.rejections, 0);
        assert_eq!(result.resting_quantity, 200.0);
        assert_eq!(result.expired_resting, 1);
        // Only the part that crossed was paid for: 1_000 x 0.085.
        assert!((result.final_cash - (10_000.0 - 85.0)).abs() < 1e-9);
    }

    #[test]
    fn a_resting_order_fills_in_pieces_across_trades() {
        let mut script = Script::new(vec![limit(OrderSide::Buy, 100.0, 0.07)]);
        let result = run_limit(
            &mut script,
            vec![
                (snapshot(1), vec![]),
                (snapshot(2), vec![trade_at(2, OrderSide::Sell, 0.07, 30.0)]),
                (snapshot(3), vec![trade_at(3, OrderSide::Sell, 0.06, 20.0)]),
            ],
            0.0,
        );

        // Two match events, each its own fill; 50 of 100 still resting.
        assert_eq!(result.maker_fills, 2);
        assert_eq!(result.fills, 2);
        assert_eq!(result.resting_quantity, 50.0);
        assert!((result.final_cash - (10_000.0 - 3.5)).abs() < 1e-9);
    }

    #[test]
    fn resting_buys_cannot_collectively_overdraw() {
        // Each order bids under the touch (so it rests, never crosses) and holds
        // 0.07 x 50_000 = 3_500 at placement. Two fit inside 10_000; the third
        // would need 10_500, so it is refused at placement.
        let mut script = Script::new(vec![
            limit(OrderSide::Buy, 50_000.0, 0.07),
            limit(OrderSide::Buy, 50_000.0, 0.07),
            limit(OrderSide::Buy, 50_000.0, 0.07),
        ]);
        let result = run_limit(
            &mut script,
            vec![
                (snapshot(1), vec![]),
                (snapshot(2), vec![]),
                (snapshot(3), vec![]),
            ],
            0.0,
        );

        assert_eq!(result.rejections, 1, "the third order is unfunded");
        assert_eq!(result.expired_resting, 2);
        assert!((result.resting_reserved - 7_000.0).abs() < 1e-9);
    }

    #[test]
    fn a_resting_sell_cannot_promise_shares_twice() {
        // Buy 100 at market, then try to rest two sells of 100 each: the second
        // has no free inventory left to cover it.
        let mut script = Script::new(vec![
            buy(100.0),
            limit(OrderSide::Sell, 100.0, 0.5),
            limit(OrderSide::Sell, 100.0, 0.5),
        ]);
        let result = run_limit(
            &mut script,
            vec![
                (snapshot(1), vec![]),
                (snapshot(2), vec![]),
                (snapshot(3), vec![]),
            ],
            0.0,
        );

        assert_eq!(result.rejections, 1, "the second sell is uncovered");
        assert_eq!(result.expired_resting, 1);
    }

    /// A strategy sees its own resting order, cancels it by id, and the hold it
    /// carried comes back — the full place/observe/cancel loop.
    #[test]
    fn a_strategy_can_see_and_cancel_its_resting_order() {
        /// Rests a bid on the first snapshot, then cancels whatever it finds.
        struct Quoter {
            placed: bool,
            saw_resting: bool,
        }
        impl Strategy for Quoter {
            fn on_market_state(
                &mut self,
                state: &MarketState,
                _portfolio: &Portfolio,
                orders: &[OpenOrder],
            ) -> Vec<Action> {
                if !self.placed {
                    self.placed = true;
                    return vec![Action::Place(OrderRequest::limit(
                        state.market_id.clone(),
                        OrderSide::Buy,
                        100.0,
                        price(0.05),
                        TimeInForce::Gtc,
                    ))];
                }
                // The id is only knowable from the view, on a later snapshot.
                match orders.first() {
                    Some(order) if order.state == OrderState::Resting => {
                        self.saw_resting = true;
                        vec![Action::Cancel(order.id)]
                    }
                    _ => Vec::new(),
                }
            }
        }

        let mut quoter = Quoter {
            placed: false,
            saw_resting: false,
        };
        let feed = io::fixture(vec![snapshot(1), snapshot(2), snapshot(3)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut quoter, None)
            .unwrap();

        assert!(quoter.saw_resting, "the strategy should see its own order");
        assert_eq!(result.cancels, 1);
        assert_eq!(result.fills, 0, "a 0.05 bid never crosses");
        assert_eq!(
            result.expired_resting, 0,
            "it was cancelled, so nothing outlived the feed"
        );
        assert_eq!(
            result.final_cash, 10_000.0,
            "a cancelled order returns its hold untouched"
        );
    }

    /// Cancelling an id that is gone is a normal race, not an error.
    #[test]
    fn cancelling_an_unknown_order_is_a_silent_no_op() {
        let mut script = ActionScript(vec![vec![Action::Cancel(OrderId::new(999))]], 0);
        let feed = io::fixture(vec![snapshot(1), snapshot(2)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut script, None)
            .unwrap();

        assert_eq!(result.cancels, 0, "nothing was actually withdrawn");
        assert_eq!(result.rejections, 0, "a stale cancel is not a rejection");
    }

    /// The whole limit-order path over real data: the book parquet and its
    /// sibling trades tape merge into one feed, a bid rests under the touch,
    /// and the day's actual sell flow reaches it — as a maker, for free.
    ///
    /// Run under both queue models, because the contrast *is* the result. The
    /// book sits at 0.35/0.36 all day, so a 0.35 bid can only ever fill as a
    /// maker. Assuming no queue, the day's sell prints fill it. Joining the
    /// back of the displayed size at 0.35 — the realistic default — they do
    /// not come close to draining the line ahead, and it never fills at all.
    /// That gap is the single largest correction the queue model made.
    #[test]
    #[ignore = "reads the real data pair; run with `cargo test -- --ignored`"]
    fn a_resting_order_fills_from_the_real_tape() {
        use crate::feed::MergedFeed;
        use crate::resting::QueueModel;

        let token =
            "110136933550893624733134445460153301975615510734202337526927943993346922198810";
        let path = format!("data/telonex/{token}_2026-06-12_book_snapshot_full.parquet");

        let run_with = |queue: QueueModel| {
            let (feed, tape) = MergedFeed::from_book_path_optional(&path).unwrap();
            assert!(tape, "the sibling trades file should be found");
            let mut script = Script::new(vec![Some(OrderRequest::limit(
                MarketId::new(token.to_string()),
                OrderSide::Buy,
                100.0,
                price(0.35),
                TimeInForce::Gtc,
            ))]);
            let mut sim = Simulation::zero();
            sim.fee_rate = 0.05; // sports tier: makers still pay nothing
            sim.queue = queue;
            Engine::new(10_000.0, Box::new(SweepFillModel), sim)
                .run(feed, &mut script, None)
                .unwrap()
        };

        // No queue: the real sell flow reaches us.
        let front = run_with(QueueModel::Front);
        assert!(
            front.maker_fills > 0,
            "assuming no queue, the real tape fills a 0.35 bid"
        );
        assert_eq!(front.taker_fills, 0, "a 0.35 bid never crosses a 0.36 ask");
        assert_eq!(front.fees, 0.0, "makers pay no fee");
        // Whatever filled was bought at exactly our resting price.
        let filled = 100.0 - front.resting_quantity;
        assert!((front.final_cash - (10_000.0 - 0.35 * filled)).abs() < 1e-6);

        // Behind the displayed size: a quiet day's flow never reaches us.
        let real = run_with(QueueModel::Proportional);
        assert_eq!(
            real.maker_fills, 0,
            "behind the real queue at 0.35, this day's flow never gets to us"
        );
        assert_eq!(real.resting_quantity, 100.0, "the whole bid is still there");
        assert_eq!(real.final_cash, 10_000.0, "nothing traded, nothing spent");
    }

    /// The unbounded case, and the reason perception needed the clock. The next
    /// book is 100 s away; a delivery due 9 ms after its snapshot must not wait
    /// for it. Otherwise the modelled delay is not `feed` at all, it is "feed,
    /// rounded up to whenever the market next moves".
    #[test]
    fn a_perception_delivery_does_not_wait_for_the_next_book() {
        use crate::report::record::{RecorderConfig, TimeSeriesRecorder};

        let snaps = vec![
            snapshot_at(0, 0.08, 0.085),
            snapshot_at(100 * SEC, 0.19, 0.20),
        ];
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        Engine::new(10_000.0, Box::new(NaiveFillModel), perception_sim(9_000))
            .run_recorded(
                io::fixture(snaps),
                &mut Script::new(vec![buy(10.0)]),
                None,
                &mut recorder,
            )
            .unwrap();

        assert_eq!(
            recorder.fills().iter().map(|f| f.ts).collect::<Vec<_>>(),
            vec![9_000],
            "9 ms after its snapshot, not 100 s"
        );
        // And on the book that was in force then, not the one 100 s later.
        assert!((recorder.fills()[0].price - 0.085).abs() < 1e-9);
    }

    /// A book with a chosen bid size, to watch a level get eaten.
    fn bid_of(ts: i64, size: f64) -> MarketState {
        MarketState::new(
            MarketId::new("token".to_string()),
            ts,
            Box::from([Level {
                price: price(0.30),
                size,
            }]),
            Box::from([Level {
                price: price(0.31),
                size: 1_000.0,
            }]),
        )
        .unwrap()
    }

    /// Rests one GTC bid and asks to be woken once, to force a tick.
    struct RestAndWake {
        placed: bool,
        wake: i64,
    }
    impl Strategy for RestAndWake {
        fn on_market_state(
            &mut self,
            _state: &MarketState,
            _p: &Portfolio,
            _o: &[OpenOrder],
        ) -> Vec<Action> {
            if self.placed {
                return Vec::new();
            }
            self.placed = true;
            vec![
                Action::Place(OrderRequest::limit(
                    MarketId::new("token".to_string()),
                    OrderSide::Buy,
                    100.0,
                    price(0.30),
                    TimeInForce::Gtc,
                )),
                Action::WakeAt(self.wake),
            ]
        }
    }

    /// Trades delivered on a TICK must still explain the shrinkage the next book
    /// shows. The level goes 1000 -> 500 entirely because 500 shares traded, so
    /// nothing is left over to blame on cancellations and our queue position
    /// must not improve beyond what the trade itself drained.
    #[test]
    fn a_trade_on_a_tick_still_explains_the_next_book() {
        let mut sim = Simulation::zero();
        sim.queue = QueueModel::Proportional;
        let steps = feed_with_trades(vec![
            (bid_of(0, 1_000.0), vec![]),
            // The tick at 120 s carries this print; the book showing its effect
            // does not arrive until 300 s.
            (
                bid_of(300 * SEC, 500.0),
                vec![trade_at(40 * SEC, OrderSide::Sell, 0.30, 500.0)],
            ),
            // Queue is now 500 if the trade was accounted for once, 250 if its
            // volume was double-counted. This 300-share print fills us only in
            // the second case.
            (
                bid_of(400 * SEC, 500.0),
                vec![trade_at(350 * SEC, OrderSide::Sell, 0.30, 300.0)],
            ),
        ]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(
                io::replay(steps),
                &mut RestAndWake {
                    placed: false,
                    wake: 120 * SEC,
                },
                None,
            )
            .unwrap();

        assert_eq!(
            result.maker_fills, 0,
            "queue advanced twice on one trade: 500 ahead of us, a 300 print cannot reach us"
        );
    }

    // --- a source with appointments of its own ------------------------------

    /// Wraps a source and interposes a tick every `every` µs, whether or not the
    /// engine asked for one — a stand-in for the heartbeat a live connection
    /// must send (Polymarket expects one every 5 s and cancels every open order
    /// after 10 s without one).
    ///
    /// It defers to its data for the end of the stream: when the inner source is
    /// exhausted this one stops too, rather than beating forever. A real live
    /// source would never be exhausted, which is the same rule read the other
    /// way.
    struct Heartbeat<S> {
        inner: S,
        every: i64,
        next_beat: i64,
        /// Wake-ups this source asked for that the engine had not. Shared so the
        /// test can prove the interruptions actually happened — otherwise it
        /// would pass just as well with a heartbeat that never beat.
        beats: std::rc::Rc<std::cell::Cell<u64>>,
    }

    impl<S: EventSource> EventSource for Heartbeat<S> {
        type Error = S::Error;

        fn next_event(&mut self, deadline: Option<i64>) -> Option<Result<Event, S::Error>> {
            // The engine's deadline is a ceiling; our own beat may be sooner.
            let ceiling = deadline.map_or(self.next_beat, |at| at.min(self.next_beat));
            if deadline.is_none_or(|at| self.next_beat < at) {
                self.beats.set(self.beats.get() + 1);
            }
            let event = self.inner.next_event(Some(ceiling))?;
            if let Ok(event) = &event {
                let ts = match event {
                    Event::Book(state) => state.timestamp,
                    Event::Trade(trade) => trade.timestamp,
                    Event::Time(at) => *at,
                };
                while self.next_beat <= ts {
                    self.next_beat += self.every;
                }
            }
            Some(event)
        }
    }

    /// The engine must tolerate being woken earlier than it asked. It never
    /// assumed a tick landed on its own deadline, and this pins that: with a
    /// heartbeat every 10 s interrupting constantly, a GTD due at 120 s still
    /// expires at 120 s to the microsecond, and the run still terminates.
    #[test]
    fn a_source_may_wake_the_engine_before_its_own_deadline() {
        use crate::report::record::{OrderEventKind, RecorderConfig, TimeSeriesRecorder};

        let beats = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut script = Script::new(vec![gtd_bid(100.0, 180 * SEC)]);
        let feed = Heartbeat {
            inner: io::fixture(vec![snapshot(0), snapshot(300 * SEC)]),
            every: 10 * SEC,
            next_beat: 10 * SEC,
            beats: std::rc::Rc::clone(&beats),
        };
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run_recorded(feed, &mut script, None, &mut recorder)
            .unwrap();

        assert_eq!(result.expired_gtd, 1);
        let expiry = recorder
            .orders()
            .iter()
            .find(|e| e.kind == OrderEventKind::Expired)
            .expect("an expiry was recorded");
        assert_eq!(
            expiry.ts,
            120 * SEC,
            "heartbeats must not drag the appointment off its instant"
        );
        assert!(
            beats.get() > 10,
            "the heartbeat never interrupted, so this proved nothing: {}",
            beats.get()
        );
    }

    // --- on_time and strategy-requested wake-ups ---------------------------

    /// Records every time hook it receives, and asks for one wake-up.
    struct Clockwatcher {
        woke_at: Vec<i64>,
        request: Option<i64>,
        placed: bool,
    }

    impl Strategy for Clockwatcher {
        fn on_market_state(
            &mut self,
            _state: &MarketState,
            _portfolio: &Portfolio,
            _orders: &[OpenOrder],
        ) -> Vec<Action> {
            if self.placed {
                return Vec::new();
            }
            self.placed = true;
            self.request
                .take()
                .map(Action::WakeAt)
                .into_iter()
                .collect()
        }

        fn on_time(
            &mut self,
            state: &MarketState,
            _portfolio: &Portfolio,
            _orders: &[OpenOrder],
        ) -> Vec<Action> {
            self.woke_at.push(state.timestamp);
            Vec::new()
        }
    }

    /// A strategy can create an appointment where the engine had none. Without
    /// this it can only ever act when a book arrives, so on a quiet market
    /// anything it meant to do on a schedule never happens at all.
    #[test]
    fn a_strategy_can_ask_to_be_woken_when_the_market_is_silent() {
        let mut strategy = Clockwatcher {
            woke_at: Vec::new(),
            request: Some(30 * SEC),
            placed: false,
        };
        // Nothing else is scheduled — no orders at all — and the next book is
        // 100 s out. The only reason to stop at 30 s is because it was asked.
        let feed = io::fixture(vec![snapshot(0), snapshot(100 * SEC)]);
        Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        assert_eq!(strategy.woke_at, vec![30 * SEC]);
    }

    /// A request already in the past is dropped, not fired late — a callback
    /// that cannot be honoured on time is worse than none.
    #[test]
    fn a_wake_up_in_the_past_is_dropped() {
        let mut strategy = Clockwatcher {
            woke_at: Vec::new(),
            request: Some(-5),
            placed: false,
        };
        let feed = io::fixture(vec![snapshot(0), snapshot(100 * SEC)]);
        Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        assert!(strategy.woke_at.is_empty());
    }

    /// The hook also fires on appointments the strategy did not ask for — an
    /// order of ours expiring is time passing too, and that is precisely when a
    /// strategy most wants to act.
    #[test]
    fn a_gtd_expiry_wakes_the_strategy() {
        let mut strategy = Clockwatcher {
            woke_at: Vec::new(),
            request: None,
            placed: true, // never places, so only the engine schedules anything
        };
        // Hand-place a GTD so there is an expiry appointment at 120 s.
        let mut script = Script::new(vec![gtd_bid(100.0, 180 * SEC)]);
        let feed = io::fixture(vec![snapshot(0), snapshot(300 * SEC)]);
        let placed = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut script, None)
            .unwrap();
        assert_eq!(placed.expired_gtd, 1, "the appointment exists");

        // Same feed, a strategy that only watches the clock: no orders, so no
        // appointments, so no wake-ups.
        let feed = io::fixture(vec![snapshot(0), snapshot(300 * SEC)]);
        Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();
        assert!(
            strategy.woke_at.is_empty(),
            "nothing was scheduled, so nothing woke it"
        );
    }

    /// Under perception the hook must hand over the book the strategy has
    /// actually been shown — never the newer one being withheld, which would
    /// leak through the timer the very information the delay models.
    #[test]
    fn on_time_respects_the_perception_delay() {
        struct BookWatcher {
            saw: Vec<(i64, f64)>,
            asked: bool,
        }
        impl Strategy for BookWatcher {
            fn on_market_state(
                &mut self,
                _state: &MarketState,
                _p: &Portfolio,
                _o: &[OpenOrder],
            ) -> Vec<Action> {
                if self.asked {
                    return Vec::new();
                }
                self.asked = true;
                // Wake at 2.5 s: after the 2 s book exists, before it is
                // delivered at 3.5 s. That gap is the whole test.
                vec![Action::WakeAt(2_500_000)]
            }
            fn on_time(
                &mut self,
                state: &MarketState,
                _p: &Portfolio,
                _o: &[OpenOrder],
            ) -> Vec<Action> {
                self.saw
                    .push((state.timestamp, state.ask().unwrap().price.as_f64()));
                Vec::new()
            }
        }

        let mut strategy = BookWatcher {
            saw: Vec::new(),
            asked: false,
        };
        // Feed 1.5 s. The 2 s book (ask 0.10) is real and current at 2.5 s, but
        // this observer does not receive it until 3.5 s. Waking at 2.5 s must
        // therefore still show 0.085 — otherwise the timer becomes a side door
        // onto exactly the information the delay exists to withhold.
        let snaps = vec![
            snapshot_at(0, 0.08, 0.085),
            snapshot_at(2 * SEC, 0.09, 0.10),
            snapshot_at(10 * SEC, 0.19, 0.20),
        ];
        Engine::new(
            10_000.0,
            Box::new(NaiveFillModel),
            perception_sim(1_500_000),
        )
        .run(io::fixture(snaps), &mut strategy, None)
        .unwrap();

        let at_2_5 = strategy
            .saw
            .iter()
            .find(|(ts, _)| *ts == 2_500_000)
            .expect("the requested wake-up fired");
        assert_eq!(
            at_2_5.1, 0.085,
            "leaked the undelivered 0.10 book through the time hook"
        );
    }

    // --- the event clock ---------------------------------------------------

    /// Time reaches an appointment exactly, not whenever data next happens to
    /// arrive. The GTD below is due at 120 s and the next book is 89 minutes
    /// later; the recorded expiry must land on 120 s, not on the snapshot.
    #[test]
    fn an_appointment_fires_at_its_own_instant_not_the_next_snapshot() {
        use crate::report::record::{OrderEventKind, RecorderConfig, TimeSeriesRecorder};

        let mut script = Script::new(vec![gtd_bid(100.0, 180 * SEC)]);
        let feed = io::fixture(vec![snapshot(0), snapshot(5_400 * SEC)]);
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run_recorded(feed, &mut script, None, &mut recorder)
            .unwrap();

        assert_eq!(result.expired_gtd, 1);
        let expiry = recorder
            .orders()
            .iter()
            .find(|e| e.kind == OrderEventKind::Expired)
            .expect("an expiry was recorded");
        assert_eq!(
            expiry.ts,
            120 * SEC,
            "180 s stated - 60 s buffer, to the microsecond"
        );
    }

    /// The reason a tick has to carry trades. This bid is retired at 120 s, but
    /// a sell printed at 119 s — while it was still live — and is bundled with a
    /// snapshot that does not arrive until 300 s. Delivering the trade with its
    /// snapshot would hand it to an order that no longer exists; cutting the
    /// trade window at the appointment fills it, which is what really happened.
    #[test]
    fn a_trade_before_an_appointment_still_reaches_the_order() {
        let mut sim = Simulation::zero();
        sim.queue = QueueModel::Front;
        let steps = feed_with_trades(vec![
            (snapshot_at(0, 0.30, 0.31), vec![]),
            (
                snapshot_at(300 * SEC, 0.30, 0.31),
                vec![trade_at(119 * SEC, OrderSide::Sell, 0.30, 100.0)],
            ),
        ]);
        let mut script = Script::new(vec![Some(OrderRequest::limit(
            MarketId::new("token".to_string()),
            OrderSide::Buy,
            100.0,
            price(0.30),
            TimeInForce::Gtd {
                expires_at: 180 * SEC,
            },
        ))]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(io::replay(steps), &mut script, None)
            .unwrap();

        assert_eq!(
            result.maker_fills, 1,
            "the print beat the expiry, so it must fill"
        );
        assert_eq!(result.expired_gtd, 0, "it filled before it could expire");
    }

    /// The mirror: a print *after* the appointment is too late.
    #[test]
    fn a_trade_after_an_appointment_finds_nothing_left() {
        let mut sim = Simulation::zero();
        sim.queue = QueueModel::Front;
        let steps = feed_with_trades(vec![
            (snapshot_at(0, 0.30, 0.31), vec![]),
            (
                snapshot_at(300 * SEC, 0.30, 0.31),
                vec![trade_at(121 * SEC, OrderSide::Sell, 0.30, 100.0)],
            ),
        ]);
        let mut script = Script::new(vec![Some(OrderRequest::limit(
            MarketId::new("token".to_string()),
            OrderSide::Buy,
            100.0,
            price(0.30),
            TimeInForce::Gtd {
                expires_at: 180 * SEC,
            },
        ))]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(io::replay(steps), &mut script, None)
            .unwrap();

        assert_eq!(result.maker_fills, 0, "the order was already retired");
        assert_eq!(result.expired_gtd, 1);
    }

    /// With nothing scheduled there are no appointments, so the feed is walked
    /// exactly as before — the property every marketable-order regression rests
    /// on.
    #[test]
    fn with_nothing_scheduled_the_clock_never_ticks() {
        let mut engine_pending = VecDeque::new();
        let engine = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero());
        assert_eq!(
            engine.next_deadline(&engine_pending, &VecDeque::new(), 0, 0),
            None
        );
        engine_pending.clear();
    }

    // --- GTD: the exchange retires the order on a timer --------------------

    const SEC: i64 = 1_000_000;

    fn gtd_bid(quantity: f64, stated_expiry: i64) -> Option<OrderRequest> {
        Some(OrderRequest::limit(
            MarketId::new("token".to_string()),
            OrderSide::Buy,
            quantity,
            price(0.07), // under the 0.08 bid, so it can only ever rest
            TimeInForce::Gtd {
                expires_at: stated_expiry,
            },
        ))
    }

    /// The order dies a full minute BEFORE the timestamp it stated — Polymarket
    /// applies that buffer as a security threshold. Stating 180 s therefore buys
    /// 120 s of life, and the pair of runs below brackets exactly that: alive at
    /// 100 s, gone at 130 s. An engine that honoured the stated 180 s would show
    /// the 130 s run still resting.
    #[test]
    fn a_gtd_order_dies_a_minute_before_its_stated_expiry() {
        let run_to = |end: i64| {
            let mut script = Script::new(vec![gtd_bid(100.0, 180 * SEC)]);
            let feed = io::fixture(vec![snapshot(0), snapshot(end)]);
            Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
                .run(feed, &mut script, None)
                .unwrap()
        };

        let alive = run_to(100 * SEC);
        assert_eq!(
            alive.expired_gtd, 0,
            "120 s of life has not run out at 100 s"
        );
        assert_eq!(alive.expired_resting, 1, "it outlived the feed instead");

        let dead = run_to(130 * SEC);
        assert_eq!(
            dead.expired_gtd, 1,
            "180 s stated - 60 s buffer = gone by 120 s"
        );
        assert_eq!(dead.expired_resting, 0, "it left before the feed ended");
        assert_eq!(dead.resting_quantity, 0.0);
    }

    /// The exchange refuses an expiry less than three minutes out, so the order
    /// never reaches the book at all. That is a rejection, not an expiry.
    #[test]
    fn a_gtd_expiring_inside_three_minutes_is_rejected() {
        let mut script = Script::new(vec![gtd_bid(100.0, 179 * SEC)]);
        let feed = io::fixture(vec![snapshot(0), snapshot(SEC)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut script, None)
            .unwrap();

        assert_eq!(result.rejections, 1);
        assert_eq!(result.resting_quantity, 0.0, "it never reached the book");
        assert_eq!(result.expired_gtd, 0, "rejected, not expired");
        assert_eq!(result.final_cash, 10_000.0, "and nothing was held for it");
    }

    /// Expiring releases the cash the order was holding. Both bids below reserve
    /// 9,100 of 10,000, so the second can only be accepted if the first's hold
    /// was genuinely returned — a leak would show up as a rejection.
    #[test]
    fn an_expired_gtd_releases_what_it_held() {
        let mut script = Script::new(vec![
            gtd_bid(130_000.0, 180 * SEC), // reserves 9,100, dies at 120 s
            None,                          // the 130 s snapshot: it expires
            gtd_bid(130_000.0, 320 * SEC), // needs another 9,100
        ]);
        let feed = io::fixture(vec![snapshot(0), snapshot(130 * SEC), snapshot(140 * SEC)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut script, None)
            .unwrap();

        assert_eq!(result.expired_gtd, 1);
        assert_eq!(result.rejections, 0, "the second bid was affordable again");
        assert_eq!(
            result.expired_resting, 1,
            "the second was still resting at the end"
        );
        assert!((result.resting_reserved - 9_100.0).abs() < 1e-6);
    }

    /// A marketable GTD behaves like a marketable GTC: the crossing part takes
    /// liquidity, the rest joins the book — and is then subject to the timer.
    #[test]
    fn a_marketable_gtd_fills_then_rests_the_remainder() {
        let mut strategy = ActionScript(
            vec![vec![Action::Place(OrderRequest::limit(
                MarketId::new("token".to_string()),
                OrderSide::Buy,
                200.0,
                price(0.60), // crosses the 0.50 ask
                TimeInForce::Gtd {
                    expires_at: 180 * SEC,
                },
            ))]],
            0,
        );
        let feed = io::fixture(vec![thin_ask(0, 100.0), thin_ask(130 * SEC, 100.0)]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run(feed, &mut strategy, None)
            .unwrap();

        assert_eq!(result.taker_fills, 1, "the 100 available were taken");
        assert_eq!(
            result.expired_gtd, 1,
            "the resting remainder then timed out"
        );
        assert_eq!(result.open_position.as_ref().unwrap().quantity(), 100.0);
    }

    // --- latency-aware cancels: adverse selection --------------------------

    /// Places a resting bid, then tries to cancel it on every later snapshot.
    struct PlaceThenCancel {
        placed: bool,
    }
    impl Strategy for PlaceThenCancel {
        fn on_market_state(
            &mut self,
            state: &MarketState,
            _portfolio: &Portfolio,
            orders: &[OpenOrder],
        ) -> Vec<Action> {
            if !self.placed {
                self.placed = true;
                return vec![Action::Place(OrderRequest::limit(
                    state.market_id.clone(),
                    OrderSide::Buy,
                    100.0,
                    price(0.30),
                    TimeInForce::Gtc,
                ))];
            }
            orders
                .first()
                .map_or(Vec::new(), |o| vec![Action::Cancel(o.id)])
        }
    }

    /// The whole point of the change: a cancel is a message in flight, so a
    /// trade arriving *before it lands* fills the order the strategy tried to
    /// pull. This is a maker being picked off.
    ///
    /// Timings are chosen so the race is genuine rather than an artifact of
    /// snapshot granularity: dublin's placement delay is 13 ms, the cancel is
    /// requested at t=2.000 s so it lands at 2.013 s, and the trade prints at
    /// 2.005 s — really before the cancel reached the exchange.
    #[test]
    fn a_trade_beats_an_in_flight_cancel() {
        let mut sim = Simulation::with_latency(LatencyProfile::dublin());
        sim.queue = QueueModel::Front;
        let steps = feed_with_trades(vec![
            (snapshot_at(1_000_000, 0.30, 0.31), vec![]), // place
            (snapshot_at(2_000_000, 0.30, 0.31), vec![]), // resting; cancel requested
            // Cancel lands at 2.013 s; this print at 2.005 s beats it.
            (
                snapshot_at(2_010_000, 0.30, 0.31),
                vec![trade_at(2_005_000, OrderSide::Sell, 0.30, 100.0)],
            ),
            (snapshot_at(3_000_000, 0.30, 0.31), vec![]),
        ]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(
                io::replay(steps),
                &mut PlaceThenCancel { placed: false },
                None,
            )
            .unwrap();

        assert_eq!(
            result.maker_fills, 1,
            "the in-flight cancel did not save us"
        );
        assert!(
            result.cancels_missed > 0,
            "the cancel should arrive to find the order already filled"
        );
        assert_eq!(result.cancels, 0, "nothing was actually withdrawn");
    }

    /// With no latency a cancel lands instantly, so the same sequence pulls the
    /// order before the trade arrives — and nothing fills.
    #[test]
    fn without_latency_the_cancel_wins() {
        let mut sim = Simulation::zero();
        sim.queue = QueueModel::Front;
        let steps = feed_with_trades(vec![
            (snapshot_at(1_000_000, 0.30, 0.31), vec![]),
            (snapshot_at(2_000_000, 0.30, 0.31), vec![]),
            (
                snapshot_at(3_000_000, 0.30, 0.31),
                vec![trade_at(2_500_000, OrderSide::Sell, 0.30, 100.0)],
            ),
        ]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), sim)
            .run(
                io::replay(steps),
                &mut PlaceThenCancel { placed: false },
                None,
            )
            .unwrap();

        assert_eq!(result.maker_fills, 0);
        assert_eq!(result.cancels, 1);
        assert_eq!(result.cancels_missed, 0);
    }
}
