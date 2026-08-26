//! The merged book + trades feed.
//!
//! Pairs each book snapshot with the trades that printed since the previous
//! snapshot (merge shape B), so a later resting-order matcher can advance
//! against real executions without changing the engine's snapshot-driven loop
//! shape. Both input streams are time-monotonic (each source guards its own
//! order), so the merge is a single linear pass.
//!
//! This is the engine's feed: `Engine::run` takes `Snapshot`s, so a book-only
//! replay is just a merge with an absent [`Tape`]. The trades are carried but
//! not yet consumed — the resting-order matcher reads them next.

use std::collections::VecDeque;
use std::iter::Peekable;
use std::path::{Path, PathBuf};

use crate::domain::marketstate::MarketState;
use crate::domain::trade::Trade;
use crate::io::LoadError;
use crate::telonex::{TelonexBookSource, TelonexTradesSource};

/// A book snapshot bundled with the trades that printed in the interval closing
/// at its timestamp. `MarketState` is left untouched — the trades ride alongside
/// it in the feed layer, not inside the book type.
pub struct Snapshot {
    pub state: MarketState,
    pub trades: Box<[Trade]>,
}

/// One thing the engine's clock advances to.
///
/// Three atomic kinds, because there are three atomic things: the market
/// repriced, someone traded, time passed. They used to be two — a book arrived
/// with its window of trades bundled inside, and time was bolted on afterwards
/// as a "tick" that also carried trades, which is a contradiction in terms.
///
/// The bundling was an artifact of the historical format (a book file and a
/// trades file, joined on a window) and it cost real money to maintain: every
/// source had to cut its trade window at the deadline, independently and
/// correctly, or a print from before an appointment would be delivered after
/// it. Two implementations, two readings, one of them wrong. Decomposed, there
/// is no window to cut — ordering falls out of comparing timestamps, and the
/// hazard cannot be expressed.
///
/// It is also how the data actually arrives live: the CLOB websocket sends book
/// and trade frames independently, so a live source would have had to *invent*
/// the bundling purely to fit the old shape.
pub enum Event {
    /// The book was republished.
    Book(MarketState),
    /// A print crossed the tape.
    Trade(Trade),
    /// Time reached this instant with nothing else to report.
    Time(i64),
}

/// A source the engine drives against its own schedule.
///
/// Splitting time out of the data stream is what lets the historical replay and
/// a future live connection be the *same* loop: live, timers genuinely fire
/// independently of market data, and here they do too.
pub trait EventSource {
    /// How this source fails.
    ///
    /// An associated type rather than a fixed [`LoadError`], because that is a
    /// *file-loading* error — its variants are bad parquet, unsorted ladders, a
    /// missing sibling tape. A live websocket source fails at disconnects,
    /// protocol violations and expired auth, none of which are loading
    /// problems. Forcing those through `LoadError` is how a type ends up with
    /// an `Io(String)` variant that means "something else went wrong".
    ///
    /// Each source declares what it can actually fail with, which fixes the
    /// abstraction without inventing live variants before there is a live
    /// source to shape them.
    ///
    /// Bounded as a real error so callers can always `?` it into
    /// `Box<dyn Error>`, which is what `main` does with every failure.
    type Error: std::error::Error + 'static;

    /// The earliest of: this source's next data, this source's own appointment,
    /// or `deadline`.
    ///
    /// `deadline` is a **ceiling, not an instruction**. The engine is saying
    /// "wake me by then at the latest"; a source is free to come back sooner
    /// with a time event of its own. That is what a live connection needs —
    /// Polymarket expects a heartbeat every 5 s and cancels every open order
    /// after 10 s without one, plus reconnect backoff and subscription refresh.
    /// The engine never assumes a time event landed on the instant it asked
    /// for; an appointment it has not reached yet is proposed again next time.
    ///
    /// `deadline` is **always strictly after the last event this source
    /// emitted**. Violating that would run the engine's clock backwards,
    /// silently: appointments stop firing and orders stop executing, with no
    /// error anywhere. Implementations assert it.
    ///
    /// The contract, as five rules, all checked by [`conformance`] — prose
    /// alone already let two implementations disagree once, see rule 4.
    ///
    /// 1. With `deadline: None` and no appointment of your own, emit the next
    ///    data.
    /// 2. With an appointment strictly before the next data, emit
    ///    [`Event::Time`] at it.
    /// 3. With an appointment at or after the next data, emit the data.
    /// 4. **With no data left AND no appointment of your own, return `None` —
    ///    never manufacture time.** A source that keeps ticking once exhausted
    ///    runs forever against any strategy that replaces an order when one
    ///    expires; this replay ticked into 2037 before it was fixed. A
    ///    historical source exhausts and stops; a live one always has a
    ///    heartbeat pending and so never does — one rule, both worlds.
    /// 5. Events are emitted in timestamp order, and a trade at the same
    ///    instant as a book comes **first**: the prints of a window happened
    ///    before the picture that shows their effect.
    ///
    fn next_event(&mut self, deadline: Option<i64>) -> Option<Result<Event, Self::Error>>;
}

/// Interleaves a book source and a trades source into one time-ordered stream of
/// [`Snapshot`]s. Each snapshot carries every not-yet-emitted trade with
/// `ts <= snapshot.ts` — i.e. the `(previous_ts, this_ts]` window. Leading
/// trades attach to the first snapshot; **trailing** trades (after the last
/// snapshot) are dropped, since there is no snapshot to carry them and
/// settlement happens after the feed drains anyway. Any error from either input
/// short-circuits the stream (fail-fast).
pub struct MergedFeed<B, T>
where
    B: Iterator<Item = Result<MarketState, LoadError>>,
    T: Iterator<Item = Result<Trade, LoadError>>,
{
    book: Peekable<B>,
    trades: Peekable<T>,
    /// The instant of the last event handed out, so a caller that would run the
    /// clock backwards trips an assertion instead of corrupting the run.
    last_emitted: i64,
}

/// A trades stream that may not exist. The tape is required to fill resting
/// orders, but not to replay a book, so a missing sibling file degrades to
/// [`Tape::Absent`] (an empty stream) rather than failing the whole run — the
/// engine enforces its presence only where it actually matters.
pub enum Tape {
    Present(TelonexTradesSource),
    Absent,
}

impl Iterator for Tape {
    type Item = Result<Trade, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Tape::Present(source) => source.next(),
            Tape::Absent => None,
        }
    }
}

/// A feed over the on-disk Telonex pair, with or without the trades tape.
pub type FileFeed = MergedFeed<TelonexBookSource, Tape>;

impl MergedFeed<TelonexBookSource, Tape> {
    /// Open a book file and its sibling trades file. The trades path is the book
    /// path with `_book_snapshot_full` swapped for `_trades` — the pairing
    /// `scripts/telonex_pull.py` writes.
    pub fn from_book_path(book_path: impl AsRef<Path>) -> Result<Self, LoadError> {
        let book_path = book_path.as_ref();
        Ok(Self::new(
            TelonexBookSource::from_path(book_path)?,
            Tape::Present(TelonexTradesSource::from_path(sibling_trades_path(
                book_path,
            ))?),
        ))
    }

    /// Like [`from_book_path`](Self::from_book_path), but tolerates a missing
    /// tape: returns the feed plus whether trades were found, so the caller can
    /// warn. A malformed (as opposed to absent) tape still fails.
    pub fn from_book_path_optional(book_path: impl AsRef<Path>) -> Result<(Self, bool), LoadError> {
        let book_path = book_path.as_ref();
        let book = TelonexBookSource::from_path(book_path)?;
        let trades_path = sibling_trades_path(book_path);
        let (tape, found) = if trades_path.exists() {
            (
                Tape::Present(TelonexTradesSource::from_path(trades_path)?),
                true,
            )
        } else {
            (Tape::Absent, false)
        };
        Ok((Self::new(book, tape), found))
    }
}

impl<B, T> MergedFeed<B, T>
where
    B: Iterator<Item = Result<MarketState, LoadError>>,
    T: Iterator<Item = Result<Trade, LoadError>>,
{
    pub fn new(book: B, trades: T) -> Self {
        Self {
            book: book.peekable(),
            trades: trades.peekable(),
            last_emitted: i64::MIN,
        }
    }
}

impl<B, T> EventSource for MergedFeed<B, T>
where
    B: Iterator<Item = Result<MarketState, LoadError>>,
    T: Iterator<Item = Result<Trade, LoadError>>,
{
    type Error = LoadError;

    fn next_event(&mut self, deadline: Option<i64>) -> Option<Result<Event, LoadError>> {
        debug_assert!(
            deadline.is_none_or(|at| at > self.last_emitted),
            "deadline {deadline:?} is not after the last event at {} — the clock would run backwards",
            self.last_emitted
        );

        // Peek both inputs, surfacing either error by consuming it (fail-fast).
        let next_trade = match self.trades.peek() {
            Some(Ok(trade)) => Some(trade.timestamp),
            Some(Err(_)) => return self.trades.next().map(|t| Err(t.unwrap_err())),
            None => None,
        };
        let next_book = match self.book.peek() {
            Some(Ok(state)) => Some(state.timestamp),
            Some(Err(_)) => return self.book.next().map(|b| Err(b.unwrap_err())),
            None => None,
        };

        // Rule 4: nothing left is the end, whatever the engine has scheduled.
        let data = match (next_trade, next_book) {
            (Some(t), Some(b)) => t.min(b),
            (Some(t), None) => t,
            (None, Some(b)) => b,
            (None, None) => return None,
        };

        // Rule 2: an appointment strictly before the next data.
        if let Some(at) = deadline {
            if at < data {
                self.last_emitted = at;
                return Some(Ok(Event::Time(at)));
            }
        }

        // Rule 5: a trade at the same instant as a book goes first.
        let trade_first = match (next_trade, next_book) {
            (Some(t), Some(b)) => t <= b,
            (Some(_), None) => true,
            _ => false,
        };
        if trade_first {
            self.last_emitted = next_trade.expect("trade_first implies a trade");
            return Some(self.trades.next().expect("peeked").map(Event::Trade));
        }
        self.last_emitted = next_book.expect("a book or we returned above");
        Some(self.book.next().expect("peeked").map(Event::Book))
    }
}

/// Replays pre-built [`Snapshot`]s as events — an in-memory source for tests and
/// for any producer that already bundles a book with its trades.
///
/// It unbundles: the trades of a window are handed over one at a time, at their
/// own instants, and the book follows. What used to be delicate here — cutting
/// a trade window at a deadline, and getting the same answer as every other
/// source — is now nothing at all.
pub struct Replay<I>
where
    I: Iterator<Item = Result<Snapshot, LoadError>>,
{
    inner: I,
    /// A window pulled from `inner`: its book, and the trades of that window not
    /// yet handed over.
    held: Option<(MarketState, VecDeque<Trade>)>,
    /// See [`MergedFeed::last_emitted`].
    last_emitted: i64,
}

impl<I> Replay<I>
where
    I: Iterator<Item = Result<Snapshot, LoadError>>,
{
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            held: None,
            last_emitted: i64::MIN,
        }
    }
}

impl<I> EventSource for Replay<I>
where
    I: Iterator<Item = Result<Snapshot, LoadError>>,
{
    type Error = LoadError;

    fn next_event(&mut self, deadline: Option<i64>) -> Option<Result<Event, LoadError>> {
        debug_assert!(
            deadline.is_none_or(|at| at > self.last_emitted),
            "deadline {deadline:?} is not after the last event at {} — the clock would run backwards",
            self.last_emitted
        );

        if self.held.is_none() {
            match self.inner.next()? {
                Ok(snapshot) => {
                    self.held = Some((snapshot.state, snapshot.trades.into_vec().into()))
                }
                Err(err) => return Some(Err(err)),
            }
        }
        let (state, trades) = self.held.as_mut().expect("just filled");

        // The next thing due: the earliest un-delivered trade, else the book.
        let data = trades.front().map_or(state.timestamp, |t| t.timestamp);
        if let Some(at) = deadline {
            if at < data {
                self.last_emitted = at;
                return Some(Ok(Event::Time(at)));
            }
        }
        if let Some(trade) = trades.pop_front() {
            self.last_emitted = trade.timestamp;
            return Some(Ok(Event::Trade(trade)));
        }
        let (state, _) = self.held.take().expect("held above");
        self.last_emitted = state.timestamp;
        Some(Ok(Event::Book(state)))
    }
}

/// Derive the sibling trades path from a `book_snapshot_full` path. A path that
/// doesn't match the pattern yields a nonexistent path, which surfaces as a
/// clear file-not-found error when opened.
fn sibling_trades_path(book_path: &Path) -> PathBuf {
    let trades_name = book_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.replace("_book_snapshot_full.parquet", "_trades.parquet"))
        .unwrap_or_default();
    book_path.with_file_name(trades_name)
}

/// The contract of [`EventSource`], as executable rules.
///
/// This exists because prose was not enough. `MergedFeed` and `Replay` read the
/// same sentence two different ways and disagreed about rule 4 — one of them
/// ticked past the end of its data forever — and no test could catch it, because
/// every fixture used one implementation and only real data used the other.
/// Anything implementing the trait, including the future live source, is held
/// to these.
///
/// Scope note: these sources have no appointments of their own, so rule 2 is
/// checked as "a tick at exactly the deadline". A source with its own schedule
/// — a live connection's heartbeat — may legitimately come back sooner, since
/// the deadline is a ceiling. That case is pinned engine-side by
/// `a_source_may_wake_the_engine_before_its_own_deadline`.
#[cfg(test)]
mod conformance {
    use super::*;
    use crate::domain::marketstate::{Level, MarketState};
    use crate::domain::order::OrderSide;
    use crate::domain::types::{MarketId, Price};

    /// One book, and the trades that printed in the window closing at it.
    type Spec = [(i64, Vec<Trade>)];

    fn state(ts: i64) -> MarketState {
        let level: Box<[Level]> = Box::from([Level {
            price: Price::from_probability(0.5).unwrap(),
            size: 10.0,
        }]);
        MarketState::new(MarketId::new("t".to_string()), ts, level.clone(), level).unwrap()
    }

    fn trade(ts: i64) -> Trade {
        Trade::new(
            ts,
            Price::from_probability(0.5).unwrap(),
            1.0,
            OrderSide::Buy,
            MarketId::new("t".to_string()),
        )
    }

    /// A feed built from an in-memory spec, one vec per input stream.
    type SpecFeed = MergedFeed<
        std::vec::IntoIter<Result<MarketState, LoadError>>,
        std::vec::IntoIter<Result<Trade, LoadError>>,
    >;

    fn merged(spec: &Spec) -> SpecFeed {
        let books: Vec<_> = spec.iter().map(|(ts, _)| Ok(state(*ts))).collect();
        let trades: Vec<_> = spec
            .iter()
            .flat_map(|(_, t)| t.iter().cloned().map(Ok))
            .collect();
        MergedFeed::new(books.into_iter(), trades.into_iter())
    }

    fn replay(spec: &Spec) -> Replay<std::vec::IntoIter<Result<Snapshot, LoadError>>> {
        let snaps: Vec<_> = spec
            .iter()
            .map(|(ts, t)| {
                Ok(Snapshot {
                    state: state(*ts),
                    trades: t.clone().into_boxed_slice(),
                })
            })
            .collect();
        Replay::new(snaps.into_iter())
    }

    fn at(event: &Event) -> i64 {
        match event {
            Event::Book(state) => state.timestamp,
            Event::Trade(trade) => trade.timestamp,
            Event::Time(at) => *at,
        }
    }

    /// Drive a source to exhaustion, proposing each deadline that is still in
    /// the future. Returns every event in order.
    fn drive<S: EventSource>(source: &mut S, deadlines: &[i64]) -> Vec<Event> {
        let mut out = Vec::new();
        let mut last = i64::MIN;
        let mut pending = deadlines.iter().copied().peekable();
        loop {
            while pending.peek().is_some_and(|&d| d <= last) {
                pending.next();
            }
            let Some(event) = source.next_event(pending.peek().copied()) else {
                break;
            };
            let event = event.expect("the spec has no errors in it");
            last = at(&event);
            out.push(event);
            assert!(out.len() < 10_000, "source is not terminating");
        }
        out
    }

    fn check<S: EventSource>(name: &str, make: impl Fn(&Spec) -> S) {
        let spec: Vec<(i64, Vec<Trade>)> = vec![
            (100, vec![trade(40), trade(90)]),
            (200, vec![trade(150)]),
            (300, vec![]),
        ];

        // 1. No appointment, no time events — just the data, in order. Note the
        //    trades come out on their own, ahead of the book of their window.
        let events = drive(&mut make(&spec), &[]);
        assert!(
            !events.iter().any(|e| matches!(e, Event::Time(_))),
            "{name}: rule 1 — emitted a time event with nothing scheduled"
        );
        let shape: Vec<(&str, i64)> = events
            .iter()
            .map(|e| match e {
                Event::Book(state) => ("book", state.timestamp),
                Event::Trade(t) => ("trade", t.timestamp),
                Event::Time(at) => ("time", *at),
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                ("trade", 40),
                ("trade", 90),
                ("book", 100),
                ("trade", 150),
                ("book", 200),
                ("book", 300),
            ],
            "{name}: rule 1 — wrong event sequence"
        );

        // 2. An appointment strictly before the next data becomes a time event.
        let mut source = make(&spec);
        assert_eq!(at(&source.next_event(None).unwrap().unwrap()), 40);
        match source.next_event(Some(60)).unwrap().unwrap() {
            Event::Time(at) => assert_eq!(at, 60, "{name}: rule 2 — wrong instant"),
            other => panic!("{name}: rule 2 — expected a time event, got {}", at(&other)),
        }

        // 3. An appointment at or after the next data yields the data.
        let mut source = make(&spec);
        assert!(
            matches!(
                source.next_event(Some(40)).unwrap().unwrap(),
                Event::Trade(_)
            ),
            "{name}: rule 3 — a deadline on the data's own instant must yield the data"
        );
        let mut source = make(&spec);
        assert!(
            matches!(
                source.next_event(Some(999)).unwrap().unwrap(),
                Event::Trade(_)
            ),
            "{name}: rule 3 — a deadline past the next data must yield the data"
        );

        // 4. Exhausted means finished. This is the one the two implementations
        //    disagreed about, and it ticked a replay into 2037.
        let mut source = make(&spec);
        while source.next_event(None).is_some() {}
        assert!(
            source.next_event(Some(10_000)).is_none(),
            "{name}: rule 4 — invented time past the end of the data"
        );

        // 5. Order holds however the appointments fall, and every trade is
        //    delivered exactly once, at its own instant.
        let events = drive(&mut make(&spec), &[50, 95, 160, 250, 275]);
        let times: Vec<i64> = events.iter().map(at).collect();
        assert!(
            times.windows(2).all(|w| w[1] >= w[0]),
            "{name}: rule 5 — time went backwards: {times:?}"
        );
        let prints: Vec<i64> = events
            .iter()
            .filter_map(|e| match e {
                Event::Trade(t) => Some(t.timestamp),
                _ => None,
            })
            .collect();
        assert_eq!(
            prints,
            vec![40, 90, 150],
            "{name}: rule 5 — trades lost, duplicated or reordered"
        );

        // 5, the tie: a print at the same instant as a book comes first — the
        // trades of a window happened before the picture showing their effect.
        let tie: Vec<(i64, Vec<Trade>)> = vec![(100, vec![trade(100)])];
        let events = drive(&mut make(&tie), &[]);
        assert!(
            matches!(events.first(), Some(Event::Trade(_))),
            "{name}: rule 5 — the book overtook a print at its own instant"
        );
        assert!(matches!(events.get(1), Some(Event::Book(_))));
    }

    #[test]
    fn merged_feed_conforms() {
        check("MergedFeed", merged);
    }

    #[test]
    fn replay_conforms() {
        check("Replay", replay);
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    use crate::domain::marketstate::{Level, MarketState};
    use crate::domain::types::{MarketId, Price};

    fn state(ts: i64) -> MarketState {
        let level: Box<[Level]> = Box::from([Level {
            price: Price::from_probability(0.5).unwrap(),
            size: 10.0,
        }]);
        MarketState::new(MarketId::new("t".to_string()), ts, level.clone(), level).unwrap()
    }

    fn feed(times: &[i64]) -> MergedFeed<std::vec::IntoIter<Result<MarketState, LoadError>>, Tape> {
        let states: Vec<_> = times.iter().map(|&t| Ok(state(t))).collect();
        MergedFeed::new(states.into_iter(), Tape::Absent)
    }

    #[test]
    fn an_appointment_before_the_next_book_becomes_a_tick() {
        let mut f = feed(&[0, 100]);
        assert!(matches!(f.next_event(None), Some(Ok(Event::Book(_)))));
        match f.next_event(Some(40)) {
            Some(Ok(Event::Time(at))) => assert_eq!(at, 40),
            other => panic!("expected a tick at 40, got {:?}", other.is_some()),
        }
    }

    /// The feed ends when its data does. Without this a strategy that replaces
    /// an order on every expiry keeps the clock running forever — the replay
    /// ticks past the end of the file and never terminates.
    #[test]
    fn an_appointment_past_the_last_book_ends_the_feed() {
        let mut f = feed(&[0]);
        assert!(matches!(f.next_event(None), Some(Ok(Event::Book(_)))));
        assert!(
            f.next_event(Some(9_999)).is_none(),
            "no book follows, so there is no more time to give"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::order::OrderSide;
    use crate::domain::types::{MarketId, Price};

    fn state(ts: i64) -> MarketState {
        MarketState::new(
            MarketId::new("token".to_string()),
            ts,
            Box::from([]),
            Box::from([]),
        )
        .unwrap()
    }

    fn trade(ts: i64) -> Trade {
        Trade::new(
            ts,
            Price::parse_decimal("0.5").unwrap(),
            10.0,
            OrderSide::Buy,
            MarketId::new("origin".to_string()),
        )
    }

    fn book(tss: &[i64]) -> std::vec::IntoIter<Result<MarketState, LoadError>> {
        tss.iter()
            .map(|&ts| Ok(state(ts)))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn trades(tss: &[i64]) -> std::vec::IntoIter<Result<Trade, LoadError>> {
        tss.iter()
            .map(|&ts| Ok(trade(ts)))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Collect each snapshot's timestamp and the timestamps of the trades it carries.
    /// Regroup the decomposed stream back into `(book_ts, [trade_ts])` windows,
    /// which is what these tests are really about: that every print lands in
    /// the right window and in the right order. The source no longer *builds*
    /// windows — that was the bundling the decomposition removed — so the test
    /// does the grouping instead.
    fn windows<B, T>(mut feed: MergedFeed<B, T>) -> Vec<(i64, Vec<i64>)>
    where
        B: Iterator<Item = Result<MarketState, LoadError>>,
        T: Iterator<Item = Result<Trade, LoadError>>,
    {
        let mut out = Vec::new();
        let mut pending: Vec<i64> = Vec::new();
        while let Some(event) = feed.next_event(None) {
            match event.expect("no errors in these fixtures") {
                Event::Trade(trade) => pending.push(trade.timestamp),
                Event::Book(state) => out.push((state.timestamp, std::mem::take(&mut pending))),
                Event::Time(_) => unreachable!("no deadline was given"),
            }
        }
        out
    }

    #[test]
    fn each_snapshot_carries_its_trade_window() {
        // Book at 10/20/30, trades at 5/12/15/25/40.
        let feed = MergedFeed::new(book(&[10, 20, 30]), trades(&[5, 12, 15, 25, 40]));
        assert_eq!(
            windows(feed),
            vec![
                (10, vec![5]),      // leading trade 5 attaches to the first
                (20, vec![12, 15]), // (10, 20] window
                (30, vec![25]),     // trade 40 is trailing → dropped
            ]
        );
    }

    #[test]
    fn no_trades_yields_bare_snapshots() {
        let feed = MergedFeed::new(book(&[1, 2]), trades(&[]));
        assert_eq!(windows(feed), vec![(1, vec![]), (2, vec![])]);
    }

    #[test]
    fn a_trade_at_the_snapshot_timestamp_is_included() {
        // Boundary: ts == snapshot.ts belongs to that snapshot (inclusive).
        let feed = MergedFeed::new(book(&[10, 20]), trades(&[10, 20]));
        assert_eq!(windows(feed), vec![(10, vec![10]), (20, vec![20])]);
    }

    #[test]
    fn trade_error_short_circuits() {
        let tradefeed =
            vec![Ok(trade(5)), Err(LoadError::InvalidSide("x".to_string()))].into_iter();
        let mut feed = MergedFeed::new(book(&[10, 20]), tradefeed);
        // The good print is handed over first — decomposed, a trade is not
        // hostage to the window it used to be bundled into, so nothing before
        // the error is discarded.
        assert!(matches!(
            feed.next_event(None),
            Some(Ok(Event::Trade(t))) if t.timestamp == 5
        ));
        // And then the stream still short-circuits, as it must.
        assert!(matches!(
            feed.next_event(None),
            Some(Err(LoadError::InvalidSide(_)))
        ));
    }

    #[test]
    fn an_absent_tape_yields_trade_free_snapshots() {
        // The degrade path: a book with no sibling tape still replays, it just
        // carries no trades (so nothing can fill a resting order).
        let feed = MergedFeed::new(book(&[1, 2]), Tape::Absent);
        assert_eq!(windows(feed), vec![(1, vec![]), (2, vec![])]);
    }

    /// The whole tape must reach the engine: every print is delivered as its
    /// own event, so the resting-order matcher can't miss a fill through a
    /// merge gap. Uses the same entry point `main` does.
    #[test]
    #[ignore = "reads the real data pair; run with `cargo test -- --ignored`"]
    fn every_trade_reaches_a_snapshot() {
        let path = "data/telonex/110136933550893624733134445460153301975615510734202337526927943993346922198810_2026-06-12_book_snapshot_full.parquet";
        let (feed, found) = MergedFeed::from_book_path_optional(path).unwrap();
        assert!(found, "the sibling tape should be found");
        let mut feed = feed;
        let mut carried = 0usize;
        while let Some(event) = feed.next_event(None) {
            if matches!(event.unwrap(), Event::Trade(_)) {
                carried += 1;
            }
        }
        assert_eq!(carried, 1_389, "the 06-12 tape has 1,389 prints");
    }

    /// Reads the real game-day pair: confirms the merge runs end-to-end and,
    /// crucially, that `side` is the **aggressor's** side — buys lift the ask
    /// (print above mid), sells hit the bid (below mid). If the label were
    /// inverted, both fractions would fall below 0.5. Lenient by design: a
    /// sanity check that answers open question #28, not a proof.
    #[test]
    #[ignore = "reads the real data pair; run with `cargo test -- --ignored`"]
    fn side_convention_over_real_pair() {
        let path = "data/telonex/110136933550893624733134445460153301975615510734202337526927943993346922198810_2026-06-14_book_snapshot_full.parquet";
        let mut feed = MergedFeed::from_book_path(path).unwrap();

        let (mut buy_total, mut buy_above) = (0u64, 0u64);
        let (mut sell_total, mut sell_below) = (0u64, 0u64);
        let mut carried = 0u64;
        // The book in force when each print crossed — the decomposed stream
        // hands them over separately, so the test carries it forward itself.
        let mut last: Option<MarketState> = None;

        while let Some(event) = feed.next_event(None) {
            let trades = match event.unwrap() {
                Event::Book(state) => {
                    last = Some(state);
                    continue;
                }
                Event::Trade(trade) => {
                    carried += 1;
                    vec![trade]
                }
                Event::Time(_) => unreachable!("no deadline was given"),
            };
            let Some(mid) = last.as_ref().and_then(|s| s.mid()) else {
                continue;
            };
            for t in trades.iter() {
                match t.side {
                    OrderSide::Buy => {
                        buy_total += 1;
                        if t.price > mid {
                            buy_above += 1;
                        }
                    }
                    OrderSide::Sell => {
                        sell_total += 1;
                        if t.price < mid {
                            sell_below += 1;
                        }
                    }
                }
            }
        }

        assert!(
            carried > 5_000,
            "expected the full tape, carried only {carried}"
        );
        let buy_frac = buy_above as f64 / buy_total as f64;
        let sell_frac = sell_below as f64 / sell_total as f64;
        assert!(
            buy_frac > 0.5,
            "buys not above mid ({buy_frac:.2}) — `side` may be inverted"
        );
        assert!(
            sell_frac > 0.5,
            "sells not below mid ({sell_frac:.2}) — `side` may be inverted"
        );
    }
}
