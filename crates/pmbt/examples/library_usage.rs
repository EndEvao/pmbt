//! A custom strategy using only public APIs, with no registry entry.
//! cargo run -p pmbt --example library_usage -- <book_snapshot_full.parquet> [report.html]
use pmbt::{
    domain::{
        marketstate::MarketState,
        order::{OrderRequest, OrderSide, TimeInForce},
    },
    portfolio::Portfolio,
    strategy::{Action, OpenOrder, Strategy},
    Backtest, BacktestConfig, OutputConfig, StrategyMetadata,
};

struct FirstAsk {
    size: f64,
    submitted: bool,
}
impl Strategy for FirstAsk {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        _: &Portfolio,
        _: &[OpenOrder],
    ) -> Vec<Action> {
        if self.submitted {
            return vec![];
        }
        let Some(ask) = state.ask() else {
            return vec![];
        };
        if ask.size() < self.size {
            return vec![];
        }
        self.submitted = true; // submitted is not a claim that the exchange filled it
        vec![Action::Place(OrderRequest::limit(
            state.market_id().clone(),
            OrderSide::Buy,
            self.size,
            ask.price(),
            TimeInForce::Fok,
        ))]
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: library_usage <book_snapshot_full.parquet> [report.html]")?;
    let outputs = args.next().map(OutputConfig::html).unwrap_or_default();
    if args.next().is_some() {
        return Err("too many arguments".into());
    }
    let mut config = BacktestConfig::new(path);
    config.simulation.fill = Some("sweep".into());
    let size = 25.0;
    let mut strategy = FirstAsk {
        size,
        submitted: false,
    };
    let mut metadata = StrategyMetadata::new("First ask example");
    metadata.parameters.push(("Size".into(), size.to_string()));
    let outcome = Backtest::new(config)
        .metadata(metadata)
        .outputs(outputs)
        .run(&mut strategy)?;
    println!(
        "fills: {}; realized trading P&L: {:.2}; open inventory: {:?}",
        outcome.result.fills, outcome.result.realized_pnl, outcome.result.open_position
    );
    for note in outcome.diagnostics {
        eprintln!("{note:?}");
    }
    for artifact in outcome.artifacts {
        println!("{:?}: {}", artifact.kind, artifact.path.display());
    }
    Ok(())
}
