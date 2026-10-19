// Dependency-free JavaScript/interaction smoke checks using a minimal DOM stub.
// This checks rendering logic and controls, not browser layout or screenshots.
// node scripts/check_report.mjs [generated-report.html]
import fs from "node:fs";
import vm from "node:vm";
import assert from "node:assert/strict";
const html = fs.readFileSync(
  process.argv[2] || new URL("../assets/report.html", import.meta.url),
  "utf8",
);
const scripts = [...html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)];
const code = scripts.find(
  ([_, attrs]) => !attrs.includes("application/json"),
)[2];
const dataText = scripts
  .find(([_, attrs]) => attrs.includes("application/json"))[2]
  .trim();
const blank = {
  title: "Empty run",
  title_sub: "test.parquet",
  context: {
    market_name: null,
    outcome: null,
    settings: [],
    queue: null,
    tape_present: null,
  },
  summary: {
    initial_capital: 10000,
    final_cash: 10000,
    realized_pnl: 0,
    total_pnl: 0,
    fees: 0,
    settlement: "unresolved",
    settlement_pnl: 0,
    redemption: null,
    fills: 0,
    maker_fills: 0,
    taker_fills: 0,
    rejections: 0,
    cancels: 0,
    expired: 0,
    expired_resting: 0,
    resting_quantity: 0,
    open_qty: null,
    open_avg: null,
  },
  analysis: {
    ending_equity: 10000,
    headline_pnl: 0,
    headline_return: 0,
    unrealized_pnl: 0,
    avg_deployed: null,
    peak_deployed: 0,
    peak_inventory: 0,
    recorded_samples: 0,
    fill_records: 0,
    cancels_missed: 0,
    expired_gtd: 0,
    depth_levels: 10,
  },
  quality: {
    snapshots: 0,
    one_sided: 0,
    locked_or_crossed: 0,
    max_interval_us: 0,
  },
  stats: {
    bar_seconds: 1,
    bars: 0,
    bars_per_year: 31557600,
    sharpe: 0,
    sortino: 0,
    sharpe_raw: 0,
    sortino_raw: 0,
    annual_vol: 0,
    pct_positive_bars: 0,
    max_drawdown: {
      frac: 0,
      dollars: 0,
      peak_ts: 0,
      trough_ts: 0,
      duration_us: 0,
    },
    exposure: 0,
    turnover: 0,
  },
  trade_stats: {
    round_trips: 0,
    win_rate: 0,
    avg_win: 0,
    avg_loss: 0,
    avg_hold_us: 0,
  },
  order_stats: {
    placed: 0,
    rested: 0,
    cancelled: 0,
    expired: 0,
    fill_ratio: 0,
    cancel_rate: 0,
    mean_time_in_queue_us: 0,
    max_time_in_queue_us: 0,
  },
  bench_final_pnl: 0,
  pts: [],
  fills: [],
  blotter: [],
  depth: [],
};
function run(model, width) {
  const elements = new Map(
    [...html.matchAll(/id="([^"]+)"/g)].map((m) => [
      m[1],
      {
        innerHTML: "",
        textContent: "",
        value: "0",
        checked: m[1] === "show-quotes",
        hidden: false,
        disabled: false,
        attributes: {},
        setAttribute(k, v) {
          this.attributes[k] = v;
        },
        getBoundingClientRect() {
          return { left: 0, width };
        },
        querySelector(selector) {
          assert.equal(selector, ".cross");
          assert.ok(this.innerHTML.includes('class="cross"'));
          return { setAttribute() {} };
        },
      },
    ]),
  );
  const get = (id) => {
    assert.ok(elements.has(id), `Unknown DOM id: ${id}`);
    return elements.get(id);
  };
  get("report-data").textContent = JSON.stringify(model);
  let printed = false;
  const document = {
    getElementById: get,
    documentElement: { dataset: { theme: "dark" } },
  };
  const window = {
    print() {
      printed = true;
    },
    addEventListener() {},
  };
  vm.runInNewContext(
    code,
    { document, window, setTimeout, clearTimeout },
    { timeout: 5000 },
  );
  assert.ok(get("metrics").innerHTML.includes("Net P&amp;L"));
  assert.equal(
    (get("metrics").innerHTML.match(/class="metric"/g) || []).length,
    5,
  );
  assert.ok(get("configuration").innerHTML);
  for (const id of ["show-benchmark", "show-quotes", "show-complement"]) {
    get(id).checked = !get(id).checked;
    get(id).onchange();
  }
  assert.equal(get("benchmark-panel").hidden, false);
  get("theme").onclick();
  assert.equal(document.documentElement.dataset.theme, "light");
  get("theme").onclick();
  assert.equal(document.documentElement.dataset.theme, "dark");
  get("print").onclick();
  assert.ok(printed);
  if (model.pts.length) {
    get("pnl-chart").onpointermove({ clientX: width / 2 });
    get("pnl-chart").onkeydown({ key: "ArrowRight", preventDefault() {} });
    assert.ok(get("readout").textContent.includes("P&L"));
    assert.ok(get("market-chart").innerHTML.includes("stroke-dasharray"));
  } else assert.ok(get("pnl-chart").innerHTML.includes("No recorded samples"));
  if (model.blotter.length > 25) {
    assert.equal(get("next").disabled, false);
    get("next").onclick();
    assert.ok(get("page-label").textContent.startsWith("26"));
    get("prev").onclick();
    assert.ok(get("page-label").textContent.startsWith("1"));
  }
  if (model.depth.length) {
    get("depth-slider").value = String(model.depth.length - 1);
    get("depth-slider").oninput();
    assert.ok(get("depth-time").textContent.includes("UTC"));
  }
  for (const [id, e] of elements) {
    assert.ok(
      !/(?:NaN|Infinity|undefined)/.test(e.innerHTML + e.textContent),
      `Invalid output in ${id}`,
    );
  }
  return get;
}
const populated = structuredClone(blank);
populated.title = "Open position";
populated.context.market_name = "<img src=x onerror=alert(1)>";
populated.analysis = {
  ...populated.analysis,
  ending_equity: 10002,
  headline_pnl: 2,
  headline_return: 0.0002,
  unrealized_pnl: 2,
  avg_deployed: 0.002,
  recorded_samples: 2,
};
populated.pts = [
  {
    t: 1760000000000,
    y: 0.4,
    n: 0.6,
    b: 0.39,
    a: 0.41,
    p: 0,
    e: 0,
    q: 100,
    dd: 0,
    qb: 0.38,
    qa: 0.42,
    settlement: false,
  },
  {
    t: 1760000010000,
    y: 0.42,
    n: 0.58,
    b: 0.41,
    a: 0.43,
    p: 2,
    e: 500,
    q: 100,
    dd: 0,
    qb: null,
    qa: null,
    settlement: false,
  },
];
populated.depth = [
  {
    t: populated.pts[0].t,
    b: [
      [0.39, 100],
      [0.38, 20],
    ],
    a: [
      [0.41, 50],
      [0.42, 50],
    ],
  },
];
populated.fills = [
  { t: populated.pts[0].t, side: "buy", price: 0.4, qty: 0.1234 },
];
populated.blotter = Array.from({ length: 27 }, (_, i) => ({
  ts: populated.pts[0].t + i,
  side: "buy",
  price: 0.4,
  qty: 0.1234,
  position_after: (i + 1) * 0.1234,
  realized: 0,
}));
for (const width of [380, 1120]) {
  run(blank, width);
  const get = run(populated, width);
  assert.equal(
    get("market").textContent,
    `${populated.context.market_name} · Outcome: not supplied`,
  );
  assert.ok(get("reconcile").textContent.includes("RECONCILED"));
  const singleton = structuredClone(populated);
  singleton.pts = singleton.pts.slice(0, 1);
  run(singleton, width);
  if (dataText !== "__DATA__") run(JSON.parse(dataText), width);
}
console.log(
  `Report JavaScript smoke checks passed (empty, open inventory, one timestamp, controls, pagination, narrow/wide${dataText === "__DATA__" ? "" : ", generated report"}).`,
);
