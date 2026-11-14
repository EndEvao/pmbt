//! A dedicated socket worker keeps heartbeats independent of engine/disk work.
use crate::{record::Capture, Driver, Input, Result};
use futures_util::{SinkExt, StreamExt};
use pmbt::engine::paper::PaperStatus;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

pub const MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLL: Duration = Duration::from_millis(100);

struct Clock {
    base: i64,
    start: Instant,
}
impl Clock {
    fn new() -> Self {
        Self {
            base: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before epoch")
                .as_micros() as i64,
            start: Instant::now(),
        }
    }
    fn now(&self) -> i64 {
        self.base + self.start.elapsed().as_micros() as i64
    }
}
struct Control {
    epoch: AtomicU64,
    stop: AtomicBool,
    reason: Mutex<String>,
}
impl Control {
    fn invalidate(&self, reason: String) {
        *self.reason.lock().expect("health mutex poisoned") = reason;
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }
}
struct Packet {
    epoch: u64,
    received_us: i64,
    raw: Option<String>,
}

fn send(tx: &SyncSender<Packet>, epoch: u64, clock: &Clock, raw: Option<String>) -> Result<()> {
    tx.try_send(Packet {
        epoch,
        received_us: clock.now(),
        raw,
    })
    .map_err(|_| "feed queue full or receiver closed".into())
}

async fn connection(
    url: &str,
    assets: &[String; 2],
    tx: &SyncSender<Packet>,
    control: &Control,
    clock: &Clock,
    epoch: u64,
) -> Result<()> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(1024 * 1024))
        .max_frame_size(Some(1024 * 1024));
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(url, Some(config), true),
    )
    .await??;
    tokio::time::timeout(
        Duration::from_secs(10),
        ws.send(Message::Text(
            serde_json::json!({"type":"market","assets_ids":assets})
                .to_string()
                .into(),
        )),
    )
    .await??;
    send(tx, epoch, clock, None)?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut check = tokio::time::interval(POLL);
    let mut unanswered: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = check.tick() => {
                if control.stop.load(Ordering::SeqCst) || control.epoch.load(Ordering::SeqCst) != epoch { return Ok(()); }
                if unanswered.is_some_and(|at| at.elapsed() >= Duration::from_secs(20)) { return Err("market heartbeat timed out".into()); }
            }
            _ = heartbeat.tick() => {
                tokio::time::timeout(Duration::from_secs(5), ws.send(Message::Text("PING".into()))).await??;
                if unanswered.is_none() { unanswered = Some(Instant::now()); }
            }
            frame = ws.next() => {
                match frame {
                    Some(Ok(Message::Text(raw))) if raw == "PONG" => { unanswered = None; }
                    Some(Ok(Message::Text(raw))) => send(tx,epoch,clock,Some(raw.to_string()))?,
                    Some(Ok(Message::Ping(_))) => { tokio::time::timeout(Duration::from_secs(5),ws.flush()).await??; }
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Ok(Message::Close(_))) | None => return Err("market connection closed".into()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(_)) => return Err("unexpected non-text market frame".into()),
                }
            }
        }
    }
}

async fn watch_signals(control: Arc<Control>) {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        } else {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    control.stop.store(true, Ordering::SeqCst);
}

struct Worker {
    control: Arc<Control>,
    handle: Option<thread::JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.control.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Run until a signal or the optional duration, retaining in-memory session state
/// across reconnects. A process restart still requires a new session directory.
pub fn run(
    driver: &mut Driver,
    capture: &mut Capture,
    assets: [String; 2],
    duration: Option<Duration>,
) -> Result<PaperStatus> {
    run_at(MARKET_URL, driver, capture, assets, duration, true)
}

fn run_at(
    url: &str,
    driver: &mut Driver,
    capture: &mut Capture,
    assets: [String; 2],
    duration: Option<Duration>,
    signals: bool,
) -> Result<PaperStatus> {
    let clock = Arc::new(Clock::new());
    let control = Arc::new(Control {
        epoch: AtomicU64::new(1),
        stop: AtomicBool::new(false),
        reason: Mutex::new("starting".into()),
    });
    // At most 64 frames of <=1MiB, plus backpressure age protection below.
    let (tx, rx) = mpsc::sync_channel(64);
    let c = control.clone();
    let worker_clock = clock.clone();
    let url = url.to_owned();
    let handle = thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                c.invalidate(e.to_string());
                c.stop.store(true, Ordering::SeqCst);
                return;
            }
        };
        runtime.block_on(async {
            if signals {
                tokio::spawn(watch_signals(c.clone()));
            }
            let mut attempts = 0u32;
            while !c.stop.load(Ordering::SeqCst) {
                let epoch = c.epoch.load(Ordering::SeqCst);
                let started = Instant::now();
                let result = connection(&url, &assets, &tx, &c, &worker_clock, epoch).await;
                if c.stop.load(Ordering::SeqCst) {
                    break;
                }
                if started.elapsed() >= Duration::from_secs(60) {
                    attempts = 0;
                }
                c.invalidate(
                    result
                        .err()
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "resynchronization requested".into()),
                );
                let delay_ms = (500u64 * (1u64 << attempts.min(6))).min(30_000)
                    + worker_clock.now().unsigned_abs() % 251;
                attempts += 1;
                let until = Instant::now() + Duration::from_millis(delay_ms);
                while Instant::now() < until && !c.stop.load(Ordering::SeqCst) {
                    tokio::time::sleep(POLL).await;
                }
            }
        });
    });
    let _worker = Worker {
        control: control.clone(),
        handle: Some(handle),
    };
    let start = Instant::now();
    let mut epoch = control.epoch.load(Ordering::SeqCst);
    let mut last = 0i64;
    let mut connected_at: Option<Instant> = None;
    let mut synchronized_once = false;
    let mut status_at = Instant::now();
    capture.status(&driver.status(), "connecting")?;
    loop {
        if control.stop.load(Ordering::SeqCst) || duration.is_some_and(|d| start.elapsed() >= d) {
            break;
        }
        let current = control.epoch.load(Ordering::SeqCst);
        if current != epoch {
            epoch = current;
            let reason = control
                .reason
                .lock()
                .map_err(|_| "health mutex poisoned")?
                .clone();
            last = clock.now().max(last + 1);
            let input = Input::Gap {
                at_us: last,
                reason: reason.clone(),
            };
            capture.input(&input)?;
            driver.apply(&input, capture)?;
            capture.status(&driver.status(), "disconnected")?;
            eprintln!(
                "paper: connection lost: {reason}; working orders retained, missed fills unknown"
            );
            connected_at = None;
        }
        if connected_at.is_some_and(|at| at.elapsed() > Duration::from_secs(30))
            && !driver.status().ready
        {
            control.invalidate("initial outcome snapshots timed out".into());
            continue;
        }
        let wait = driver
            .next_deadline()
            .map(|at| Duration::from_micros(at.saturating_sub(clock.now()).max(0) as u64).min(POLL))
            .unwrap_or(POLL);
        match rx.recv_timeout(wait) {
            Ok(packet) => {
                if packet.epoch != epoch || control.epoch.load(Ordering::SeqCst) != epoch {
                    continue;
                }
                if clock.now() - packet.received_us > 2_000_000 {
                    control.invalidate("feed backlog exceeded 2 seconds".into());
                    continue;
                }
                last = packet.received_us.max(last + 1);
                let input = if let Some(raw) = packet.raw {
                    Input::Frame {
                        at_us: last,
                        received_us: packet.received_us,
                        raw,
                    }
                } else {
                    connected_at = Some(Instant::now());
                    Input::Connected { at_us: last }
                };
                capture.input(&input)?;
                if let Err(error) = driver.apply(&input, capture) {
                    if error.downcast_ref::<crate::ResyncRequired>().is_some() {
                        control.invalidate(error.to_string());
                    } else {
                        return Err(error);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if control.epoch.load(Ordering::SeqCst) != epoch {
                    continue;
                }
                let now = clock.now();
                if driver.next_deadline().is_some_and(|at| at <= now) {
                    last = now.max(last + 1);
                    let input = Input::Tick { at_us: last };
                    capture.input(&input)?;
                    driver.apply(&input, capture)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("socket worker exited unexpectedly".into())
            }
        }
        if status_at.elapsed() >= Duration::from_secs(1) {
            let status = driver.status();
            capture.status(&status, if status.ready { "running" } else { "syncing" })?;
            status_at = Instant::now();
        }
        synchronized_once |= driver.status().ready;
    }
    if !synchronized_once {
        return Err("session ended before fresh books for both outcomes were received".into());
    }
    let input = Input::Stop {
        at_us: clock.now().max(last + 1),
    };
    capture.input(&input)?;
    driver.apply(&input, capture)?;
    Ok(driver.status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{books, config, driver, temp};

    #[test]
    fn real_websocket_subscribes_pings_and_reconnects_without_rebuying() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let server = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let listener=tokio::net::TcpListener::from_std(listener).unwrap();
                let mut pings=0;
                for _ in 0..2 {
                    let (stream,_)=tokio::time::timeout(Duration::from_secs(8),listener.accept()).await.unwrap().unwrap();
                    let mut ws=tokio_tungstenite::accept_async(stream).await.unwrap();
                    let subscription=ws.next().await.unwrap().unwrap().into_text().unwrap();
                    let value:serde_json::Value=serde_json::from_str(&subscription).unwrap();
                    assert_eq!(value["assets_ids"],serde_json::json!(["yes","no"]));
                    ws.send(Message::Text(books().into())).await.unwrap();
                    let close=tokio::time::sleep(Duration::from_millis(350));tokio::pin!(close);
                    loop {
                        tokio::select! {
                            _ = &mut close => {let _=ws.close(None).await;break;}
                            frame = ws.next() => match frame {
                                Some(Ok(Message::Text(s))) if s=="PING" => {pings+=1;ws.send(Message::Text("PONG".into())).await.unwrap();}
                                None | Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                    }
                }
                pings
            })
        });
        let dir = temp();
        let mut out = Capture::create(&dir, &config()).unwrap();
        let mut d = driver();
        let status = run_at(
            &url,
            &mut d,
            &mut out,
            ["yes".into(), "no".into()],
            Some(Duration::from_secs(3)),
            false,
        )
        .unwrap();
        assert_eq!(status.cash, 90.);
        assert_eq!(status.assets[0].fills, 1);
        assert_eq!(status.assets[1].fills, 1);
        assert!(status.gaps >= 1);
        assert!(server.join().unwrap() >= 2);
        out.finish(&status, "stopped").unwrap();
        let contents = std::fs::read_to_string(dir.join("events-000000.jsonl")).unwrap();
        assert!(contents.matches("\"type\":\"connected\"").count() >= 2);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
