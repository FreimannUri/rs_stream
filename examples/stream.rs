#![no_std]
#![no_main]

use core::fmt::Write as _;
use core::sync::atomic::{AtomicI32, AtomicU32, Ordering::Relaxed};

use embassy_executor::Spawner;
use embassy_futures::{
    join::join4,
    select::{Either, select},
};
use embassy_net::{Runner, Stack, StackResources, tcp::TcpSocket};
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, channel::Channel};
use embassy_time::{Duration, Instant, Timer};
use esp_backtrace as _;
use esp_hal::{
    clock::CpuClock,
    dma::DmaRxBuf,
    lcd_cam::cam::EofMode,
    psram::{Psram, PsramConfig, SpiRamFreq, SpiTimingConfigCoreClock},
    rng::Rng,
    timer::timg::TimerGroup,
    tsens::{Config as TsConfig, TemperatureSensor},
};
use esp_println::println;
use esp_radio::wifi::{
    AuthenticationMethodConfig, Config as WifiConfig, Interface, PowerSaveMode, WifiController,
    sta::StationConfig,
};
use heapless::String;
use rs_stream::{
    Esp32S3Eye, ensure_flash_qe, jpeg_buffers, jpeg_slice,
    sensors::{Resolution, ov5640::Ov5640},
};
use static_cell::StaticCell;

esp_bootloader_esp_idf::esp_app_desc!();

const SSID: &str = "BALTICOM2G_3124301";
const PASSWORD: &str = "zeyhd4xpgmte";

const SERVER: (u8, u8, u8, u8) = (192, 168, 80, 136);
const INGEST_PORT: u16 = 9000;
const STATS_PORT: u16 = 9001;

// const RESOLUTION: Resolution = Resolution::Hd720;
const RESOLUTION: Resolution = Resolution::FullHd;
const JPEG_QSCALE: u8 = 0x04;

// const HTTP_PORT: u16 = 80;
const TCP_TX_BUF: usize = 32 * 1024;
// const TCP_RX_BUF: usize = 1024;

// Counters shared between tasks; `stats_push` reports and resets them once a second.
static CAPTURED: AtomicU32 = AtomicU32::new(0);
static INVALID: AtomicU32 = AtomicU32::new(0);
static ERRORS: AtomicU32 = AtomicU32::new(0);
static SENT: AtomicU32 = AtomicU32::new(0);
static SENT_BYTES: AtomicU32 = AtomicU32::new(0);
static SEND_US: AtomicU32 = AtomicU32::new(0);
static DROPPED: AtomicU32 = AtomicU32::new(0);
static FREE_WAIT_US: AtomicU32 = AtomicU32::new(0);
static LAG_MAX_US: AtomicU32 = AtomicU32::new(0);
// Totals since boot (never reset)
static WIFI_DROPS: AtomicU32 = AtomicU32::new(0);
static PUSH_DROPS: AtomicU32 = AtomicU32::new(0);
static RSSI: AtomicI32 = AtomicI32::new(0);

macro_rules! mk_static {
    ($t:ty, $val:expr) => {{
        static CELL: StaticCell<$t> = StaticCell::new();
        CELL.init($val)
    }};
}

trait WriteAll {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), embassy_net::tcp::Error>;
}

impl WriteAll for TcpSocket<'_> {
    async fn write_all(&mut self, mut data: &[u8]) -> Result<(), embassy_net::tcp::Error> {
        while !data.is_empty() {
            let n = self.write(data).await?;
            if n == 0 {
                return Err(embassy_net::tcp::Error::ConnectionReset);
            }
            data = &data[n..];
        }
        Ok(())
    }
}

type BufChannel = Channel<NoopRawMutex, DmaRxBuf, 2>;

#[embassy_executor::task]
async fn wifi_task(mut controller: WifiController<'static>) {
    let cfg = WifiConfig::Station(
        StationConfig::default()
            .with_ssid(match SSID.try_into() {
                Ok(s) => s,
                Err(_) => panic!("SSID is too long"),
            })
            .with_authentication(AuthenticationMethodConfig::Wpa2Personal(
                match PASSWORD.try_into() {
                    Ok(p) => p,
                    Err(_) => panic!("Wrong Wi-Fi password"),
                },
            )),
    );

    controller.set_config(&cfg).unwrap();

    controller.set_power_saving(PowerSaveMode::None).unwrap();

    loop {
        match controller.connect_async().await {
            Ok(_) => {
                println!("[wifi] connected");
                loop {
                    let ev =
                        select(controller.wait_for_disconnect_async(), Timer::after_secs(1)).await;
                    match ev {
                        Either::First(_) => break,
                        Either::Second(_) => {
                            if let Ok(r) = controller.rssi() {
                                RSSI.store(r, Relaxed);
                            }
                        }
                    }
                }
                WIFI_DROPS.fetch_add(1, Relaxed);
                println!("[wifi] disconnected, reconnecting");
            }
            Err(e) => println!("[wifi] connect failed: {:?}", e),
        }
        Timer::after_millis(1000).await;
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, Interface>) {
    runner.run().await
}

async fn capture_loop(eye: &mut Esp32S3Eye<Ov5640>, free: &BufChannel, filled: &BufChannel) -> ! {
    let (mut total, mut invalid, mut errors) = (0u32, 0u32, 0u32);
    let mut t0 = Instant::now();
    loop {
        let waited = Instant::now();
        let buf = free.receive().await;
        FREE_WAIT_US.fetch_add(waited.elapsed().as_micros() as u32, Relaxed);
        let started = Instant::now();
        match eye.capture(buf).await {
            Ok(buf) => {
                total += 1;
                CAPTURED.fetch_add(1, Relaxed);

                if jpeg_slice(&buf).is_some() {
                    filled.send(buf).await;
                } else {
                    invalid += 1;
                    INVALID.fetch_add(1, Relaxed);
                    free.send(buf).await;
                }
            }
            Err((e, buf)) => {
                errors += 1;
                ERRORS.fetch_add(1, Relaxed);

                let rx = buf.number_of_received_bytes();
                let valid = jpeg_slice(&buf).is_some();
                println!(
                    "[cam] {:?} after {} ms: rx {} B, jpeg {}",
                    e,
                    started.elapsed().as_millis(),
                    rx,
                    if valid {
                        "VALID (salvaged)"
                    } else {
                        "incomplete"
                    }
                );
                if valid {
                    filled.send(buf).await;
                } else {
                    free.send(buf).await;
                    Timer::after_millis(10).await;
                }
            }
        }
        if t0.elapsed() >= Duration::from_secs(1) {
            println!(
                "[cap] captures/s {} (invalid {}), errors {}",
                total, invalid, errors
            );
            (total, invalid, errors) = (0, 0, 0);
            t0 = Instant::now();
        }
    }
}

async fn push(stack: Stack<'static>, free: &BufChannel, filled: &BufChannel) -> ! {
    let rx = mk_static!([u8; 64], [0; 64]);
    let tx = mk_static!([u8; TCP_TX_BUF], [0; TCP_TX_BUF]);
    let ep = (
        embassy_net::Ipv4Address::new(SERVER.0, SERVER.1, SERVER.2, SERVER.3),
        INGEST_PORT,
    );

    loop {
        let mut sock = TcpSocket::new(stack, rx, tx);
        sock.set_timeout(Some(Duration::from_secs(5)));

        if sock.connect(ep).await.is_err() {
            // laptop/ffmpeg not listening yet: drop stale frames and retry
            while let Ok(b) = filled.try_receive() {
                free.send(b).await;
            }
            Timer::after_millis(1000).await;
            continue;
        }
        println!("[push] connected");

        loop {
            let mut buf = filled.receive().await;
            // keep only the newest frame
            while let Ok(newer) = filled.try_receive() {
                free.send(buf).await;
                buf = newer;
                DROPPED.fetch_add(1, Relaxed);
            }
            let ok = match jpeg_slice(&buf) {
                Some(j) => {
                    let t = Instant::now();
                    let r = sock.write_all(j).await.is_ok();
                    if r {
                        SENT.fetch_add(1, Relaxed);
                        SENT_BYTES.fetch_add(j.len() as u32, Relaxed);
                        SEND_US.fetch_add(t.elapsed().as_micros() as u32, Relaxed);
                    }
                    r
                }
                None => true,
            };
            free.send(buf).await;
            if !ok {
                break;
            }
        }
        sock.abort();
        PUSH_DROPS.fetch_add(1, Relaxed);
        println!("[push] disconnected");
    }
}

// Measures how late a 10 ms timer fires: a proxy for how busy the executor is.
async fn lag_probe() -> ! {
    loop {
        let t = Instant::now();
        Timer::after_millis(10).await;
        let lag = t.elapsed().as_micros().saturating_sub(10_000) as u32;
        LAG_MAX_US.fetch_max(lag, Relaxed);
    }
}

// Sends one JSON line per second to the collector on the laptop.
async fn stats_push(stack: Stack<'static>, mut tsens: TemperatureSensor<'static>) -> ! {
    let rx = mk_static!([u8; 64], [0; 64]);
    let tx = mk_static!([u8; 512], [0; 512]);
    let ep = (
        embassy_net::Ipv4Address::new(SERVER.0, SERVER.1, SERVER.2, SERVER.3),
        STATS_PORT,
    );
    let boot = Instant::now();

    loop {
        let mut sock = TcpSocket::new(stack, rx, tx);
        sock.set_timeout(Some(Duration::from_secs(5)));
        if sock.connect(ep).await.is_err() {
            Timer::after_millis(1000).await;
            continue;
        }
        println!("[stats] connected");
        // don't report what piled up while disconnected
        for c in [
            &CAPTURED,
            &INVALID,
            &ERRORS,
            &SENT,
            &SENT_BYTES,
            &SEND_US,
            &DROPPED,
            &FREE_WAIT_US,
            &LAG_MAX_US,
        ] {
            c.store(0, Relaxed);
        }
        loop {
            Timer::after_secs(1).await;
            let sent = SENT.swap(0, Relaxed);
            let bytes = SENT_BYTES.swap(0, Relaxed);
            let us = SEND_US.swap(0, Relaxed);
            let send_ms = if sent > 0 { us / sent / 1000 } else { 0 };
            let kb_frame = if sent > 0 { bytes / sent / 1024 } else { 0 };
            let wait_pct = FREE_WAIT_US.swap(0, Relaxed) / 10_000;
            let lag_ms = LAG_MAX_US.swap(0, Relaxed) / 1000;
            let heap_kb = (esp_alloc::HEAP.free() / 1024) as u32;
            let temp_c = tsens.get_temperature().to_celsius() as i32;

            let mut line: String<384> = String::new();
            let _ = write!(
                line,
                "{{\"up\":{},\"cap\":{},\"inv\":{},\"err\":{},\"fps\":{},\"kbs\":{},\"send_ms\":{},\"drop\":{},\
                    \"wait\":{},\"kb_frame\":{},\"lag\":{},\"wdrops\":{},\"pdrops\":{},\
                    \"heap\":{},\"rssi\":{},\"temp\":{}}}\n",
                boot.elapsed().as_secs(),
                CAPTURED.swap(0, Relaxed),
                INVALID.swap(0, Relaxed),
                ERRORS.swap(0, Relaxed),
                sent,
                bytes / 1024,
                send_ms,
                DROPPED.swap(0, Relaxed),
                wait_pct,
                kb_frame,
                lag_ms,
                WIFI_DROPS.load(Relaxed),
                PUSH_DROPS.load(Relaxed),
                heap_kb,
                RSSI.load(Relaxed),
                temp_c
            );
            if sock.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
        sock.abort();
        println!("[stats] disconnected");
    }
}

#[esp_rtos::main]
async fn main(spawner: Spawner) {
    let p = esp_hal::init(esp_hal::Config::default().with_cpu_clock(CpuClock::max()));

    esp_alloc::heap_allocator!(#[esp_hal::ram(reclaimed)] size: 64 * 1024);
    esp_alloc::heap_allocator!(size: 96 * 1024);
    ensure_flash_qe();

    let timg0 = TimerGroup::new(p.TIMG0);
    esp_rtos::start(timg0.timer0);

    let psram = Psram::new(
        p.PSRAM,
        PsramConfig {
            ram_frequency: SpiRamFreq::Freq80m,
            core_clock: Some(SpiTimingConfigCoreClock::SpiTimingConfigCoreClock160m),
            ..Default::default()
        },
    );
    let (psram_base, _psram_size) = psram.raw_parts();
    let [buf_a, buf_b] = jpeg_buffers(psram_base);

    let controller = WifiController::new(p.WIFI, Default::default()).unwrap();
    let sta = Interface::station();

    let rng = Rng::new();
    let seed = (rng.random() as u64) << 32 | rng.random() as u64;
    let (stack, runner) = embassy_net::new(
        sta,
        embassy_net::Config::dhcpv4(Default::default()),
        mk_static!(StackResources<6>, StackResources::<6>::new()),
        seed,
    );
    spawner.spawn(wifi_task(controller).expect("REASON"));
    spawner.spawn(net_task(runner).expect("REASON"));

    let mut eye = Esp32S3Eye::<Ov5640>::init(
        p.LCD_CAM,
        p.DMA_CH0,
        EofMode::VsyncSignal,
        p.I2C0,
        p.GPIO15, // xclk
        p.GPIO13, // pclk
        p.GPIO6,  // vsync
        p.GPIO7,  // href
        p.GPIO11,
        p.GPIO9,
        p.GPIO8,
        p.GPIO10, // d0-d3
        p.GPIO12,
        p.GPIO18,
        p.GPIO17,
        p.GPIO16, // d4-d7
        p.GPIO4,  // sda
        p.GPIO5,  // scl
    )
    .unwrap();

    eye.set_jpeg(RESOLUTION, JPEG_QSCALE).unwrap();
    Timer::after_millis(300).await;

    let tsens = TemperatureSensor::new(p.TSENS, TsConfig::default()).unwrap();
    Timer::after_micros(300).await;

    let free = BufChannel::new();
    let filled = BufChannel::new();
    free.send(buf_a).await;
    free.send(buf_b).await;

    stack.wait_config_up().await;
    if let Some(cfg) = stack.config_v4() {
        println!("[main] stream: http://{}/stream", cfg.address.address());
    }

    join4(
        capture_loop(&mut eye, &free, &filled),
        push(stack, &free, &filled),
        stats_push(stack, tsens),
        lag_probe(),
    )
    .await;
}
