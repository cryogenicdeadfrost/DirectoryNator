use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
enum Md {
    Map,
    Bench,
    Stress,
}

#[derive(Clone)]
enum Fm {
    Text,
    Bin,
    Both,
}

#[derive(Clone)]
enum Ps {
    Light,
    Balanced,
    Hard,
    Extreme,
}

#[derive(Clone)]
struct Cfg {
    md: Md,
    root: PathBuf,
    out: PathBuf,
    wk: Option<usize>,
    fast: bool,
    fm: Fm,
    ps: Ps,
    runs: usize,
    name: String,
}

struct St {
    q: VecDeque<PathBuf>,
    open: usize,
    done: bool,
}

#[derive(Clone)]
struct Rs {
    map: Arc<Mutex<HashMap<String, Vec<String>>>>,
    dirs: Arc<AtomicU64>,
    files: Arc<AtomicU64>,
    den: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
}

#[derive(Clone)]
struct Sm {
    ms: u128,
    wk: usize,
    dirs: u64,
    files: u64,
    den: u64,
    err: u64,
}

fn arg() -> Cfg {
    let mut md = Md::Map;
    let mut root = PathBuf::from(std::path::MAIN_SEPARATOR.to_string());
    let mut out = PathBuf::from("rust_nator/out");
    let mut wk = None;
    let mut fast = false;
    let mut fm = Fm::Both;
    let mut ps = Ps::Balanced;
    let mut runs = 1usize;
    let mut name = String::from("run");

    let mut it = env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--mode" => {
                if let Some(v) = it.next() {
                    md = match v.as_str() {
                        "map" => Md::Map,
                        "bench" => Md::Bench,
                        "stress" => Md::Stress,
                        _ => Md::Map,
                    }
                }
            }
            "--root" => {
                if let Some(v) = it.next() {
                    root = PathBuf::from(v)
                }
            }
            "--out" => {
                if let Some(v) = it.next() {
                    out = PathBuf::from(v)
                }
            }
            "--workers" => {
                if let Some(v) = it.next() {
                    wk = v.parse::<usize>().ok()
                }
            }
            "--fast" => fast = true,
            "--fmt" => {
                if let Some(v) = it.next() {
                    fm = match v.as_str() {
                        "text" => Fm::Text,
                        "bin" => Fm::Bin,
                        _ => Fm::Both,
                    }
                }
            }
            "--preset" => {
                if let Some(v) = it.next() {
                    ps = match v.as_str() {
                        "light" => Ps::Light,
                        "hard" => Ps::Hard,
                        "extreme" => Ps::Extreme,
                        _ => Ps::Balanced,
                    }
                }
            }
            "--runs" => {
                if let Some(v) = it.next() {
                    runs = v.parse::<usize>().ok().unwrap_or(1).max(1)
                }
            }
            "--name" => {
                if let Some(v) = it.next() {
                    name = v
                }
            }
            _ => {}
        }
    }

    Cfg {
        md,
        root,
        out,
        wk,
        fast,
        fm,
        ps,
        runs,
        name,
    }
}

fn cpu() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn rec(wk: Option<usize>, fast: bool) -> usize {
    if let Some(v) = wk {
        return v.max(1);
    }
    let c = cpu();
    if fast {
        (c * 2).min(256).max(2)
    } else if c <= 4 {
        c
    } else if c <= 16 {
        c + 2
    } else {
        ((c as f64) * 1.4) as usize
    }
}

fn mk_out(p: &Path) -> io::Result<()> {
    fs::create_dir_all(p)
}

fn now() -> String {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    format!("{}", t.as_secs())
}

fn scan(root: &Path, wk: usize) -> SmMap {
    let state = Arc::new((
        Mutex::new(St {
            q: {
                let mut q = VecDeque::new();
                q.push_back(root.to_path_buf());
                q
            },
            open: 0,
            done: false,
        }),
        Condvar::new(),
    ));

    let rs = Rs {
        map: Arc::new(Mutex::new(HashMap::new())),
        dirs: Arc::new(AtomicU64::new(0)),
        files: Arc::new(AtomicU64::new(0)),
        den: Arc::new(AtomicU64::new(0)),
        err: Arc::new(AtomicU64::new(0)),
    };

    let st = Instant::now();
    let mut hs = Vec::with_capacity(wk);

    for _ in 0..wk {
        let state_c = Arc::clone(&state);
        let rs_c = rs.clone();

        hs.push(thread::spawn(move || loop {
            let dir = {
                let (lk, cv) = (&state_c.0, &state_c.1);
                let mut s = lk.lock().unwrap();
                while s.q.is_empty() && !s.done {
                    s = cv.wait(s).unwrap();
                }
                if s.done {
                    None
                } else {
                    let d = s.q.pop_front();
                    if d.is_some() {
                        s.open += 1;
                    }
                    d
                }
            };

            let dir = match dir {
                Some(d) => d,
                None => break,
            };

            let k = dir.to_string_lossy().to_string();
            let mut fvec: Vec<String> = Vec::new();

            match fs::read_dir(&dir) {
                Ok(rd) => {
                    for e in rd {
                        match e {
                            Ok(en) => {
                                let p = en.path();
                                match en.file_type() {
                                    Ok(ft) => {
                                        if ft.is_dir() {
                                            rs_c.dirs.fetch_add(1, Ordering::Relaxed);
                                            let (lk, cv) = (&state_c.0, &state_c.1);
                                            let mut s = lk.lock().unwrap();
                                            s.q.push_back(p);
                                            cv.notify_one();
                                        } else if ft.is_file() {
                                            rs_c.files.fetch_add(1, Ordering::Relaxed);
                                            fvec.push(p.to_string_lossy().to_string());
                                        }
                                    }
                                    Err(_) => {
                                        rs_c.err.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(e) => {
                                if e.kind() == io::ErrorKind::PermissionDenied {
                                    rs_c.den.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    rs_c.err.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::PermissionDenied {
                        rs_c.den.fetch_add(1, Ordering::Relaxed);
                    } else {
                        rs_c.err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            {
                let mut mm = rs_c.map.lock().unwrap();
                mm.insert(k, fvec);
            }

            let (lk, cv) = (&state_c.0, &state_c.1);
            let mut s = lk.lock().unwrap();
            if s.open > 0 {
                s.open -= 1;
            }
            if s.q.is_empty() && s.open == 0 {
                s.done = true;
                cv.notify_all();
            }
        }));
    }

    for h in hs {
        let _ = h.join();
    }

    let ms = st.elapsed().as_millis();
    let map = Arc::try_unwrap(rs.map).unwrap().into_inner().unwrap();
    let sm = Sm {
        ms,
        wk,
        dirs: rs.dirs.load(Ordering::Relaxed),
        files: rs.files.load(Ordering::Relaxed),
        den: rs.den.load(Ordering::Relaxed),
        err: rs.err.load(Ordering::Relaxed),
    };

    SmMap { sm, map }
}

struct SmMap {
    sm: Sm,
    map: HashMap<String, Vec<String>>,
}

fn wr_txt(p: &Path, map: &HashMap<String, Vec<String>>) -> io::Result<()> {
    let f = File::create(p)?;
    let mut w = BufWriter::new(f);
    for (k, v) in map {
        writeln!(w, "{}:", k)?;
        for x in v {
            writeln!(w, "    {}", x)?;
        }
        writeln!(w)?;
    }
    w.flush()
}

fn wr_bin(p: &Path, map: &HashMap<String, Vec<String>>, sm: &Sm) -> io::Result<()> {
    let f = File::create(p)?;
    let mut w = BufWriter::new(f);
    w.write_all(b"DNRS1")?;
    w.write_all(&(map.len() as u64).to_le_bytes())?;

    for (k, v) in map {
        let kb = k.as_bytes();
        w.write_all(&(kb.len() as u32).to_le_bytes())?;
        w.write_all(kb)?;
        w.write_all(&(v.len() as u32).to_le_bytes())?;
        for x in v {
            let xb = x.as_bytes();
            w.write_all(&(xb.len() as u32).to_le_bytes())?;
            w.write_all(xb)?;
        }
    }

    w.write_all(&sm.ms.to_le_bytes())?;
    w.write_all(&(sm.wk as u64).to_le_bytes())?;
    w.write_all(&sm.dirs.to_le_bytes())?;
    w.write_all(&sm.files.to_le_bytes())?;
    w.write_all(&sm.den.to_le_bytes())?;
    w.write_all(&sm.err.to_le_bytes())?;
    w.flush()
}

fn wr_run(
    out: &Path,
    tag: &str,
    sm: &Sm,
    map: &HashMap<String, Vec<String>>,
    fm: &Fm,
) -> io::Result<()> {
    let ts = now();
    let txt = out.join(format!("dnrs_{}_{}.txt", tag, ts));
    let bin = out.join(format!("dnrs_{}_{}.bin", tag, ts));

    match fm {
        Fm::Text => wr_txt(&txt, map)?,
        Fm::Bin => wr_bin(&bin, map, sm)?,
        Fm::Both => {
            wr_txt(&txt, map)?;
            wr_bin(&bin, map, sm)?;
        }
    }
    Ok(())
}

fn sets(ps: &Ps, fast: bool) -> (usize, Vec<usize>) {
    let c = cpu();
    let base = if fast { (c * 2).min(256) } else { c };
    match ps {
        Ps::Light => (1, vec![1.max(c / 2), base]),
        Ps::Balanced => (2, vec![1.max(c / 2), c, (c * 2).min(256)]),
        Ps::Hard => (3, vec![c, (c * 2).min(256), (c * 3).min(256)]),
        Ps::Extreme => (
            4,
            vec![c, (c * 2).min(256), (c * 3).min(256), (c * 4).min(256)],
        ),
    }
}

fn bench(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let mut ws = if let Some(w) = cfg.wk {
        vec![w.max(1)]
    } else {
        sets(&cfg.ps, cfg.fast).1
    };
    ws.sort_unstable();
    ws.dedup();

    let mut recs: Vec<Sm> = Vec::new();
    for w in ws {
        let mut ms_all: Vec<u128> = Vec::new();
        let mut last = Sm {
            ms: 0,
            wk: w,
            dirs: 0,
            files: 0,
            den: 0,
            err: 0,
        };
        for _ in 0..cfg.runs {
            let r = scan(&cfg.root, w);
            last = r.sm.clone();
            ms_all.push(last.ms);
        }
        let avg = ms_all.iter().sum::<u128>() / (ms_all.len() as u128);
        last.ms = avg;
        recs.push(last);
    }

    recs.sort_by_key(|x| x.ms);
    let p = cfg.out.join(format!("dnrs_bench_{}.txt", now()));
    let f = File::create(&p)?;
    let mut w = BufWriter::new(f);
    writeln!(w, "root={}", cfg.root.display())?;
    writeln!(w, "runs={}", cfg.runs)?;
    writeln!(w, "fast={}", cfg.fast)?;
    for (i, r) in recs.iter().enumerate() {
        let tps = if r.ms == 0 {
            0.0
        } else {
            (r.files as f64) / (r.ms as f64 / 1000.0)
        };
        writeln!(
            w,
            "{}. workers={} avg_ms={} files={} dirs={} files_per_sec={:.2} den={} err={}",
            i + 1,
            r.wk,
            r.ms,
            r.files,
            r.dirs,
            tps,
            r.den,
            r.err
        )?;
    }
    w.flush()?;

    println!("benchmark ready: {}", p.display());
    if let Some(b) = recs.first() {
        let tps = if b.ms == 0 {
            0.0
        } else {
            (b.files as f64) / (b.ms as f64 / 1000.0)
        };
        println!(
            "best workers={} avg_ms={} files={} files_per_sec={:.2}",
            b.wk, b.ms, b.files, tps
        );
    }
    Ok(())
}

fn stress(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let (loops, ws) = sets(&cfg.ps, cfg.fast);
    let p = cfg.out.join(format!("dnrs_stress_{}.txt", now()));
    let f = File::create(&p)?;
    let mut w = BufWriter::new(f);

    writeln!(w, "root={}", cfg.root.display())?;
    writeln!(
        w,
        "preset={}",
        match cfg.ps {
            Ps::Light => "light",
            Ps::Balanced => "balanced",
            Ps::Hard => "hard",
            Ps::Extreme => "extreme",
        }
    )?;
    writeln!(w, "loops={} fast={}", loops, cfg.fast)?;

    for i in 0..loops {
        for ww in ws.clone() {
            let r = scan(&cfg.root, ww);
            let s = r.sm;
            writeln!(
                w,
                "cycle={} workers={} ms={} files={} dirs={} den={} err={}",
                i + 1,
                s.wk,
                s.ms,
                s.files,
                s.dirs,
                s.den,
                s.err
            )?;
            println!(
                "stress cycle {} workers {} -> {} ms files {}",
                i + 1,
                s.wk,
                s.ms,
                s.files
            );
        }
    }
    w.flush()?;
    println!("stress report: {}", p.display());
    Ok(())
}

fn map(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let wk = rec(cfg.wk, cfg.fast);
    let r = scan(&cfg.root, wk);
    wr_run(&cfg.out, &cfg.name, &r.sm, &r.map, &cfg.fm)?;
    println!("scan done root={}", cfg.root.display());
    println!(
        "workers={} ms={} dirs={} files={} den={} err={}",
        r.sm.wk, r.sm.ms, r.sm.dirs, r.sm.files, r.sm.den, r.sm.err
    );
    Ok(())
}

fn main() {
    let cfg = arg();
    if !cfg.root.exists() {
        eprintln!("root path not found: {}", cfg.root.display());
        std::process::exit(2);
    }

    let res = match cfg.md {
        Md::Map => map(&cfg),
        Md::Bench => bench(&cfg),
        Md::Stress => stress(&cfg),
    };

    if let Err(e) = res {
        eprintln!("run failed: {}", e);
        std::process::exit(1);
    }
}
