use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
enum Md {
    Menu,
    Map,
    Bench,
    Stress,
    Disk,
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

#[derive(Clone)]
struct Hw {
    os: String,
    arch: String,
    cores: usize,
    ram_mb: u64,
}

struct St {
    q: VecDeque<(PathBuf, usize)>,
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
    deep: Arc<AtomicU64>,
}

#[derive(Clone)]
struct Sm {
    ms: u128,
    wk: usize,
    dirs: u64,
    files: u64,
    den: u64,
    err: u64,
    deep: u64,
    fps: f64,
    score: f64,
}

#[derive(Clone)]
struct Dk {
    mode: String,
    path: String,
    write_mb_s: f64,
    read_mb_s: f64,
    create_ops_s: f64,
    delete_ops_s: f64,
    files: usize,
    total_mb: usize,
}

struct SmMap {
    sm: Sm,
    map: HashMap<String, Vec<String>>,
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn now() -> String {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    format!("{}", t.as_secs())
}

fn cpu() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn ram_mb() -> u64 {
    if let Ok(s) = fs::read_to_string("/proc/meminfo") {
        for ln in s.lines() {
            if let Some(v) = ln.strip_prefix("MemTotal:") {
                let n = v
                    .split_whitespace()
                    .next()
                    .unwrap_or("0")
                    .parse::<u64>()
                    .unwrap_or(0);
                return n / 1024;
            }
        }
    }
    0
}

fn hw() -> Hw {
    Hw {
        os: env::consts::OS.to_string(),
        arch: env::consts::ARCH.to_string(),
        cores: cpu(),
        ram_mb: ram_mb(),
    }
}

fn hw_json(h: &Hw) -> String {
    format!(
        "{{\"os\":\"{}\",\"arch\":\"{}\",\"cores\":{},\"ram_mb\":{}}}",
        esc(&h.os),
        esc(&h.arch),
        h.cores,
        h.ram_mb
    )
}

fn sm_json(s: &Sm) -> String {
    format!(
        "{{\"ms\":{},\"wk\":{},\"dirs\":{},\"files\":{},\"den\":{},\"err\":{},\"deep\":{},\"fps\":{:.2},\"score\":{:.2}}}",
        s.ms, s.wk, s.dirs, s.files, s.den, s.err, s.deep, s.fps, s.score
    )
}

fn out_json(mode: &str, root: &str, hw: &Hw, stats: &[Sm]) -> String {
    let s = stats.iter().map(sm_json).collect::<Vec<_>>().join(",");
    format!(
        "{{\"mode\":\"{}\",\"root\":\"{}\",\"hw\":{},\"stats\":[{}]}}",
        esc(mode),
        esc(root),
        hw_json(hw),
        s
    )
}

fn dk_json(d: &Dk) -> String {
    format!(
        "{{\"mode\":\"{}\",\"path\":\"{}\",\"write_mb_s\":{:.2},\"read_mb_s\":{:.2},\"create_ops_s\":{:.2},\"delete_ops_s\":{:.2},\"files\":{},\"total_mb\":{}}}",
        esc(&d.mode),
        esc(&d.path),
        d.write_mb_s,
        d.read_mb_s,
        d.create_ops_s,
        d.delete_ops_s,
        d.files,
        d.total_mb
    )
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

fn score(files: u64, ms: u128, deep: u64, den: u64, err: u64) -> (f64, f64) {
    let fps = if ms == 0 {
        files as f64
    } else {
        files as f64 / (ms as f64 / 1000.0)
    };
    let depth_bonus = (deep as f64 * 0.2).max(1.0);
    let penalty = (den + err) as f64 * 0.5;
    let sc = (fps * depth_bonus - penalty).max(0.0);
    (fps, sc)
}

fn mk_out(p: &Path) -> io::Result<()> {
    fs::create_dir_all(p)
}

fn arg() -> Cfg {
    let mut md = Md::Menu;
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
                        "disk" => Md::Disk,
                        _ => Md::Menu,
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

fn ask(s: &str) -> String {
    print!("{}", s);
    let _ = io::stdout().flush();
    let mut b = String::new();
    let _ = io::stdin().read_line(&mut b);
    b.trim().to_string()
}

fn menu(mut cfg: Cfg) -> Cfg {
    let h = hw();
    println!("DirectoryNator RS");
    println!(
        "os={} arch={} cores={} ram_mb={}",
        h.os, h.arch, h.cores, h.ram_mb
    );
    println!("1) map 2) bench 3) stress 4) disk");
    let c = ask("choice: ");
    cfg.md = match c.as_str() {
        "1" => Md::Map,
        "2" => Md::Bench,
        "3" => Md::Stress,
        "4" => Md::Disk,
        _ => Md::Map,
    };
    let r = ask(&format!("root [{}]: ", cfg.root.display()));
    if !r.is_empty() {
        cfg.root = PathBuf::from(r);
    }
    let f = ask("fast mode y/n [n]: ");
    cfg.fast = f.eq_ignore_ascii_case("y");
    let o = ask(&format!("out [{}]: ", cfg.out.display()));
    if !o.is_empty() {
        cfg.out = PathBuf::from(o);
    }
    cfg
}

fn scan(root: &Path, wk: usize) -> SmMap {
    let state = Arc::new((
        Mutex::new(St {
            q: {
                let mut q = VecDeque::new();
                q.push_back((root.to_path_buf(), 0));
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
        deep: Arc::new(AtomicU64::new(0)),
    };

    let st = Instant::now();
    let mut hs = Vec::with_capacity(wk);

    for _ in 0..wk {
        let state_c = Arc::clone(&state);
        let rs_c = rs.clone();
        hs.push(thread::spawn(move || loop {
            let it = {
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

            let (dir, dep) = match it {
                Some(v) => v,
                None => break,
            };

            rs_c.deep.fetch_max(dep as u64, Ordering::Relaxed);
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
                                            s.q.push_back((p, dep + 1));
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
    let dirs = rs.dirs.load(Ordering::Relaxed);
    let files = rs.files.load(Ordering::Relaxed);
    let den = rs.den.load(Ordering::Relaxed);
    let err = rs.err.load(Ordering::Relaxed);
    let deep = rs.deep.load(Ordering::Relaxed);
    let (fps, score) = score(files, ms, deep, den, err);

    let map = Arc::try_unwrap(rs.map).unwrap().into_inner().unwrap();
    SmMap {
        sm: Sm {
            ms,
            wk,
            dirs,
            files,
            den,
            err,
            deep,
            fps,
            score,
        },
        map,
    }
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
    w.write_all(b"DNRS2")?;
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
    w.write_all(&sm.deep.to_le_bytes())?;
    w.flush()
}

fn wr_str(p: &Path, s: &str) -> io::Result<()> {
    let mut f = File::create(p)?;
    f.write_all(s.as_bytes())?;
    Ok(())
}

fn wr_run(
    out: &Path,
    tag: &str,
    sm: &Sm,
    map: &HashMap<String, Vec<String>>,
    fm: &Fm,
) -> io::Result<()> {
    let t = now();
    let txt = out.join(format!("dnrs_{}_{}.txt", tag, t));
    let bin = out.join(format!("dnrs_{}_{}.bin", tag, t));
    let js = out.join(format!("dnrs_{}_{}.json", tag, t));
    match fm {
        Fm::Text => wr_txt(&txt, map)?,
        Fm::Bin => wr_bin(&bin, map, sm)?,
        Fm::Both => {
            wr_txt(&txt, map)?;
            wr_bin(&bin, map, sm)?;
        }
    }
    wr_str(&js, &out_json("map", tag, &hw(), std::slice::from_ref(sm)))?;
    Ok(())
}

fn sets(ps: &Ps, fast: bool) -> (usize, Vec<usize>) {
    let c = cpu();
    let b = if fast { (c * 2).min(256) } else { c };
    match ps {
        Ps::Light => (1, vec![1.max(c / 2), b]),
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
        let mut all: Vec<Sm> = Vec::new();
        for _ in 0..cfg.runs {
            all.push(scan(&cfg.root, w).sm);
        }
        let mut x = all[0].clone();
        x.ms = all.iter().map(|r| r.ms).sum::<u128>() / all.len() as u128;
        x.fps = all.iter().map(|r| r.fps).sum::<f64>() / all.len() as f64;
        x.score = all.iter().map(|r| r.score).sum::<f64>() / all.len() as f64;
        recs.push(x);
    }

    recs.sort_by_key(|x| x.ms);
    let p = cfg.out.join(format!("dnrs_bench_{}.txt", now()));
    let mut w = BufWriter::new(File::create(&p)?);
    writeln!(w, "root={}", cfg.root.display())?;
    writeln!(w, "runs={}", cfg.runs)?;
    writeln!(w, "fast={}", cfg.fast)?;
    for (i, r) in recs.iter().enumerate() {
        writeln!(
            w,
            "{}. workers={} avg_ms={} files={} dirs={} files_per_sec={:.2} depth={} score={:.2} den={} err={}",
            i + 1,
            r.wk,
            r.ms,
            r.files,
            r.dirs,
            r.fps,
            r.deep,
            r.score,
            r.den,
            r.err
        )?;
    }
    w.flush()?;

    let js = cfg.out.join(format!("dnrs_bench_{}.json", now()));
    wr_str(
        &js,
        &out_json("bench", &cfg.root.display().to_string(), &hw(), &recs),
    )?;

    println!("benchmark ready: {}", p.display());
    println!("benchmark json: {}", js.display());
    if let Some(b) = recs.first() {
        println!(
            "best workers={} avg_ms={} fps={:.2} score={:.2}",
            b.wk, b.ms, b.fps, b.score
        );
    }
    Ok(())
}

fn stress(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let (loops, ws) = sets(&cfg.ps, cfg.fast);
    let mut rows: Vec<Sm> = Vec::new();
    for i in 0..loops {
        for ww in ws.clone() {
            let s = scan(&cfg.root, ww).sm;
            println!(
                "stress cycle {} workers {} -> {} ms score {:.2}",
                i + 1,
                s.wk,
                s.ms,
                s.score
            );
            rows.push(s);
        }
    }
    let js = cfg.out.join(format!("dnrs_stress_{}.json", now()));
    wr_str(
        &js,
        &out_json("stress", &cfg.root.display().to_string(), &hw(), &rows),
    )?;
    println!("stress report: {}", js.display());
    Ok(())
}

fn disk(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let base = cfg.out.join(format!("dnrs_disk_{}", now()));
    fs::create_dir_all(&base)?;
    let fp = base.join("blob.bin");
    let mb = 64usize;
    let block = vec![0xAB; 1024 * 1024];

    let stw = Instant::now();
    {
        let mut f = File::create(&fp)?;
        for _ in 0..mb {
            f.write_all(&block)?;
        }
        f.flush()?;
    }
    let wms = stw.elapsed().as_millis().max(1);

    let strd = Instant::now();
    {
        let mut f = File::open(&fp)?;
        let mut b = vec![0u8; 1024 * 1024];
        while f.read(&mut b)? > 0 {}
    }
    let rms = strd.elapsed().as_millis().max(1);

    let cnt = 400usize;
    let tiny = vec![1u8; 1024];
    let stc = Instant::now();
    for i in 0..cnt {
        let p = base.join(format!("t{}.dat", i));
        let mut f = File::create(p)?;
        f.write_all(&tiny)?;
    }
    let cms = stc.elapsed().as_millis().max(1);

    let stdel = Instant::now();
    for i in 0..cnt {
        let p = base.join(format!("t{}.dat", i));
        let _ = fs::remove_file(p);
    }
    let dms = stdel.elapsed().as_millis().max(1);

    let _ = fs::remove_file(&fp);
    let _ = fs::remove_dir_all(&base);

    let dk = Dk {
        mode: "disk".to_string(),
        path: cfg.out.display().to_string(),
        write_mb_s: mb as f64 / (wms as f64 / 1000.0),
        read_mb_s: mb as f64 / (rms as f64 / 1000.0),
        create_ops_s: cnt as f64 / (cms as f64 / 1000.0),
        delete_ops_s: cnt as f64 / (dms as f64 / 1000.0),
        files: cnt,
        total_mb: mb,
    };

    let js = cfg.out.join(format!("dnrs_disk_{}.json", now()));
    wr_str(&js, &dk_json(&dk))?;
    println!(
        "disk json: {} write={:.2}MB/s read={:.2}MB/s create_ops={:.2}/s delete_ops={:.2}/s",
        js.display(),
        dk.write_mb_s,
        dk.read_mb_s,
        dk.create_ops_s,
        dk.delete_ops_s
    );
    Ok(())
}

fn map(cfg: &Cfg) -> io::Result<()> {
    mk_out(&cfg.out)?;
    let wk = rec(cfg.wk, cfg.fast);
    let r = scan(&cfg.root, wk);
    wr_run(&cfg.out, &cfg.name, &r.sm, &r.map, &cfg.fm)?;
    println!(
        "scan done root={} workers={} ms={} dirs={} files={} depth={} fps={:.2} score={:.2} den={} err={}",
        cfg.root.display(),
        r.sm.wk,
        r.sm.ms,
        r.sm.dirs,
        r.sm.files,
        r.sm.deep,
        r.sm.fps,
        r.sm.score,
        r.sm.den,
        r.sm.err
    );
    Ok(())
}

fn main() {
    let mut cfg = arg();
    if matches!(cfg.md, Md::Menu) {
        cfg = menu(cfg);
    }
    if !cfg.root.exists() {
        eprintln!("root path not found: {}", cfg.root.display());
        std::process::exit(2);
    }

    let res = match cfg.md {
        Md::Menu | Md::Map => map(&cfg),
        Md::Bench => bench(&cfg),
        Md::Stress => stress(&cfg),
        Md::Disk => disk(&cfg),
    };

    if let Err(e) = res {
        eprintln!("run failed: {}", e);
        std::process::exit(1);
    }
}
