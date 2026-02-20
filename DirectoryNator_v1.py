import argparse
import datetime
import json
import os
import platform
import tempfile
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait


def out_dir():
    p = os.path.join(os.getcwd(), "directorynator")
    os.makedirs(p, exist_ok=True)
    return p


def ts():
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def hw_info():
    return {
        "os": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cores": os.cpu_count() or 1,
    }


def auto_threads(user=None, fast=False):
    if user:
        return max(1, int(user))
    c = os.cpu_count() or 4
    if fast:
        return min(256, max(2, c * 2))
    if c <= 4:
        return c
    if c <= 16:
        return c + 2
    return min(256, int(c * 1.4))


def walk_one(path):
    ds, fs = [], []
    den = err = 0
    try:
        for e in os.scandir(path):
            try:
                if e.is_dir(follow_symlinks=False):
                    ds.append(e.path)
                elif e.is_file(follow_symlinks=False):
                    fs.append(e.path)
            except PermissionError:
                den += 1
            except OSError:
                err += 1
    except PermissionError:
        den += 1
    except OSError:
        err += 1
    return ds, fs, den, err


def scan_mt(root, threads):
    q = deque([(root, 0)])
    mapping = {root: []}
    files = folders = den = err = 0
    deep = 0
    st = time.time()
    fut = {}

    with ThreadPoolExecutor(max_workers=threads) as ex:
        while q or fut:
            while q and len(fut) < max(8, threads * 4):
                p, d = q.popleft()
                mapping.setdefault(p, [])
                fut[ex.submit(walk_one, p)] = (p, d)

            if not fut:
                continue

            done, _ = wait(fut, return_when=FIRST_COMPLETED)
            for f in done:
                p, d = fut.pop(f)
                deep = max(deep, d)
                try:
                    ds, fs, dn, er = f.result()
                    mapping[p].extend(fs)
                    files += len(fs)
                    den += dn
                    err += er
                    for dd in ds:
                        if dd not in mapping:
                            mapping[dd] = []
                            q.append((dd, d + 1))
                            folders += 1
                except Exception:
                    err += 1

    ms = int((time.time() - st) * 1000)
    fps = files / (ms / 1000) if ms else 0
    score = max(0, fps * max(1, deep * 0.2) - (den + err) * 0.5)
    return mapping, {
        "root": root,
        "workers": threads,
        "ms": ms,
        "folders": folders,
        "files": files,
        "depth": deep,
        "fps": round(fps, 2),
        "score": round(score, 2),
        "permission_denied": den,
        "errors": err,
    }


def write_map_text(path, mapping):
    with open(path, "w", encoding="latin-1", errors="replace") as f:
        for k, v in mapping.items():
            f.write(f"{k}:\n")
            for x in v:
                f.write(f"    {x}\n")
            f.write("\n")


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run_map(root, threads=None, fast=False):
    od = out_dir()
    wk = auto_threads(threads, fast=fast)
    mapping, stats = scan_mt(root, wk)
    txt = os.path.join(od, f"directorynator_py_map_{ts()}.txt")
    js = os.path.join(od, f"directorynator_py_map_{ts()}.json")
    write_map_text(txt, mapping)
    write_json(js, {"mode": "map", "hw": hw_info(), "stats": stats, "report": txt})
    print("map done", stats)
    print("report:", txt)
    print("json:", js)


def bench_set(fast=False):
    c = os.cpu_count() or 4
    base = min(256, c * 2) if fast else c
    return sorted({1, max(2, c // 2), c, base})


def run_bench(root, runs=1, fast=False):
    od = out_dir()
    rec = []
    for w in bench_set(fast=fast):
        rows = []
        for _ in range(max(1, runs)):
            _, s = scan_mt(root, w)
            rows.append(s)
        ms = sum(x["ms"] for x in rows) // len(rows)
        fps = round(sum(x["fps"] for x in rows) / len(rows), 2)
        score = round(sum(x["score"] for x in rows) / len(rows), 2)
        one = rows[0]
        one["ms"] = ms
        one["fps"] = fps
        one["score"] = score
        rec.append(one)
    rec.sort(key=lambda x: x["ms"])

    txt = os.path.join(od, f"directorynator_py_bench_{ts()}.txt")
    with open(txt, "w", encoding="utf-8") as f:
        f.write(f"root={root}\n")
        f.write(f"runs={runs}\n")
        for i, r in enumerate(rec, 1):
            f.write(
                f"{i}. workers={r['workers']} avg_ms={r['ms']} files={r['files']} fps={r['fps']} depth={r['depth']} score={r['score']} den={r['permission_denied']} err={r['errors']}\n"
            )

    js = os.path.join(od, f"directorynator_py_bench_{ts()}.json")
    write_json(js, {"mode": "bench", "hw": hw_info(), "root": root, "results": rec})
    print("benchmark ranking:")
    for i, r in enumerate(rec, 1):
        print(i, r["workers"], "ms", r["ms"], "fps", r["fps"], "score", r["score"])
    print("report:", txt)
    print("json:", js)


def run_disk(path=None):
    od = out_dir()
    base = path or od
    tdir = tempfile.mkdtemp(prefix="dn_py_disk_", dir=base)
    fpath = os.path.join(tdir, "blob.bin")
    blk = b"a" * (1024 * 1024)
    mb = 64

    st = time.time()
    with open(fpath, "wb") as f:
        for _ in range(mb):
            f.write(blk)
    wms = max(1, int((time.time() - st) * 1000))

    st = time.time()
    with open(fpath, "rb") as f:
        while f.read(1024 * 1024):
            pass
    rms = max(1, int((time.time() - st) * 1000))

    cnt = 300
    st = time.time()
    for i in range(cnt):
        with open(os.path.join(tdir, f"x{i}.tmp"), "wb") as f:
            f.write(b"x" * 1024)
    cms = max(1, int((time.time() - st) * 1000))

    st = time.time()
    for i in range(cnt):
        os.remove(os.path.join(tdir, f"x{i}.tmp"))
    dms = max(1, int((time.time() - st) * 1000))

    os.remove(fpath)
    os.rmdir(tdir)

    payload = {
        "mode": "disk",
        "path": base,
        "write_mb_s": round(mb / (wms / 1000), 2),
        "read_mb_s": round(mb / (rms / 1000), 2),
        "create_ops_s": round(cnt / (cms / 1000), 2),
        "delete_ops_s": round(cnt / (dms / 1000), 2),
    }
    js = os.path.join(od, f"directorynator_py_disk_{ts()}.json")
    write_json(js, payload)
    print("disk benchmark:", payload)
    print("json:", js)


def menu_driver():
    h = hw_info()
    print("DirectoryNator Python")
    print("hardware:", h)
    while True:
        print("\n1) map 2) bench 3) disk 4) quit")
        c = input("choice: ").strip()
        if c == "1":
            root = input("root [.]: ").strip() or "."
            fast = (input("fast mode y/n [n]: ").strip().lower() == "y")
            run_map(os.path.abspath(root), fast=fast)
        elif c == "2":
            root = input("root [.]: ").strip() or "."
            runs = int(input("runs [1]: ").strip() or "1")
            fast = (input("fast mode y/n [n]: ").strip().lower() == "y")
            run_bench(os.path.abspath(root), runs=runs, fast=fast)
        elif c == "3":
            run_disk()
        elif c == "4":
            break


def parser():
    p = argparse.ArgumentParser(description="DirectoryNator Python mapper/bench/disk CLI")
    p.add_argument("--mode", choices=["menu", "map", "bench", "disk"], default="menu")
    p.add_argument("--root", default=os.path.abspath(os.sep))
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--fast", action="store_true")
    return p


def main():
    a = parser().parse_args()
    if a.mode == "menu":
        menu_driver()
    elif a.mode == "map":
        run_map(os.path.abspath(a.root), threads=a.threads, fast=a.fast)
    elif a.mode == "bench":
        run_bench(os.path.abspath(a.root), runs=max(1, a.runs), fast=a.fast)
    elif a.mode == "disk":
        run_disk()


if __name__ == "__main__":
    main()
