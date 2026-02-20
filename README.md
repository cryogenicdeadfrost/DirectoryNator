# DirectoryNator

DirectoryNator is a Python CLI utility that maps directory/file hierarchies and can be used as both:

1. a **filesystem inventory scanner**
2. a **multithreading benchmark + throttling test tool**
3. an **automation utility for periodic IT security/health scans**

## What it does

- Scans a target root path and maps folders/files into timestamped report files.
- Supports BFS, DFS, Trie, and high-throughput multithread traversal.
- Auto-detects practical thread counts from CPU cores.
- Benchmarks multiple thread profiles and ranks them by runtime + files/sec.
- Produces small JSON result files for quick machine-readable reporting.
- Can run repeatedly in automation mode (e.g., every few minutes/hours/days via scheduler wrappers).

## Requirements

- Python 3.9+
- No third-party dependencies (standard library only)

## Quick start

```bash
python DirectoryNator_v1.py
```

Interactive menu options:

1. Multi-Thread Option (CPU-aware)
2. Algorithmic Options (Trie, BFS, DFS)
3. Multithread Benchmark Mode
4. Automation Mode (periodic runs)
5. Exit

## Non-interactive command modes

### Multithread mapping

```bash
python DirectoryNator_v1.py --mode multithread --root /path/to/scan --threads 16 --throttle-ms 0
```

### Benchmark mode

```bash
python DirectoryNator_v1.py --mode benchmark --root /path/to/scan --iterations 2 --throttle-ms 1
```

### Automation mode (for IT environment periodic checks)

```bash
python DirectoryNator_v1.py --mode automation --automation-mode multithread --root /path/to/scan --runs 4 --interval 300
```

Benchmark automation example:

```bash
python DirectoryNator_v1.py --mode automation --automation-mode benchmark --root /path/to/scan --runs 3 --interval 600 --iterations 2
```

## Output files

All outputs are written under:

```text
./directorynator/
```

Generated artifacts include:

- Full mapping text reports
  - `directorynator_multithread_<N>threads_<timestamp>.txt`
  - `directorynator_bfs_<timestamp>.txt`
  - `directorynator_dfs_<timestamp>.txt`
  - `directorynator_trie_<timestamp>.txt`
- Benchmark ranking text report
  - `directorynator_benchmark_<timestamp>.txt`
- Small JSON summary files (for automation and quick ingestion)
  - `directorynator_run_summary_<timestamp>.json`
  - `directorynator_benchmark_summary_<timestamp>.json`
  - `directorynator_automation_summary_<timestamp>.json`
  - `directorynator_*_latest.json`

## Notes

- Permission-restricted paths are skipped and counted.
- Large root scans can be expensive in I/O and runtime.
- Benchmark results depend on filesystem type, storage medium, and active system load.
- For production IT scheduling, run this through cron/systemd/Task Scheduler and keep output directory monitored/archived.
