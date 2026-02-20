# DirectoryNator

DirectoryNator now ships with two full implementations:

- `DirectoryNator_v1.py` (Python) with interactive menu + CLI modes
- `rust_nator/` (Rust) for low-level high-speed scanning, stress presets, binary output, and JSON data for GUI analytics

## What is new

- simple menu driver for both Python and Rust
- hardware auto-detection in both (OS, architecture, cores, runtime/system info)
- improved benchmark scoring with depth-aware context
- disk performance mode for SSD/HDD context testing
- Rust JSON outputs designed for table+graph GUI inspection
- GUI viewer for Rust JSON benchmark/stress/map outputs

## Python version

### Menu mode

```bash
python DirectoryNator_v1.py
```

Menu options:

1. map
2. bench
3. disk
4. quit

### CLI modes

```bash
python DirectoryNator_v1.py --mode map --root . --fast
python DirectoryNator_v1.py --mode bench --root . --runs 2 --fast
python DirectoryNator_v1.py --mode disk
```

Python outputs go to:

```text
./directorynator/
```

## Rust version

Path: `rust_nator/`

### Build

```bash
cd rust_nator
cargo build --release
```

### Menu mode (interactive)

```bash
cargo run --release
```

### CLI modes

```bash
cargo run --release -- --mode map --root . --fmt both --name quick --fast
cargo run --release -- --mode bench --root . --preset balanced --runs 2 --fast
cargo run --release -- --mode stress --root . --preset hard --fast
cargo run --release -- --mode disk --out rust_nator/out
```

### Key Rust args

- `--mode map|bench|stress|disk`
- `--root <path>`
- `--out <path>`
- `--workers <n>`
- `--fast`
- `--fmt text|bin|both`
- `--preset light|balanced|hard|extreme`
- `--runs <n>`
- `--name <tag>`

Rust outputs go to:

```text
rust_nator/out/
```

Artifacts:

- `dnrs_<tag>_<ts>.txt`
- `dnrs_<tag>_<ts>.bin`
- `dnrs_<tag>_<ts>.json`
- `dnrs_bench_<ts>.txt`
- `dnrs_bench_<ts>.json`
- `dnrs_stress_<ts>.json`
- `dnrs_disk_<ts>.json`

## Rust GUI analytics

Open graph/table viewer:

```bash
python rust_nator/gui_viewer.py
```

What it shows:

- worker table (`wk`, `ms`, `files`, `fps`, `deep`, `score`, `den`, `err`)
- bar graph (`ms` and `score`)
- detected hardware + root context from JSON

This allows visual comparison of throttling/fast-mode/preset runs and better error mapping analysis.

## Notes

- fast mode is aggressive and useful for stress scenarios
- benchmark score is depth-aware and penalizes permission/errors
- disk mode adds storage context to benchmark outcomes
- all scans skip inaccessible paths and keep counting errors
