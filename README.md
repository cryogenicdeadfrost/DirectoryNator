# DirectoryNator

DirectoryNator now includes two implementations:

- `DirectoryNator_v1.py` for the original Python CLI flow
- `rust_nator/` for a faster, low-level Rust binary with multi-OS support

## Rust implementation

Path: `rust_nator/`

### Features

- multithreaded directory mapping
- benchmark mode with worker comparison
- stress presets (`light`, `balanced`, `hard`, `extreme`)
- fast mode (`--fast`) for more aggressive threading
- binary encoded output (`.bin`) plus text output (`.txt`)
- same mapping logic: folder -> file list with summary stats

### Build

```bash
cd rust_nator
cargo build --release
```

### Run mapping

```bash
cargo run --release -- --mode map --root . --fmt both --name quick
```

### Run benchmark

```bash
cargo run --release -- --mode bench --root . --preset balanced --runs 2 --fast
```

### Run stress test

```bash
cargo run --release -- --mode stress --root . --preset hard --fast
```

### Key args

- `--mode map|bench|stress`
- `--root <path>`
- `--out <path>`
- `--workers <n>`
- `--fast`
- `--fmt text|bin|both`
- `--preset light|balanced|hard|extreme`
- `--runs <n>`
- `--name <tag>`

### Output

Default output folder:

```text
rust_nator/out/
```

Artifacts include:

- `dnrs_<tag>_<ts>.txt`
- `dnrs_<tag>_<ts>.bin`
- `dnrs_bench_<ts>.txt`
- `dnrs_stress_<ts>.txt`

## Notes

- Rust version is optimized for fast repeated IT scans and performance profiling.
- Binary encoding stores mapped data and summary counters in a compact custom format.
- Runs on Linux, macOS, and Windows with standard Rust toolchain.
