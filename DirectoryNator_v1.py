import argparse
import datetime
import json
import os
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait


def print_banner():
    banner = r"""



$$$$$$$\  $$\                                 $$\                                   $$\   $$\             $$\
$$  __$$\ \__|                                $$ |                                  $$$\  $$ |            $$ |
$$ |  $$ |$$\  $$$$$$\   $$$$$$\   $$$$$$$\ $$$$$$\    $$$$$$\   $$$$$$\  $$\   $$\ $$$$\ $$ | $$$$$$\  $$$$$$\    $$$$$$\   $$$$$$\
$$ |  $$ |$$ |$$  __$$\ $$  __$$\ $$  _____|\_$$  _|  $$  __$$\ $$  __$$\ $$ |  $$ |$$ $$\$$ | \____$$\ \_$$  _|  $$  __$$\ $$  __$$\
$$ |  $$ |$$ |$$ |  \__|$$$$$$$$ |$$ /        $$ |    $$ /  $$ |$$ |  \__|$$ |  $$ |$$ \$$$$ | $$$$$$$ |  $$ |    $$ /  $$ |$$ |  \__|
$$ |  $$ |$$ |$$ |      $$   ____|$$ |        $$ |$$\ $$ |  $$ |$$ |      $$ |  $$ |$$ |\$$$ |$$  __$$ |  $$ |$$\ $$ |  $$ |$$ |
$$$$$$$  |$$ |$$ |      \$$$$$$$\ \$$$$$$$\   \$$$$  |\$$$$$$  |$$ |      \$$$$$$$ |$$ | \$$ |\$$$$$$$ |  \$$$$  |\$$$$$$  |$$ |
\_______/ \__|\__|       \_______| \_______|   \____/  \______/ \__|       \____$$ |\__|  \__| \_______|   \____/  \______/ \__|
                                                                          $$\   $$ |
                                                                          \$$$$$$  |
                                                                           \______/



                                                         by Abhradeep Basak
    """
    print(banner)


def ensure_output_folder():
    output_folder = os.path.join(os.getcwd(), "directorynator")
    os.makedirs(output_folder, exist_ok=True)
    return output_folder


def traverse_directory(dirpath):
    """Returns (subfolders, files, permission_denied_count, other_error_count)."""
    folder_paths, file_paths = [], []
    permission_denied_count, other_error_count = 0, 0

    try:
        for entry in os.scandir(dirpath):
            try:
                if entry.is_dir(follow_symlinks=False):
                    folder_paths.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    file_paths.append(entry.path)
            except PermissionError:
                permission_denied_count += 1
            except OSError:
                other_error_count += 1
    except PermissionError:
        permission_denied_count += 1
    except OSError:
        other_error_count += 1

    return folder_paths, file_paths, permission_denied_count, other_error_count


def write_report(file_path, folder_files_map):
    try:
        with open(file_path, "w", encoding="latin-1", errors="replace") as report_file:
            for folder, files in folder_files_map.items():
                report_file.write(f"{folder}:\n")
                for file in files:
                    report_file.write(f"    {file}\n")
                report_file.write("\n")
    except Exception as error:
        print(f"Error writing report: {error}")


def write_small_results_file(output_folder, payload, label):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(output_folder, f"directorynator_{label}_summary_{timestamp}.json")
    with open(path, "w", encoding="utf-8") as result_file:
        json.dump(payload, result_file, indent=2)
    latest_path = os.path.join(output_folder, f"directorynator_{label}_latest.json")
    with open(latest_path, "w", encoding="utf-8") as latest_file:
        json.dump(payload, latest_file, indent=2)
    return path


def detect_recommended_threads(user_threads=None):
    if user_threads is not None:
        return max(1, int(user_threads))

    logical_cores = os.cpu_count() or 4
    if logical_cores <= 4:
        return logical_cores
    if logical_cores <= 12:
        return logical_cores + 2
    if logical_cores <= 32:
        return int(logical_cores * 1.5)
    return min(128, int(logical_cores * 1.25))


def multithread_scan(root_dir, thread_count, throttle_ms=0):
    """Threaded traversal with dynamic scheduling and optional throttling."""
    folder_files_map = {}
    folder_count, file_count = 0, 0
    permission_denied_count, other_error_count = 0, 0

    pending_dirs = deque([root_dir])
    active_futures = {}
    inflight_limit = max(thread_count * 4, 8)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        while pending_dirs or active_futures:
            while pending_dirs and len(active_futures) < inflight_limit:
                current_dir = pending_dirs.popleft()
                folder_files_map.setdefault(current_dir, [])
                active_futures[executor.submit(traverse_directory, current_dir)] = current_dir

                if throttle_ms > 0:
                    time.sleep(throttle_ms / 1000)

            if not active_futures:
                continue

            completed, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in completed:
                current_dir = active_futures.pop(future)
                try:
                    result_folders, result_files, denied, errors = future.result()
                    folder_files_map[current_dir].extend(result_files)
                    file_count += len(result_files)
                    permission_denied_count += denied
                    other_error_count += errors

                    for folder in result_folders:
                        if folder not in folder_files_map:
                            folder_files_map[folder] = []
                            pending_dirs.append(folder)
                            folder_count += 1
                except Exception as error:
                    other_error_count += 1
                    print(f"Error processing '{current_dir}': {error}")

    elapsed_time = time.time() - start_time
    stats = {
        "root": root_dir,
        "folders": folder_count,
        "files": file_count,
        "permissions_skipped": permission_denied_count,
        "other_errors": other_error_count,
        "elapsed": elapsed_time,
        "workers": thread_count,
        "throttle_ms": throttle_ms,
    }
    return folder_files_map, stats


def generate_directory_report_multithread(thread_count=None, root_dir=None, throttle_ms=0):
    root_dir = root_dir or os.path.abspath(os.sep)
    output_folder = ensure_output_folder()
    workers = detect_recommended_threads(thread_count)

    folder_files_map, stats = multithread_scan(root_dir, workers, throttle_ms=throttle_ms)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = os.path.join(output_folder, f"directorynator_multithread_{workers}threads_{timestamp}.txt")
    write_report(report_path, folder_files_map)

    summary_payload = {"mode": "multithread", "report_path": report_path, "stats": stats}
    summary_path = write_small_results_file(output_folder, summary_payload, "run")

    print("\nSummary:")
    print(f"Workers used: {stats['workers']}")
    print(f"Total number of folders: {stats['folders']}")
    print(f"Total number of files: {stats['files']}")
    print(f"Permission-denied entries skipped: {stats['permissions_skipped']}")
    print(f"Other IO errors: {stats['other_errors']}")
    print(f"Throttle per submit: {stats['throttle_ms']} ms")
    print(f"Time taken: {stats['elapsed']:.2f} seconds")
    print(f"Directory report saved to: {report_path} successfully!")
    print(f"Small results file saved to: {summary_path}")
    return stats, report_path, summary_path


def get_benchmark_thread_candidates():
    cores = os.cpu_count() or 4
    candidates = {1, max(2, cores // 2), cores, detect_recommended_threads(), min(128, cores * 2)}
    return sorted(candidates)


def benchmark_multithread(root_dir=None, iterations=1, throttle_ms=0):
    root_dir = root_dir or os.path.abspath(os.sep)
    output_folder = ensure_output_folder()
    candidates = get_benchmark_thread_candidates()
    results = []

    print(f"\nRunning benchmark on: {root_dir}")
    print(f"CPU logical cores detected: {os.cpu_count() or 'unknown'}")
    print(f"Worker candidates: {candidates}")

    for workers in candidates:
        run_times, run_stats = [], None
        for _ in range(iterations):
            _, run_stats = multithread_scan(root_dir, workers, throttle_ms=throttle_ms)
            run_times.append(run_stats["elapsed"])

        average_time = sum(run_times) / len(run_times)
        run_stats["elapsed"] = average_time
        run_stats["throughput_files_per_sec"] = round(run_stats["files"] / average_time, 2) if average_time > 0 else 0
        results.append(run_stats)
        print(
            f"Workers={workers:>3} | avg={average_time:.2f}s | "
            f"files/s={run_stats['throughput_files_per_sec']}"
        )

    results.sort(key=lambda item: item["elapsed"])

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    benchmark_path = os.path.join(output_folder, f"directorynator_benchmark_{timestamp}.txt")
    with open(benchmark_path, "w", encoding="utf-8") as benchmark_file:
        benchmark_file.write(f"Benchmark root: {root_dir}\n")
        benchmark_file.write(f"Logical CPU cores: {os.cpu_count() or 'unknown'}\n")
        benchmark_file.write(f"Iterations per worker count: {iterations}\n")
        benchmark_file.write(f"Throttle per submit: {throttle_ms} ms\n\n")
        benchmark_file.write("Ranked results (fastest first):\n")
        for index, item in enumerate(results, start=1):
            benchmark_file.write(
                f"{index}. workers={item['workers']} | avg_time={item['elapsed']:.2f}s "
                f"| files_per_sec={item['throughput_files_per_sec']} "
                f"| folders={item['folders']} | files={item['files']} "
                f"| permissions_skipped={item['permissions_skipped']} | errors={item['other_errors']}\n"
            )

    fastest = results[0] if results else None
    payload = {
        "mode": "benchmark",
        "root": root_dir,
        "iterations": iterations,
        "throttle_ms": throttle_ms,
        "fastest": fastest,
        "results": results,
        "benchmark_path": benchmark_path,
    }
    small_result_path = write_small_results_file(output_folder, payload, "benchmark")

    print("\nBenchmark ranking (fastest first):")
    for index, item in enumerate(results, start=1):
        print(
            f"{index}. workers={item['workers']} | avg={item['elapsed']:.2f}s | "
            f"files/s={item['throughput_files_per_sec']}"
        )
    if fastest:
        print(
            f"Best profile => workers={fastest['workers']} avg={fastest['elapsed']:.2f}s "
            f"files/s={fastest['throughput_files_per_sec']}"
        )

    print(f"Benchmark report saved to: {benchmark_path} successfully!")
    print(f"Small results file saved to: {small_result_path}")
    return results, benchmark_path, small_result_path


def run_automation_campaign(root_dir, runs, interval_seconds, mode="multithread", iterations=1, throttle_ms=0):
    output_folder = ensure_output_folder()
    automation_log = []

    print(
        f"\nAutomation start => mode={mode}, runs={runs}, interval={interval_seconds}s, "
        f"root={root_dir}, throttle={throttle_ms}ms"
    )

    for run_number in range(1, runs + 1):
        print(f"\n[Automation] Run {run_number}/{runs}")
        started = datetime.datetime.now().isoformat()

        if mode == "benchmark":
            results, report_path, summary_path = benchmark_multithread(
                root_dir=root_dir,
                iterations=iterations,
                throttle_ms=throttle_ms,
            )
            best_workers = results[0]["workers"] if results else None
            elapsed = results[0]["elapsed"] if results else None
            automation_log.append(
                {
                    "run": run_number,
                    "mode": mode,
                    "started": started,
                    "best_workers": best_workers,
                    "best_avg_time": elapsed,
                    "report_path": report_path,
                    "summary_path": summary_path,
                }
            )
        else:
            stats, report_path, summary_path = generate_directory_report_multithread(
                root_dir=root_dir,
                thread_count=None,
                throttle_ms=throttle_ms,
            )
            automation_log.append(
                {
                    "run": run_number,
                    "mode": mode,
                    "started": started,
                    "workers": stats["workers"],
                    "elapsed": stats["elapsed"],
                    "files": stats["files"],
                    "folders": stats["folders"],
                    "report_path": report_path,
                    "summary_path": summary_path,
                }
            )

        if run_number < runs:
            time.sleep(interval_seconds)

    payload = {
        "mode": "automation",
        "root": root_dir,
        "runs": runs,
        "interval_seconds": interval_seconds,
        "throttle_ms": throttle_ms,
        "history": automation_log,
    }
    summary_path = write_small_results_file(output_folder, payload, "automation")
    print(f"\nAutomation complete. Summary written to: {summary_path}")


def bfs_traverse_directory(root_dir):
    output_folder = ensure_output_folder()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(output_folder, f"directorynator_bfs_{timestamp}.txt")

    folder_files_map = {}
    queue = deque([root_dir])
    folder_count, file_count = 0, 0
    start_time = time.time()

    try:
        while queue:
            current_path = queue.popleft()
            try:
                folder_files_map[current_path] = []
                for entry in os.scandir(current_path):
                    if entry.is_dir(follow_symlinks=False):
                        queue.append(entry.path)
                        folder_files_map[entry.path] = []
                        folder_count += 1
                    elif entry.is_file(follow_symlinks=False):
                        folder_files_map[current_path].append(entry.path)
                        file_count += 1
            except PermissionError:
                pass

        elapsed_time = time.time() - start_time
        write_report(file_path, folder_files_map)
        print(f"\nSummary:\nTotal number of folders: {folder_count}")
        print(f"Total number of files: {file_count}")
        print(f"Time taken: {elapsed_time:.2f} seconds")
        print(f"BFS directory report saved to: {file_path} successfully!")
    except Exception as error:
        print(f"Error creating BFS report: {error}")


def dfs_traverse_directory(root_dir):
    output_folder = ensure_output_folder()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(output_folder, f"directorynator_dfs_{timestamp}.txt")

    folder_files_map = {}
    stack = [root_dir]
    folder_count, file_count = 0, 0
    start_time = time.time()

    try:
        while stack:
            current_path = stack.pop()
            try:
                folder_files_map[current_path] = []
                for entry in os.scandir(current_path):
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        folder_files_map[entry.path] = []
                        folder_count += 1
                    elif entry.is_file(follow_symlinks=False):
                        folder_files_map[current_path].append(entry.path)
                        file_count += 1
            except PermissionError:
                pass

        elapsed_time = time.time() - start_time
        write_report(file_path, folder_files_map)
        print(f"\nSummary:\nTotal number of folders: {folder_count}")
        print(f"Total number of files: {file_count}")
        print(f"Time taken: {elapsed_time:.2f} seconds")
        print(f"DFS directory report saved to: {file_path} successfully!")
    except Exception as error:
        print(f"Error creating DFS report: {error}")


def trie_traverse_directory(root_dir):
    output_folder = ensure_output_folder()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(output_folder, f"directorynator_trie_{timestamp}.txt")

    folder_files_map = {}
    trie = {}
    stack = [(root_dir, trie)]
    folder_count, file_count = 0, 0
    start_time = time.time()

    try:
        while stack:
            current_path, current_trie = stack.pop()
            try:
                current_trie["folders"] = {}
                current_trie["files"] = []
                for entry in os.scandir(current_path):
                    if entry.is_dir(follow_symlinks=False):
                        folder_name = os.path.basename(entry.path)
                        current_trie["folders"][folder_name] = {}
                        stack.append((entry.path, current_trie["folders"][folder_name]))
                        folder_count += 1
                    elif entry.is_file(follow_symlinks=False):
                        current_trie["files"].append(entry.path)
                        file_count += 1
            except PermissionError:
                pass

        def flatten_trie(current_trie, path):
            folder_files_map[path] = current_trie["files"]
            for folder, subtrie in current_trie["folders"].items():
                flatten_trie(subtrie, os.path.join(path, folder))

        flatten_trie(trie, root_dir)
        elapsed_time = time.time() - start_time
        write_report(file_path, folder_files_map)
        print(f"\nSummary:\nTotal number of folders: {folder_count}")
        print(f"Total number of files: {file_count}")
        print(f"Time taken: {elapsed_time:.2f} seconds")
        print(f"Trie directory report saved to: {file_path} successfully!")
    except Exception as error:
        print(f"Error creating Trie report: {error}")


def get_root_path_input(default_root):
    raw_root = input(f"Enter root path to scan [default: {default_root}]: ").strip()
    if not raw_root:
        return default_root

    chosen_root = os.path.abspath(raw_root)
    if not os.path.exists(chosen_root):
        print("Path does not exist. Falling back to default root.")
        return default_root
    return chosen_root


def get_positive_integer_input(prompt, default_value):
    raw_value = input(f"{prompt} [default: {default_value}]: ").strip()
    if not raw_value:
        return default_value
    try:
        parsed = int(raw_value)
        if parsed < 1:
            raise ValueError
        return parsed
    except ValueError:
        print("Invalid integer provided. Falling back to default.")
        return default_value


def cli_interface():
    print_banner()
    while True:
        print("\nWelcome to DirectoryNator!")
        print("Select an option:")
        print("1) Multi-Thread Option (Auto CPU-aware)")
        print("2) Algorithmic Options (Trie, BFS, DFS)")
        print("3) Multithread Benchmark Mode")
        print("4) Automation Mode (periodic runs for IT environments)")
        print("5) Exit")
        choice = input("Enter your choice: ").strip()

        default_root = os.path.abspath(os.sep)

        if choice == "1":
            root_dir = get_root_path_input(default_root)
            auto_workers = detect_recommended_threads()
            thread_count = get_positive_integer_input("Enter the number of threads to use", auto_workers)
            throttle_ms = get_positive_integer_input("Throttle in ms between directory submissions", 0)
            generate_directory_report_multithread(thread_count=thread_count, root_dir=root_dir, throttle_ms=throttle_ms)
        elif choice == "2":
            root_dir = get_root_path_input(default_root)
            print("Select an Algorithm:")
            print("1) Trie Traversal")
            print("2) BFS Traversal")
            print("3) DFS Traversal")
            algo_choice = input("Enter your choice: ").strip()
            if algo_choice == "1":
                trie_traverse_directory(root_dir)
            elif algo_choice == "2":
                bfs_traverse_directory(root_dir)
            elif algo_choice == "3":
                dfs_traverse_directory(root_dir)
            else:
                print("Invalid choice, please try again.")
        elif choice == "3":
            root_dir = get_root_path_input(default_root)
            iterations = get_positive_integer_input("Enter iterations per worker candidate", 1)
            throttle_ms = get_positive_integer_input("Throttle in ms between directory submissions", 0)
            benchmark_multithread(root_dir=root_dir, iterations=iterations, throttle_ms=throttle_ms)
        elif choice == "4":
            root_dir = get_root_path_input(default_root)
            mode = input("Automation mode (multithread/benchmark) [default: multithread]: ").strip() or "multithread"
            if mode not in {"multithread", "benchmark"}:
                mode = "multithread"
            runs = get_positive_integer_input("Number of runs", 3)
            interval_seconds = get_positive_integer_input("Interval seconds between runs", 60)
            iterations = get_positive_integer_input("Benchmark iterations (if benchmark mode)", 1)
            throttle_ms = get_positive_integer_input("Throttle in ms between directory submissions", 0)
            run_automation_campaign(
                root_dir=root_dir,
                runs=runs,
                interval_seconds=interval_seconds,
                mode=mode,
                iterations=iterations,
                throttle_ms=throttle_ms,
            )
        elif choice == "5":
            print("Exiting DirectoryNator. Goodbye!")
            break
        else:
            print("Invalid choice, please try again.")


def parse_args():
    parser = argparse.ArgumentParser(description="DirectoryNator filesystem mapper and benchmark tool")
    parser.add_argument("--mode", choices=["cli", "multithread", "benchmark", "automation"], default="cli")
    parser.add_argument("--root", default=os.path.abspath(os.sep), help="Root path to scan")
    parser.add_argument("--threads", type=int, default=None, help="Worker count for multithread mode")
    parser.add_argument("--iterations", type=int, default=1, help="Iterations for benchmark mode")
    parser.add_argument("--runs", type=int, default=3, help="Automation run count")
    parser.add_argument("--interval", type=int, default=60, help="Automation interval in seconds")
    parser.add_argument("--automation-mode", choices=["multithread", "benchmark"], default="multithread")
    parser.add_argument("--throttle-ms", type=int, default=0, help="Pause between task submissions in ms")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.mode == "cli":
        cli_interface()
    elif args.mode == "multithread":
        generate_directory_report_multithread(
            thread_count=args.threads,
            root_dir=os.path.abspath(args.root),
            throttle_ms=max(0, args.throttle_ms),
        )
    elif args.mode == "benchmark":
        benchmark_multithread(
            root_dir=os.path.abspath(args.root),
            iterations=max(1, args.iterations),
            throttle_ms=max(0, args.throttle_ms),
        )
    elif args.mode == "automation":
        run_automation_campaign(
            root_dir=os.path.abspath(args.root),
            runs=max(1, args.runs),
            interval_seconds=max(1, args.interval),
            mode=args.automation_mode,
            iterations=max(1, args.iterations),
            throttle_ms=max(0, args.throttle_ms),
        )


if __name__ == "__main__":
    main()
