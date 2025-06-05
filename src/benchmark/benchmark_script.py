# src/benchmark/benchmark_script.py
import argparse
import importlib
import sys
from pathlib import Path
import cProfile, pstats, io
from memory_profiler import memory_usage
import pandas as pd


def benchmark_function(func, *args, top_n=10, **kwargs):
    profiler = cProfile.Profile()

    def wrapped():
        profiler.enable()
        res = func(*args, **kwargs)
        profiler.disable()
        return res

    peak_mem, result = memory_usage(
        (wrapped, (), {}),
        retval=True,
        max_usage=True,
        interval=0.01,
    )

    s = io.StringIO()
    pstats.Stats(profiler, stream=s).sort_stats("cumtime").print_stats(top_n)

    stats_obj = pstats.Stats(profiler)
    time_total   = stats_obj.total_tt
    prim_calls   = stats_obj.prim_calls
    percall_avg  = time_total / prim_calls if prim_calls else 0.0

    return {
        "time_total":  time_total,
        "prim_calls":  prim_calls,
        "percall_avg": percall_avg,
        "peak_memory": peak_mem,
        "cprofile_txt": s.getvalue()
    }, result


def main():
    parser = argparse.ArgumentParser(description="Isolated benchmark of a backend+function.")
    parser.add_argument("--backend", required=True,
        choices=["pandas", "polars", "duckdb", "ijson"],
        help="Backend to evaluate")
    parser.add_argument("--func", required=True,
        choices=["top_active_dates", "top_emojis", "top_mentioned_users"],
        help="Target function")
    parser.add_argument("--ndjson", required=True, type=str,
        help="Path to the NDJSON input file")
    parser.add_argument("--n", type=int, default=10,
        help="Argument n of the function (default 10)")
    parser.add_argument("--output", default="master_summary.csv",
        help="CSV summary cumulative (created or added rows)")
    parser.add_argument("--save-cprofile", action="store_true",
        help="If indicated, save/concatenate the cProfile text in profile_reports/cprofile_combined.txt")
    args = parser.parse_args()

    sys.path.append(str(Path(__file__).parent.parent / "backend"))
    backend_module = importlib.import_module(f"{args.backend}_backend")
    target_func    = getattr(backend_module, args.func)

    summary, _ = benchmark_function(
        target_func,
        Path(args.ndjson),
        n=args.n,
        top_n=10
    )

    root_dir = Path("profile_reports")
    root_dir.mkdir(exist_ok=True)

    csv_path = root_dir / args.output if not Path(args.output).is_absolute() else Path(args.output)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "backend": args.backend,
        "function": args.func,
        "time_total_sec":   summary["time_total"],
        "prim_calls":       summary["prim_calls"],
        "percall_avg_sec":  summary["percall_avg"],
        "peak_memory_MB":   summary["peak_memory"]
    }

    df_new = pd.DataFrame([row])
    if csv_path.exists():
        df_new.to_csv(csv_path, mode="a", header=False, index=False)
    else:
        df_new.to_csv(csv_path, index=False)
    print(f"[OK] Metrics added to {csv_path}")

    if args.save_cprofile:
        cp_file = root_dir / "cprofile_combined.txt"
        with cp_file.open("a", encoding="utf-8") as f:
            hdr = f"\n# ===== {args.backend} | {args.func} =====\n"
            f.write(hdr + summary["cprofile_txt"])
        print(f"[OK] cProfile added in {cp_file}")

    print("Summary:", row)

if __name__ == "__main__":
    main()
