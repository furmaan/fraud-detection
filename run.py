#!/usr/bin/env python3
"""
run.py
──────
One-command launcher. Runs train → pipeline → dashboard.

Usage:
    python run.py              # train + local pipeline
    python run.py --dashboard  # also opens streamlit
    python run.py --kafka      # use Kafka instead of Python sim
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def banner(msg: str):
    print(f"\n{'═'*60}\n  {msg}\n{'═'*60}")

def run(cmd: str, cwd=ROOT):
    print(f"[CMD] {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    return result.returncode

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-train",  action="store_true",
                        help="Skip training (use existing model)")
    parser.add_argument("--dashboard",   action="store_true",
                        help="Launch Streamlit dashboard")
    parser.add_argument("--kafka",       action="store_true",
                        help="Use Kafka pipeline instead of Python sim")
    parser.add_argument("--rate",        type=float, default=5.0)
    args = parser.parse_args()

    banner("🛡️  Real-Time Fraud Detection System")

    # ── Step 1: Train ──────────────────────────────────────────────────────
    if not args.skip_train:
        banner("Step 1/3 — Training LightGBM model")
        rc = run(f"{sys.executable} -m models.train")
        if rc != 0:
            print("[ERROR] Training failed. Check data/creditcard.csv exists.")
            sys.exit(1)
    else:
        banner("Step 1/3 — Skipping training (--skip-train)")

    # ── Step 2: Pipeline ────────────────────────────────────────────────────
    if args.kafka:
        banner("Step 2/3 — Kafka pipeline")
        print("Start these in separate terminals:")
        print(f"  Terminal 1: python -m streaming.kafka_producer --rate {args.rate}")
        print(f"  Terminal 2: python -m streaming.kafka_consumer")
        print(f"  Terminal 3: uvicorn api.main:app --reload --port 8000")
        if args.dashboard:
            print(f"  Terminal 4: streamlit run dashboard/app.py")
    else:
        banner("Step 2/3 — Running local Python pipeline")
        if args.dashboard:
            # Launch pipeline in background, dashboard in foreground
            import threading
            def run_pipeline():
                run(f"{sys.executable} -m streaming.pipeline --rate {args.rate}")
            t = threading.Thread(target=run_pipeline, daemon=True)
            t.start()
            time.sleep(2)   # let pipeline warm up

            banner("Step 3/3 — Launching Streamlit Dashboard")
            run("streamlit run dashboard/app.py")
        else:
            banner("Step 2/3 — Running pipeline (Ctrl+C to stop)")
            run(f"{sys.executable} -m streaming.pipeline --rate {args.rate}")

if __name__ == "__main__":
    main()
