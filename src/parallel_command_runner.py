#!/usr/bin/env python3
"""
Parallel Command Runner

This script runs a command multiple times in parallel with configurable parameters.
It uses subprocess.Popen to execute commands and manages a pool of processes.
Each run can optionally get its own TMPDIR so tools like Homer won't overwrite each other’s temp‐files.
A background watcher thread prints every new file created in each TMPDIR.
"""

import os
import sys
import time
import subprocess
import tempfile
import shutil
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional

# Set to True to use a separate TMPDIR per run, False to use the default TMPDIR
USE_SEPARATE_TMPDIR = False

class ParallelCommandRunner:
    """
    Runs a command multiple times in parallel with configurable parameters.
    Each run can optionally get its own TMPDIR to avoid temp‐file collisions.
    Optionally wraps with strace if available, but always watches TMPDIR contents if enabled.
    """
    def __init__(
        self,
        command: str,
        num_runs: int,
        max_parallel: int = 5,
        delay: int = 0,
        output_dir: str = "./logs",
        log_prefix: str = "command_run",
        error_patterns: Optional[List[str]] = None,
        use_tmpdir: bool = USE_SEPARATE_TMPDIR,
    ):
        self.command = command
        self.num_runs = num_runs
        self.max_parallel = min(max_parallel, num_runs)
        self.output_dir = output_dir
        self.log_prefix = log_prefix
        self.error_patterns = error_patterns or ["!!!"]
        self.use_tmpdir = use_tmpdir

        # Delay between run starts (seconds)
        self.delay = delay / 1000.0
        # Strace disabled
        # self.strace_path = shutil.which("strace")
        self.strace_path = None
        # if not self.strace_path:
        #     print("[WARN] strace not found on PATH; TMPDIR watcher will show file activity", file=sys.stderr, flush=True)

        os.makedirs(output_dir, exist_ok=True)

        # Tracking structures
        self.processes:      Dict[int, subprocess.Popen] = {}
        self.log_files:      Dict[int, str]            = {}
        self.strace_logs:    Dict[int, str]            = {}
        self.tmp_dirs:       Dict[int, str]            = {}
        self.results:        Dict[int, Dict[str, Any]] = {}
        self.error_messages: Dict[int, List[str]]      = {}

        for i in range(self.num_runs):
            self.results[i] = {
                "run_id":      i,
                "status":      "PENDING",
                "exit_code":   None,
                "log_file":    None,
                "strace_log":  None,
                "start_time":  None,
                "end_time":    None,
                "error_detected": False
            }
            self.error_messages[i] = []

    def run(self) -> List[Dict[str, Any]]:
        print(f"Starting {self.num_runs} runs (max {self.max_parallel} parallel)", flush=True)
        print(f"Command: {self.command}\n", flush=True)

        active, next_id = 0, 0
        while next_id < self.num_runs or active > 0:
            # launch new jobs up to max_parallel
            while active < self.max_parallel and next_id < self.num_runs:
                print(f"[INFO] Scheduling run {next_id}", flush=True)
                self._start_process(next_id)
                next_id += 1
                active += 1
                # delay before scheduling next run
                if self.delay > 0:
                    time.sleep(self.delay)

            # reap any finished jobs
            finished = self._check_completed_processes()
            active  -= finished

            if active > 0:
                time.sleep(0.1)

        return [self.results[i] for i in range(self.num_runs)]

    def _start_process(self, run_id: int):
        print(f"[INFO] Starting run {run_id}", flush=True)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.output_dir, f"{self.log_prefix}_{run_id}_{ts}.log")
        str_log  = os.path.join(self.output_dir, f"{self.log_prefix}_{run_id}_{ts}.strace")

        # create a separate tmpdir only if enabled
        if self.use_tmpdir:
            tmpdir = tempfile.mkdtemp(prefix="gimme_tmp_", dir="/polio/oded/")
            self.tmp_dirs[run_id] = tmpdir
        else:
            tmpdir = None

        self.log_files[run_id]   = log_file
        self.strace_logs[run_id] = str_log

        self.results[run_id].update({
            "log_file":   log_file,
            "strace_log": str_log if self.strace_path else None,
            "start_time": datetime.now(),
            "status":     "RUNNING"
        })

        log = open(log_file, "w", buffering=1)
        log.write(f"Command: {self.command}\n")
        log.write(f"Run ID: {run_id}\n")
        log.write(f"TMPDIR: {tmpdir or 'default'}\n")
        if self.strace_path:
            log.write(f"Strace log: {str_log}\n")
        log.write(f"Started at: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        log.write("="*80 + "\n\n")

        env = os.environ.copy()
        if tmpdir:
            env["TMPDIR"] = tmpdir

        # create run-specific output directory under the gimme_injected base
        base_out = '/polio/oded/gimme_injected'
        run_out = os.path.join(base_out, f"run{run_id}")
        os.makedirs(run_out, exist_ok=True)
        # adjust command to point to this run's output dir
        local_cmd = self.command.replace(base_out, run_out, 1)

        # build the invocation (strace disabled)
        cmd = ["bash", "-lc", local_cmd]

        p = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            env=env
        )
        self.processes[run_id] = p

        # launch the TMPDIR watcher thread only if using separate tmpdir
        if tmpdir:
            watcher = threading.Thread(
                target=self._watch_tmpdir,
                args=(run_id, tmpdir, p),
                daemon=True
            )
            watcher.start()

    def _watch_tmpdir(self, run_id: int, tmpdir: str, process: subprocess.Popen):
        """
        Poll tmpdir every 0.5s, print any new files as they appear,
        and on exit print final directory contents.
        """
        seen = set(os.listdir(tmpdir))
        print(f"[DEBUG run {run_id}] TMPDIR = {tmpdir!r}", flush=True)
        print(f"[DEBUG run {run_id}] initial files: {sorted(seen)}", flush=True)

        # until the subprocess exits
        while process.poll() is None:
            now = set(os.listdir(tmpdir))
            new = now - seen
            for fn in sorted(new):
                print(f"[DEBUG run {run_id}] created file → {os.path.join(tmpdir, fn)}", flush=True)
            seen = now
            time.sleep(0.5)

        final = set(os.listdir(tmpdir))
        print(f"[DEBUG run {run_id}] final files before cleanup: {sorted(final)}", flush=True)

    def _check_completed_processes(self) -> int:
        done = 0
        for rid, p in list(self.processes.items()):
            if p.poll() is None:
                continue

            exit_code = p.returncode
            log_path  = self.log_files[rid]
            str_path  = self.strace_logs.get(rid)
            tmpdir    = self.tmp_dirs.get(rid)

            # scan for error patterns
            err_detected = False
            errs: List[str] = []
            if os.path.exists(log_path):
                text = open(log_path).read()
                for pat in self.error_patterns:
                    if pat in text:
                        err_detected = True
                        errs += [ln for ln in text.splitlines() if pat in ln]

            status = "COMPLETED" if exit_code==0 and not err_detected else "FAILED"
            self.results[rid].update({
                "exit_code":     exit_code,
                "end_time":      datetime.now(),
                "error_detected": err_detected,
                "status":        status
            })
            if errs:
                self.error_messages[rid] = errs

            # footer in log
            with open(log_path, "a") as lf:
                lf.write("\n" + "="*80 + "\n")
                lf.write(f"Finished at: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
                lf.write(f"Exit code: {exit_code}\n")
                if err_detected:
                    lf.write("Error patterns detected:\n")
                    for e in errs:
                        lf.write("  " + e + "\n")

            # strace summary if available
            print(f"[DEBUG run {rid}] TMPDIR = {tmpdir or 'default'}", flush=True)
            if str_path and os.path.exists(str_path):
                print(f"[DEBUG run {rid}] .group activity from {str_path}:", flush=True)
                for ln in open(str_path):
                    if ".group" in ln:
                        print("   " + ln.rstrip(), flush=True)
            else:
                print(f"[DEBUG run {rid}] No strace log available", flush=True)

            # cleanup TMPDIR if it was created
            if tmpdir and os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
                del self.tmp_dirs[rid]

            del self.processes[rid]
            done += 1

            extra = ""
            if err_detected:
                extra = " (error pattern detected)"
            elif exit_code != 0:
                extra = f" (exit code: {exit_code})"
            print(f"[INFO] Finished run {rid} with status {status}{extra}", flush=True)

        return done

    def print_error_summary(self) -> None:
        failed = [rid for rid,r in self.results.items() if r["status"].startswith("FAILED")]
        if not failed:
            print("\nNo errors detected in any runs.", flush=True)
            return
        print("\n=== ERROR SUMMARY ===", flush=True)
        print(f"Total failed runs: {len(failed)}", flush=True)
        for rid in failed:
            info = self.results[rid]
            print(f"\nRun {rid}:", flush=True)
            print(f"  Status:   {info['status']}", flush=True)
            print(f"  Exit code:{info['exit_code']}", flush=True)
            print(f"  Log file: {info['log_file']}", flush=True)
            if self.error_messages[rid]:
                print("  Error messages:", flush=True)
                for msg in self.error_messages[rid]:
                    print(f"    {msg}", flush=True)

# ===== CONFIGURATION =====
# COMMAND = (
#     'homer2 denovo '
#     '-i "/polio/oded/MotiFabEnv/MotiFab/datasets/run_test_40_5pct_run_1_1.fasta" '
#     '-b "/polio/oded/MotiFabEnv/MotiFab/datasets/run_background_40_5pct_run_1_1.fasta" '
#     '-len 6,8,10,12,14 -S 10 -strand both'
# )
COMMAND = (
    'bash -lc "source /powerapps/share/miniconda3-4.7.12/etc/profile.d/conda.sh && '
    'conda activate /polio/oded/tools/conda_envs/gimmemotifs && '
    'gimme motifs '
    '/polio/oded/MotiFabEnv/MotiFab/datasets_pwm/run_test_500_50pct_run_29_1.fasta '
    '/polio/oded/gimme_injected '
    '--denovo '
    '-g /polio/oded/data/project1/genome/schMedS3_h1.fa '
    '-b /polio/oded/MotiFabEnv/MotiFab/datasets_pwm/run_background_500_50pct_run_29_1.fasta '
    '--keepintermediate '
    '-N 10"'
)
# COMMAND = (
#     'sleep 1; echo "Run completed successfully"'
# )
NUM_RUNS     = 5
MAX_PARALLEL = 5
DELAY        = 1500   # delay in milliseconds between run starts
OUTPUT_DIR   = "./logs"
LOG_PREFIX   = "command_run"
ERROR_PATTERNS = [
    "!!!", "Error:", "ERROR:", "Failed:", "FAILED:",
    "Exception:", "EXCEPTION:"
]

if __name__ == "__main__":
    print(f"[DEBUG] cwd:      {os.getcwd()!r}", file=sys.stderr, flush=True)
    print(f"[DEBUG] logs dir: {os.path.abspath(OUTPUT_DIR)!r}", file=sys.stderr, flush=True)

    runner = ParallelCommandRunner(
        command=COMMAND,
        num_runs=NUM_RUNS,
        max_parallel=MAX_PARALLEL,
        delay=DELAY,
        output_dir=OUTPUT_DIR,
        log_prefix=LOG_PREFIX,
        error_patterns=ERROR_PATTERNS,
        use_tmpdir=USE_SEPARATE_TMPDIR,
    )
    results = runner.run()

    print("\n[DEBUG] Final contents of log dir:", flush=True)
    for fn in sorted(os.listdir(OUTPUT_DIR)):
        print("  ", fn, flush=True)

    print("\nSummary:", flush=True)
    print(f" Total runs:             {len(results)}", flush=True)
    print(f" Completed successfully: {sum(1 for r in results if r['status']=='COMPLETED')}", flush=True)
    print(f" Failed:                 {sum(1 for r in results if r['status']=='FAILED')}", flush=True)

    runner.print_error_summary()
    if any(r['status']=="FAILED" for r in results):
        sys.exit(1)
