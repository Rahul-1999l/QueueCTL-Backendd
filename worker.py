# queuectl/worker.py
import subprocess, time
from multiprocessing import Process
from .repo import get_ready_job, succeed, to_failed_with_retry

def _worker_loop(poll_interval: float = 0.5, base_backoff: int = 2):
    while True:
        job = get_ready_job()
        if not job:
            time.sleep(poll_interval)
            continue
        try:
            res = subprocess.run(job["command"], shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                succeed(job["id"])
            else:
                to_failed_with_retry(job, f"exit={res.returncode}\nSTDERR:\n{res.stderr}", base_backoff=base_backoff)
        except Exception as e:
            to_failed_with_retry(job, f"exception: {e!r}", base_backoff=base_backoff)

def start_workers(n: int, poll_interval: float = 0.5, base_backoff: int = 2):
    procs = [Process(target=_worker_loop, args=(poll_interval, base_backoff), daemon=False) for _ in range(n)]
    for p in procs: p.start()
    for p in procs: p.join()
