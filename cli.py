import json
import click
from .db import init
from .repo import enqueue, list_jobs, list_dlq, requeue_from_dlq
from .worker import start_workers

@click.group()
def cli():
    """queuectl - minimal background job queue"""
    init()

@cli.command()
def hello():
    click.echo("QueueCTL is ready ✅")

@cli.command("enqueue")
@click.argument("command", nargs=-1, required=True)
@click.option("--max-retries", "-r", type=int, default=3, show_default=True)
def enqueue_cmd(command, max_retries):
    cmd = " ".join(command)
    job_id = enqueue(cmd, max_retries=max_retries)
    click.echo(f"enqueued: {job_id} :: {cmd}")

@cli.command("list")
@click.option("--state", type=click.Choice(["pending","running","succeeded","failed","dead"]), default=None)
@click.option("--json", "as_json", is_flag=True)
def list_cmd(state, as_json):
    rows = list_jobs(state)
    if as_json:
        click.echo(json.dumps(rows, indent=2)); return
    if not rows:
        click.echo("no jobs."); return
    for r in rows:
        click.echo(f"{r['id']} | {r['state']} | attempts={r['attempts']}/{r['max_retries']} | cmd={r['command']}")

@cli.command("run")
@click.option("--workers", "-w", type=int, default=2, show_default=True)
@click.option("--base-backoff", type=int, default=2, show_default=True)
def run_cmd(workers, base_backoff):
    click.echo(f"starting {workers} worker(s)… base_backoff={base_backoff}s")
    start_workers(workers, base_backoff=base_backoff)

@cli.group()
def dlq():
    """Dead Letter Queue commands."""
    pass

@dlq.command("list")
def dlq_list():
    rows = list_dlq()
    if not rows:
        click.echo("dlq empty."); return
    for r in rows:
        click.echo(f"{r['id']} | attempts={r['attempts']}/{r['max_retries']} | cmd={r['command']}")
        if r.get("last_error"):
            click.echo(f"  last_error: {r['last_error'][:200]}")

@dlq.command("requeue")
@click.argument("job_id")
def dlq_requeue(job_id):
    r = requeue_from_dlq(job_id)
    click.echo(f"requeued {job_id} to pending." if r else "job not found in dlq.")

if __name__ == "__main__":
    cli()
