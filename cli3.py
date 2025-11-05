import click
from .db import init

@click.group()
def cli():
    """queuectl - minimal background job queue"""
    init()  # ensure DB/tables exist

@cli.command()
def hello():
    """Sanity check command"""
    click.echo("QueueCTL is ready ✅")

if __name__ == "__main__":
    cli()
