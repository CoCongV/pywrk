import asyncio
import uvloop
import multiprocessing

import click

from pywrk.main import main

uvloop.install()


@click.command()
@click.argument('url')
@click.option('-W',
              '--works',
              default=multiprocessing.cpu_count(),
              help="works number",
              type=int)
@click.option('-H', '--headers', help="request header", type=str)
@click.option('-T', '--timeout', help="timeout", type=int, default=2)
@click.option('-D', '--duration', default="1s", type=str)
@click.option('-C', '--connections', default=multiprocessing.cpu_count() * 100)
@click.option('-M', '--method', default='get')
def wrk(url, works, headers, timeout, duration, connections, method):
    print(f"Running {duration} test @ {url} method: {method}")
    print(f"{works} works and {connections} connections")
    asyncio.run(
        main(url, works, headers, timeout, duration, connections, method))
