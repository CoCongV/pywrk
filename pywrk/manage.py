import multiprocessing

import click

from pywrk.main import main


@click.command()
@click.argument('url')
@click.option('-W',
              '--works',
              default=multiprocessing.cpu_count(),
              help="works number",
              type=int)
@click.option('-H', '--headers', help="request header", type=str)
@click.option('-T', '--timeout', help="timeout", type=int)
@click.option('-D', '--duration', default="1s", type=str)
@click.option('-C', '--connections', default=multiprocessing.cpu_count())
@click.option('-M', '--method', default='get')
def wrk(url, works, headers, timeout, duration, connections, method):
    main(url, works, headers, timeout, duration, connections, method)
