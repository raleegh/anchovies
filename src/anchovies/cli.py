import click
from anchovies.sdk import *


class CliError(click.ClickException): 
    ...


@click.group()
def cli(): ...


operator_help = '''
The anchovies plugin path of a Downloader to run.
Examples: 
    `-o Downloader` runs the base downloader.
    `-o some_plugin.Downloader` runs the downloader from anchovies.plugins.some_plugin.
'''.strip()


@cli.command()
@click.option('--id', help='The ID of the anchovy to schedule.')
@click.option('--user', '-u', help='The User of the anchovy to schedule.')
@click.option('--operator', '-op', required=True, help=operator_help)
@click.option('--config', '-C', help='Config yaml as a string input.')
@click.option('--config-file', '-f', help='Config yaml file path location.')
@click.option('--service', '-S', is_flag=True, help='toggle the Service execution mode (defaulting to task execution mode)')
@click.option('--execution-timeout', '-t', help='the timeout for a single batch')
@click.option('--upstream', '-U', multiple=True, help='Upstream Anchovy IDs.')
@click.option('--enabled', '-e', multiple=True, help='Enable a particular table.')
@click.option('--disabled', '-d', multiple=True, help='Disable a particular table.')
@click.option('--metastore', help='the path/connection uri for the configured metastore')
@click.option('--metastore-cls', help='the runtime class to use for the metastore. use an anchovies plugins import path')
@click.option('--checkpoint-cls', help='the runtime checkpoint class to use. use an anchovies plugins import path.')
@click.option('--task-store-cls', help='the runtime class to use for the task store. use an anchovies plugins import path.')
@click.option('--tbl-store-cls', help='the runtime tbl store class to use. use an anchovies plugins import path.')
@click.option('--data-buffer-cls', help=(
        "the default data buffer used by downloaders/uploader. "
        "this is one of the best ways to customize how anchovies "
        "interacts with your data lake. if you don't like the "
        "default data format, change this. use an anchovies "
        "plugins import path."
    ))
def run(
    operator: str,
    id: str=None, 
    user: str=None, 
    *, 
    service=False,
    **kwds
):
    anchovy = Anchovy(id, user)
    kwds.update('is_task_executor', not service)
    res, exc = anchovy.run_with_exception_handling(
        operator, 
        **kwds
    )
    click.echo(res.dump())
    if isinstance(exc, BaseAnchovyException): 
        raise CliError(exc)


if __name__ == '__main__': 
    cli()
