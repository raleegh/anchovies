import os
import click
import subprocess
import yaml, json
from typing import Literal
from anchovies.sdk import *


class CliError(click.ClickException): 
    ...


@click.group()
def cli(): ...


operator_help = '''
The anchovies plugin path of a Downloader to run.
Examples: `-op Downloader` runs the base downloader. 
`-op some_plugin.Downloader` runs 
the downloader from anchovies.plugins.some_plugin.
'''.strip()


@cli.command(help='Start a single anchovy!')
@click.option('--id', help='The ID of the anchovy to schedule.')
@click.option('--user', '-u', help='The User of the anchovy to schedule.')
@click.option('--operator-cls', '-op', required=True, help=operator_help)
@click.option('--config', '-c', help='Config yaml as a string input.')
@click.option('--config-file', '-f', help='Config yaml file path location.')
@click.option('--service', '-S', is_flag=True, help='toggle the service execution mode (defaulting to task execution mode)')
@click.option('--execution-timeout', '-t', help='the timeout for a single batch')
@click.option('--upstream', '-up', multiple=True, help='Upstream Anchovy IDs.')
@click.option('--enabled', '-e', multiple=True, help='Enable a particular table.')
@click.option('--disabled', '-d', multiple=True, help='Disable a particular table.')
@click.option('--datastore', help='the path/connection uri for the configured datastore')
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
@click.option('--prod', is_flag=True, help='Set the User context to "prod".')
def run(
    operator_cls: str,
    id: str=None, 
    user: str=None, 
    *, 
    service=False,
    prod=False,
    **kwds
):
    if prod: 
        user = 'prod'
    anchovy = Anchovy(id, user)
    kwds.update(is_task_executor=not service)
    res, exc = anchovy.run_with_exception_handling(
        operator_cls, 
        **kwds
    )
    click.echo(res.dump())
    if isinstance(exc, BaseAnchovyException): 
        raise CliError(exc)
    

@cli.group(help='Start multiple anchovies! Or, work on metadata for multiple.')
def school(): ...


@school.command('run', help='Start multiple anchovies via a "school" file.')
@click.option('--school-file', '-f', help='File name of a list of commands to execute.')
@click.option('--school-data', '-d', help='Text of a list of commands to execute.')
@click.option('--policy', help='RAISE or CONTINUE. Defaults to CONTINUE.')
@click.option('--anchovy-executable', help='Path of the anchovy executable if it is non-standard.')
@click.pass_context
def school_run(
    ctx: click.Context,
    school_file: str=None, 
    school_data: str=None, 
    anchovy_executable: str=None,
    policy: Literal['RAISE', 'CONTINUE']=None,    
):
    policy = policy or 'CONTINUE'
    if school_file:
        try:
            with open(school_file) as f: 
                instructions = tuple(read_json_or_yaml(f))
        except FileNotFoundError as e: 
            raise click.ClickException(e) from e
    if school_data: 
        instructions = tuple(read_json_or_yaml(school_data))
    if not instructions: 
        raise click.ClickException(
            'Either the file or data option must be used.'
        )
    results = list()
    for instruction in instructions: 
        if isinstance(instruction, (list, tuple)): 
            cmd_string = ' '.join(instruction)
        else: 
            cmd_string = instruction
            instruction = tuple(instruction.split())
        cmd_string = 'anchovy ' + cmd_string
        click.echo(cmd_string)
        click.echo('+' * os.get_terminal_size().columns)
        cmd = (*(anchovy_executable or 'anchovy').split(), *instruction)
        result = subprocess.run(cmd)
        if policy == 'RAISE': 
            result.check_returncode()
        results.append((cmd_string, result))
        click.echo('')
    if policy == 'CONTINUE':
        click.echo('anchovy school results')
        click.echo('+' * os.get_terminal_size().columns)
        for cmd_string, result in results: 
            try:
                result.check_returncode()
                click.echo('OK -> ' + cmd_string)
            except: 
                click.echo('ERR -> ' + cmd_string)
                raise




def read_json_or_yaml(f): 
    try: 
        return yaml.safe_load(f)
    except: pass
    try: 
        return json.load(f)
    except Exception as e:
        raise click.ClickException(
            'The input file could not be read by JSON or YAMl'
        ) from e


if __name__ == '__main__': 
    cli()
