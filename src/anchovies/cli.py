import os
import logging
import subprocess
import yaml, json
import click
from typing import Literal
from prettytable import PrettyTable
from anchovies.sdk import *


class CliError(click.ClickException): 
    ...


description = '''
the `anchovies` CLI application.
manage & run anchovy tasks and services, as well as connect to microservices 
within anchovies.
'''.strip()
@click.group(help=description)
def cli(): ...


operator_help = '''
The anchovies plugin path of a Downloader to run.
Examples: `-op Downloader` runs the base downloader. 
`-op some_plugin.Downloader` runs 
the downloader from anchovies.plugins.some_plugin.
'''.strip()


def depends_on_anchovy(callable):
    '''Make the anchovy and supply to the CLI func.'''
    def wrapper(*args, prod=False, **kwds): 
        if prod: 
            kwds['user'] = 'prod'
        id, user = kwds.get('id'), kwds.get('user')
        anchovy = Anchovy(id, user)
        return callable(*args, anchovy=anchovy, **kwds)
    return wrapper


def add_anchovy_context_options(callable): 
    '''Build ID & User consistently.'''
    callable = click.option('--id', help='The ID of the anchovy to schedule.')(callable)
    callable = click.option('--user', '-u', help='The User of the anchovy to schedule.')(callable)
    callable = click.option('--prod', is_flag=True, help='Set the User context to "prod".')(callable)
    callable = click.option('--log-level', help='Change the log level between INFO/DEBUG/CRITICAL.')(callable)
    return callable


def add_common_customizers(callable): 
    '''Add customizer optinos for datastore, checkpoint, and other class configs.'''
    callable = click.option('--datastore', help='the path/connection uri for the configured datastore')(callable)
    callable = click.option('--checkpoint-cls', help='the runtime checkpoint class to use. use an anchovies plugins import path.')(callable)
    callable = click.option('--task-store-cls', help='the runtime class to use for the task store. use an anchovies plugins import path.')(callable)
    callable = click.option('--tbl-store-cls', help='the runtime tbl store class to use. use an anchovies plugins import path.')(callable)
    callable = click.option('--data-buffer-cls', help=(
            "the default data buffer used by downloaders/uploader. "
            "this is one of the best ways to customize how anchovies "
            "interacts with your data lake. if you don't like the "
            "default data format, change this. use an anchovies "
            "plugins import path."
        ))(callable)
    return callable


@cli.command('run', help='Start a single anchovy!')
@add_anchovy_context_options
@click.option('--operator-cls', '-op', required=True, help=operator_help)
@click.option('--config', '-c', help='Config yaml as a string input.')
@click.option('--config-file', '-f', help='Config yaml file path location.')
@click.option('--service', '-S', is_flag=True, help='toggle the service execution mode (defaulting to task execution mode)')
@click.option('--execution-timeout', '-t', help='the timeout for a single batch')
@click.option('--upstream', '-up', multiple=True, help='Upstream Anchovy IDs.')
@click.option('--enabled', '-e', multiple=True, help='Enable a particular table.')
@click.option('--disabled', '-d', multiple=True, help='Disable a particular table.')
@add_common_customizers
def run_anchovy(
    anchovy: Anchovy,
    operator_cls: str,
    *, 
    service=False,
    **kwds
):
    kwds.update(is_task_executor=not service)
    res, exc = anchovy.run_with_exception_handling(operator_cls, **kwds)
    click.echo(res.dump())
    if isinstance(exc, BaseAnchovyException): 
        raise CliError(exc)
    

@cli.command('checkpoints', help='Get, set, or unset anchovy checkpoints.')
@add_anchovy_context_options
@add_common_customizers
@click.option('--delete', '-D', is_flag=True, help='remove a checkpoint(s)')
@click.option('--confirm', '-y', is_flag=True, help='confirm the edit option')
@click.argument('path_or_glob', required=False)
@depends_on_anchovy
def touch_checkpoints(
    path_or_glob: str=None, 
    anchovy: Anchovy=None, 
    delete=False,
    confirm=False,
    **kwds, 
): 
    '''Get, set, or unset anchovy checkpoints.'''
    path_or_glob = path_or_glob or '*'
    ckpoints = tuple(anchovy.list_checkpoints(path_or_glob, **kwds))
    tbl = PrettyTable(('anchovy user', 'anchovy id', 'checkpoint key', 'value'))
    has_checkpoints = False
    for ckpoint in ckpoints: 
        tbl.add_row((anchovy.user, anchovy.id, *ckpoint))
        #TODO: truncate values
        has_checkpoints = True
    if not has_checkpoints:
        click.echo('None found!')
        return
    click.echo(tbl.get_string(sortby='checkpoint key'))
    if not delete:
        return
    if not confirm: 
        click.confirm('OK to delete these checkpoints?', abort=True)
    anchovy.delete_checkpoints(path_or_glob)


@cli.group(help='Manage multiple anchovies (as a "school").')
def school(): ...


@school.command('run', help='Start multiple anchovies via a "school" file.')
@click.option('--school-file', '-f', help='File name of a list of commands to execute.')
@click.option('--school-data', '-d', help='Text of a list of commands to execute.')
@click.option('--policy', help='RAISE or CONTINUE. Defaults to CONTINUE.')
@click.option('--anchovy-executable', help='Path of the anchovy executable if it is non-standard.')
def school_run(
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
