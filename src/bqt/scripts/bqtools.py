#!/usr/bin/env python
import click
from bqt.lib.table import BqTable
from bqt import bqt


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    '--start',
    default=None,
    help='start date (inclusive). only if deleting table partitions',
    metavar="DATE"
)
@click.option(
    '--end',
    default=None,
    help='end date (inclusive). only if deleting table partitions',
    metavar="DATE"
)
@click.option(
    '--all-partitions',
    default=False, is_flag=True,
    help='delete all partitions. only if deleting table partitions'
)
@click.option(
    '--all-yes',
    default=False, is_flag=True,
    help="Say yes to everything (careful!)"
)
@click.argument('name')
def delete_table(start, end, all_partitions, all_yes, name):
    if start and end and all_partitions:
        click.echo("You can't provide everything at the same time!", err=True)
        return
    if start and not end or end and not start:
        click.echo("You need both a start and end", err=True)
        return

    if (start or all_partitions) and 'YYYY' not in name:
        if not all_yes and not click.confirm(
                "You said you want to delete a bunch of tables with the "
                "format `%s`\nbut that doesn't look like a format and more"
                " like a single partition name. bruhhhhhhh...r u 4 sure...?"
                % name, prompt_suffix='??'):
            return
        click.echo("smh, hope you know what you're doing")

    if start:
        if not all_yes and not click.confirm(
                "Are you sure you want to delete `%s` from '%s' to '%s'" % (
                    name, start, end)):
            click.echo("Please think before you type next time. bye.")
            return
        project, dataset, table = BqTable.break_full_name(name)
        bqt.delete_partition_range(
            table, dataset, start, end, project=project, confirm=False
        )
    elif all_partitions:
        if not all_yes and not click.confirm(
                "Are you sure you want to delete %s partitions of `%s`" % (
                    click.style('**ALL**', fg='red', bold=True), name
                )):
            click.echo("Please think before you type next time. bye.")
            return
        project, dataset, table = BqTable.break_full_name(name)
        bqt.delete_all_partitions(table, dataset, project, confirm=False)
    else:
        if not all_yes and not click.confirm(
                "Are you sure you want to delete `%s`?" % name):
            click.echo("Please think before you type next time. bye.")
            return
        project, dataset, table = BqTable.break_full_name(name)
        bqt.delete_table(table, dataset, project, confirm=False)

    click.secho("All Done!!", fg="green")


@cli.command()
@click.option(
    '--rename',
    nargs=2, type=str, multiple=True,
    help='Rename column from first value to second value',
    metavar="from:to"
)
@click.option(
    '--cast',
    nargs=2, type=str, multiple=True,
    help='Cast column from first value to type in second value',
    metavar="col_name:sql_type"
)
@click.option(
    '--add',
    multiple=True, type=str, nargs=2,
    help='Add a new column (first value) using expression in the second value',
    metavar="name:definition"
)
@click.option(
    '--delete',
    multiple=True, type=str,
    help='Delete the provided column'
)
@click.option(
    '--start',
    default=None,
    help='start date (inclusive). only if altering range of partitions',
    metavar="DATE"
)
@click.option(
    '--end',
    default=None,
    help='end date (inclusive). only if altering range of partitions',
    metavar="DATE"
)
@click.option(
    '--all-partitions',
    default=False, is_flag=True,
    help='Apply to all partitions of the table'
)
@click.option(
    '--all-yes',
    default=False, is_flag=True,
    help="Say yes to everything (careful!)"
)
@click.argument('name')
def alter_table(rename, cast, add, delete, start, end, all_partitions,
                all_yes, name):
    if not start and not end and not all_partitions and 'YYYY' in name:
        click.echo(
            "You have specified a partition range but haven't provided"
            " --start, --end, or --all-partitions",
            err=True
        )
        return
    project, dataset, table = BqTable.break_full_name(name)
    table_obj = BqTable(
        table, dataset=dataset, project=project, start=start, end=end
    )
    bqt.change_project(project)
    at = bqt.alter_table(dataset, table_obj)
    for _from, to in rename:
        at.rename_column(_from, to)
    for col, _type in cast:
        at.cast_column(col, _type)
    for col, _def in add:
        at.add_column(col, _def)
    for col in delete:
        at.delete_column(col)

    msg = click.style(
        'Going to make these changes:\n', fg='magenta', bold=True)
    msg += at.human_summary()
    msg += "\nFor table `%s.%s.[%s]`" % (
        project, dataset, click.style(str(table), bold=True)
    )
    msg += click.style("\nDoes this look okay", fg='magenta', bold=True)
    if not all_yes and not click.confirm(msg):
        click.echo(
            "pfff, and you went through the trouble of typing all of that"
        )
        return
    try:
        at.apply(confirm=False)
        click.secho("All Done!!", fg="green")
    except RuntimeError:
        click.secho(
            "Part of the process failed, check your parameters and\n"
            "make sure all your names and expressions are valid for SQL.",
            fg='red', bold=True, err=True
        )
