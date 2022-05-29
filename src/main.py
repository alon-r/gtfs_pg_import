import click
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from gtfs_import import GTFSImport
import logging

logging.basicConfig(level=logging.INFO)


@click.group()
def cli():
    pass


@cli.command()
@click.argument('db_con_str', envvar='GTFS_DB')
def load_sources(db_con_str):
    """Load GTFS sources Urls and info to database"""
    click.echo('Load GTFS sources')
    engine = create_engine(db_con_str)
    with Session(engine) as sa_session:
        GTFSImport(sa_session=sa_session).update_feed_sources()


@cli.command()
@click.argument('db_con_str', envvar='GTFS_DB')
@click.option('--offset_v', default=None, help='Offset from table gtfs_feed_import')
@click.option('--limit_v', default=None, help='Limit from table gtfs_feed_import')
def load_data(db_con_str, offset_v, limit_v):
    """Download GTFS sources extract and load to db"""
    click.echo('Download parse and store GTFS data')
    engine = create_engine(db_con_str)
    with Session(engine) as sa_session:
        GTFSImport(sa_session).import_sources(offset_v, limit_v)
