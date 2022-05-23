import io
import traceback
from urllib.parse import urlparse
from zipfile import ZipFile
import csv
import logging
import pandas as pd
import sqlalchemy
from geopandas import GeoDataFrame
from shapely.geometry import Point, LineString
from geoalchemy2.shape import from_shape
import datetime
import requests
from io import BytesIO
import sys
import hashlib
from ftplib import FTP

from gtfs import FeedImport, Trip, Shape, Base, metadata
from gtfs_sources import GTFSSources

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.INFO)


class ErrorUnknownFeedSource(Exception):
    pass


class ErrorMissingStopFile(Exception):
    pass


class ErrorMissingStopsTimesFile(Exception):
    pass


class GTFSImport(object):
    def __init__(self, sa_session):
        self.__sa_gtfs_session = sa_session
        self.__logger = logging.getLogger(__name__)

        # Create schema if not exists
        engine = sa_session.bind
        if not engine.dialect.has_schema(engine, metadata.schema):
            engine.execute(sqlalchemy.schema.CreateSchema(metadata.schema))
        metadata.create_all(engine, checkfirst=True)

    @staticmethod
    def __build_shapes(data: io.StringIO) -> list:
        """Return list of LineString geometries"""
        df = pd.read_csv(data)
        geometry = [Point(xy) for xy in zip(df.shape_pt_lon, df.shape_pt_lat)]
        df = GeoDataFrame(df, geometry=geometry)
        df = df.groupby(['shape_id'])['geometry'].apply(lambda x: LineString(x.tolist()))
        shapes = df.to_dict()
        return shapes

    @staticmethod
    def __generate_trip_stops(input_zip: ZipFile) -> pd.DataFrame:
        """Return dataframe of trip stops"""
        data = io.StringIO(input_zip.read('stop_times.txt').decode())
        df = pd.read_csv(data)
        trip_stops = df.groupby('trip_id')['stop_id'].apply(tuple).drop_duplicates()
        return trip_stops

    def validate_zip_file(self, input_zip):
        if 'stops.txt' not in input_zip.namelist():
            raise ErrorMissingStopFile
        if 'stop_times.txt' not in input_zip.namelist():
            raise ErrorMissingStopFile

    def __import_file(self, feed_id: int, input_zip: BytesIO):
        zf = ZipFile(input_zip)
        self.validate_zip_file(zf)
        gtfs_classes = [c for c in Base.__subclasses__() if c.filename is not None]
        for gtfs_cls in gtfs_classes:
            if gtfs_cls.filename not in zf.namelist():
                self.__logger.warning(f"{gtfs_cls.filename} not exists")
                continue

            # generate dataframe with trip_stops
            if gtfs_cls is Trip:
                stops_df = GTFSImport.__generate_trip_stops(zf)

            data = io.StringIO(zf.read(gtfs_cls.filename).decode())
            # read shape file
            if gtfs_cls is Shape:
                shapes = GTFSImport.__build_shapes(data)
                for shape_id in shapes:
                    shp = Shape(feed_id=feed_id, shape_id=shape_id, shape=from_shape(shapes[shape_id]))
                    self.__sa_gtfs_session.add(shp)
                self.__sa_gtfs_session.commit()
                self.__logger.info("Insert {}".format(gtfs_cls.filename))
            # read csv data files
            else:
                reader = csv.DictReader(data)
                reader = list(reader)
                obj_collection = list()
                buffer_size = 10000
                buffer_idx = 0
                csv_lines = len(reader)
                self.__logger.info("Writing {} , (num rows {})".format(gtfs_cls.filename, csv_lines))
                for i, row in enumerate(reader):
                    if gtfs_cls is Trip:
                        try:
                            trip_id = int(row['trip_id'])
                        except ValueError:
                            trip_id = row['trip_id']

                        if trip_id in stops_df.index:
                            row['stops'] = map(str, stops_df[trip_id])
                        else:
                            continue
                    obj = gtfs_cls.dict2Obj(row)
                    obj.feed_id = feed_id
                    obj_collection.append(obj)
                    if buffer_idx > buffer_size:
                        self.__sa_gtfs_session.bulk_save_objects(obj_collection)
                        self.__sa_gtfs_session.commit()
                        buffer_idx = 0
                    buffer_idx += 1

                    sys.stdout.write("\r%d%%" % ((float(i)/csv_lines)*100))
                    sys.stdout.flush()
                self.__sa_gtfs_session.bulk_save_objects(obj_collection)
                self.__sa_gtfs_session.commit()
                self.__logger.info("Done insert {}".format(gtfs_cls.filename))

    def __download_data(self, url: str):
        # get content from url
        self.__logger.debug(f'downloading from url: {url}')
        o = urlparse(url)
        if o.scheme in ['http', 'https']:
            request = requests.get(url)
            data_content = request.content
        # get content from ftp
        elif o.scheme == 'ftp':

            ftp = FTP(o.netloc)
            ftp.login()
            r = io.StringIO()
            ftp.retrbinary('RETR ' + o.path, r.write)

            data_content = r.getvalue()
        else:
            raise ErrorUnknownFeedSource(f'feed url: {url}')

        return data_content

    def update_feed_sources(self):
        """
        Update sources list (gtfs_feed_import table)
        :return:
        """
        GTFSSources(self.__sa_gtfs_session).update_feed_sources()

    def import_sources(self, offset_v: int = None, limit_v: int = None):
        """
        Download data source and update database.
        :param offset_v: rows offset (table: gtfs_feed_import)
        :param limit_v: rows limit (table: gtfs_feed_import)
        :return:
        """
        # Fetch feeds info
        feeds = self.__sa_gtfs_session.query(FeedImport) \
            .filter(FeedImport.done == 0) \
            .order_by(FeedImport.feed_id)

        if offset_v:
            feeds = feeds.offset(offset_v)

        if limit_v:
            feeds = feeds.limit(limit_v)

        feeds = feeds.all()

        for feed in feeds:
            try:
                content = self.__download_data(feed.feed_url)
                with BytesIO(content) as zip_data_f:
                    feed.feed_size_kb = sys.getsizeof(zip_data_f)/1024
                    feed.feed_checksum = hashlib.md5(zip_data_f.read()).hexdigest()
                    feed.download_dt = datetime.datetime.now()
                    self.__import_file(feed.feed_id, zip_data_f)
                    feed.done = 1
                    self.__sa_gtfs_session.commit()
                    self.__logger.info(f"store {feed.feed_url}")
            except Exception as e:
                self.__sa_gtfs_session.rollback()
                feed.error = str(e)
                feed.done = 2
                self.__sa_gtfs_session.commit()
                self.__logger.error(f"store {feed.feed_url}")
                self.__logger.error(traceback.format_exc())
