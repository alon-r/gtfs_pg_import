from sqlalchemy import Column, Sequence, Unicode
from sqlalchemy.types import Date, String, Integer, DateTime, SmallInteger, Float, Text, ARRAY

from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry, Geography
Base = declarative_base()
metadata = Base.metadata
metadata.schema = 'gtfs'


class FeedImport(Base):
    filename = None
    __tablename__ = 'gtfs_feed_import'
    __table_args__ = {u'schema': 'gtfs'}

    feed_id = Column(Integer, Sequence(None, optional=True), primary_key=True, nullable=True)
    feed_source = Column(String(255))
    feed_name = Column(Unicode(1000))
    feed_url = Column(String(255))
    feed_dt = Column(DateTime)
    feed_size_kb = Column(Float)
    feed_checksum = Column(String(255))
    download_dt = Column(DateTime)
    done = Column(SmallInteger, default=0)
    error = Column(Text)


class Agency(Base):
    filename = 'agency.txt'
    __tablename__ = 'gtfs_agency'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, Sequence(None, optional=True), primary_key=True, nullable=True)
    feed_id = Column(Integer)
    agency_id = Column(String(255), index=True)
    agency_name = Column(String(255))
    agency_url = Column(String(255))
    agency_timezone = Column(String(50))
    agency_lang = Column(String(10))
    agency_phone = Column(String(50))
    agency_fare_url = Column(String(255))

    @classmethod
    def dict2Obj(cls, dict):
        obj = cls()
        for k, v in dict.items():
            if v:
                setattr(obj, k, v.encode("utf-8").decode('utf-8', 'ignore'))
        return obj


class Stop(Base):
    filename = 'stops.txt'
    __tablename__ = 'gtfs_stops'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer)
    stop_id = Column(String(255))
    stop_code = Column(String(50))
    stop_name = Column(String(255), nullable=False)
    stop_desc = Column(String(255))
    # stop_lat = Column(Numeric(12, 9), nullable=False)
    # stop_lon = Column(Numeric(12, 9), nullable=False)
    stop_loc = Column(Geography(geometry_type='POINT'))
    zone_id = Column(String(50))
    stop_url = Column(String(255))
    location_type = Column(Integer, index=True, default=0)
    parent_station = Column(String(255))
    stop_timezone = Column(String(50))
    wheelchair_boarding = Column(Integer, default=0)
    platform_code = Column(String(50))
    direction = Column(String(50))
    position = Column(String(50))

    stop_lat = None
    stop_lon = None

    @classmethod
    def dict2Obj(cls, dict):
        obj = cls()
        for k, v in dict.items():
            if v:
                setattr(obj, k, v)
        stop_loc = 'POINT({0} {1})'.format(dict['stop_lon'], dict['stop_lat'])
        setattr(obj, 'stop_loc', stop_loc)
        return obj


class Route(Base):
    filename = 'routes.txt'
    __tablename__ = 'gtfs_routes'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer)
    route_id = Column(String(255))
    agency_id = Column(String(255), index=True, nullable=True)
    route_short_name = Column(String(255))
    route_long_name = Column(String(255))
    route_desc = Column(String(1023))
    route_type = Column(Integer, index=True, nullable=False)
    route_url = Column(String(255))
    route_color = Column(String(6))
    route_text_color = Column(String(6))
    route_sort_order = Column(Integer, index=True)
    min_headway_minutes = Column(Integer)  # Trillium extension.

    @classmethod
    def dict2Obj(cls, dict):
        obj = cls()
        for k, v in dict.items():
            if v:
                setattr(obj, k, v.encode("utf-8").decode('utf-8', 'ignore'))
        return obj


class Trip(Base):
    filename = 'trips.txt'
    __tablename__ = 'gtfs_trips'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer)
    trip_id = Column(String(255))
    route_id = Column(String(255), index=True)
    service_id = Column(String(255), index=True, nullable=False)
    direction_id = Column(String(255), index=True)
    block_id = Column(String(255), index=True)
    shape_id = Column(String(255), index=True, nullable=True)
    trip_type = Column(String(255))
    trip_headsign = Column(String(255))
    trip_short_name = Column(String(255))
    bikes_allowed = Column(Integer, default=0)
    wheelchair_accessible = Column(Integer, default=0)
    stops = Column(ARRAY(String))

    @classmethod
    def dict2Obj(cls, dict):
        obj = cls()
        for k, v in dict.items():
            if v:
                setattr(obj, k, v)
        return obj


class Shape(Base):
    filename = 'shapes.txt'
    __tablename__ = 'gtfs_shapes'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer)
    shape_id = Column(String(255), index=True)
    shape = Column(Geometry(geometry_type='LINESTRING'))


class FeedInfo(Base):
    filename = 'feed_info.txt'
    __tablename__ = 'gtfs_feed_info'
    __table_args__ = {u'schema': 'gtfs'}

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer)
    feed_publisher_name = Column(String(255))
    feed_publisher_url = Column(String(255))
    feed_lang = Column(String(255))
    feed_start_date = Column(Date)
    feed_end_date = Column(Date)
    feed_version = Column(String(255))
    feed_license = Column(String(255))

    @classmethod
    def dict2Obj(cls, dict):
        obj = cls()
        for k, v in dict.items():
            if v:
                setattr(obj, k, v)
        return obj
