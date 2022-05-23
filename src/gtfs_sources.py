import json
import logging
import re
import urllib
from datetime import datetime
import dateutil
import pandas as pd
import requests
from gtfs import FeedImport


"""
* GTFS Exchange has shutdown
* https://transitfeeds.com/ moved to https://database.mobilitydata.org/
* www.googleapis.com/storage/v1/b/google-code-archive has shutdown or moved.
* transit_land required api-key, working together with OpenMobilityData
"""


class GTFSSources:
    def __init__(self, sa_session):
        self.__sa_gtfs_session = sa_session
        self.__logger = logging.getLogger(__name__)

    def __deprecated__get_sources_gtfs_exchange(self):
        url = "http://www.gtfs-data-exchange.com/api/agencies?format=json"
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        feed_import = list()
        for gtfs_feed in data['data']:
            feed_import.append(
                FeedImport(
                    feed_source='gtfs_exchange',
                    feed_name=gtfs_feed['name'],
                    feed_url=gtfs_feed['dataexchange_url'] + "latest.zip",
                    feed_dt=datetime.datetime.fromtimestamp(gtfs_feed['date_last_updated'])
                ))
        self.__logger.info("Download source list from {}, found {} sources".format(url, len(feed_import)))
        return feed_import

    def __deprecated__get_sources_transitfeeds(self):
        has_data = True
        page = 1
        limit = 100
        feed_import = list()

        while (has_data):
            url = "https://api.transitfeeds.com/v1/getFeeds?" \
                  "key=%s&descendants=1&page=%d&limit=%d" \
                  % ('3a4b8e30-407a-4306-bdde-24e5ec59dc0b', page, limit)
            response = urllib.urlopen(url)
            data = json.loads(response.read())
            if not data['results']['feeds']:
                break

            for gtfs_feed in data['results']['feeds']:
                if not gtfs_feed['u']:
                    continue
                feed_ts = datetime.datetime.fromtimestamp(
                    int(gtfs_feed['latest']['ts'])) if 'latest' in gtfs_feed else None
                download_url = gtfs_feed['u']['d'] if 'd' in gtfs_feed['u'] else ""
                if download_url:
                    feed_import.append(FeedImport(
                        feed_source='transitfeeds',
                        feed_name=gtfs_feed['t'],
                        feed_url=download_url,
                        feed_dt=feed_ts
                    ))
            page += 1
            self.__logger.info("Download source list from {}, found {} sources".format(url, len(feed_import)))
        return feed_import

    def __deprecated__get_sources_google(self):
        url = "https://www.googleapis.com/storage/v1/b/google-code-archive" \
              "/o/v2%2Fcode.google.com%2Fgoogletransitdatafeed%2Fwiki%2FPublicFeeds.wiki?alt=media"
        response = urllib.urlopen(url)
        content = response.read()
        feed_import = list()
        links = re.findall(r'((https|http|ftp)?://\S+.zip)', content)
        for gtfs_feed in links:
            feed_import.append(FeedImport(feed_source='google_code',
                                          feed_name='',
                                          feed_url=gtfs_feed[0],
                                          feed_dt=datetime.datetime.now()))

        self.__logger.info("Download source list from {}, found {} sources".format(url[0:40], len(feed_import)))
        return feed_import

    def __deprecated__get_sources_transit_land(self):
        download_base_url = "https://s3.amazonaws.com/transit.land/datastore-uploads/feed_version"
        url = "https://transit.land/api/v1/feeds?per_page=1000"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        feed_import = list()

        for gtfs_feed in data['feeds']:
            download_url = gtfs_feed['url']
            feed_ts = dateutil.parser.parse(gtfs_feed['updated_at'])
            feed_import.append(FeedImport(
                feed_source='transitland',
                feed_name=gtfs_feed['onestop_id'],
                feed_url=download_url,
                feed_dt=feed_ts
            ))

        self.__logger.info(f"Download source list from {download_base_url}, found {len(feed_import)} sources")
        return feed_import

    def __get_sources_the_mobility_database(self) -> list:
        """Download csv from the mobility_database site, parse it and return list of FeedImport objects"""
        download_base_url = "https://www.google.com/url?" \
                            "q=https%3A%2F%2Fbit.ly%2Fcatalogs-csv&sa=D&sntz=1&usg=AOvVaw3QVLRlS_nDhkg_h8Id_C1K"
        r = requests.get(download_base_url)
        redirect_url = r.headers['Location']
        data = pd.read_csv(redirect_url)
        feed_import = list()
        for index, row in data.iterrows():
            feed_ts = dateutil.parser.parse(row['location.bounding_box.extracted_on'])
            feed_import.append(FeedImport(
                feed_source='mobility_database',
                feed_name=row['provider'][:1000],
                feed_url=row['urls.direct_download'],
                feed_dt=feed_ts
            ))

        self.__logger.info(f"Download source list from {download_base_url}, found {len(feed_import)} sources")
        return feed_import

    def update_feed_sources(self):
        """Read sources and save them to database"""
        feed_import_list = list()
        feed_import_list += self.__get_sources_the_mobility_database()
        self.__logger.info(f"found {len(feed_import_list)} sources")
        uniq_urls = list()
        for feed in feed_import_list:
            if feed.feed_url not in uniq_urls:
                uniq_urls.append(feed)
                self.__sa_gtfs_session.add(feed)
        self.__logger.info(f'{len(uniq_urls):,} sources loaded')
        self.__sa_gtfs_session.commit()
