import os

import pandas as pd
from geocodio import GeocodioClient
from snowflake.connector.pandas_tools import write_pandas

import environment
import persistence
import query
from loggers import get_logger, timer

logger = get_logger("GEOCODE_CACHE")


@timer(logger)
def cache_location(ops_state=None, organization_name=None):
    try:
        env = environment.read()
        apikey = env['apikey']
        client = GeocodioClient(apikey)

        sql = "SELECT ops_street, ops_city, ops_state, ops_zip5, organization_name FROM " \
              f"""CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location """

        if ops_state is not None and organization_name is not None:
            sql = sql + "WHERE ops_state='{0}' AND organization_name='{1}' ". \
                format(ops_state, query.escape(organization_name))
        elif ops_state is not None:
            sql = sql + "WHERE ops_state='{0}' ".format(ops_state)
        elif organization_name is not None:
            sql = sql + "WHERE organization_name='{0}' ".format(query.escape(organization_name))

        logger.info(sql)
        conn = persistence.get_conn()
        cur = conn.cursor().execute(sql)

        fetch_size = 10
        count_processed = 0

        while True:
            ret = cur.fetchmany(fetch_size)
            if not ret:
                break
            else:

                df_data = []
                street_list = []
                for record in ret:
                    street = [str(record[0]), str(record[1]), str(record[2]), str(record[3])]
                    street = ",".join(street)
                    street_list.append(query.escape(street))

                    geo = client.batch_geocode(street_list)

                    # doesn't enforce best match, for testing purpose only
                    geo = geo[0]

                    detail_list = []
                    if geo is not None and geo.get('results') is not None and len(geo.get('results')) > 0:
                        # print(json.dumps(geo.get('results')[0]))
                        # geo_details_list.append(query.escape(json.dumps(geo.get('results')[0])))

                        accuracy_type = geo.get('results')[0]['accuracy_type']
                        accuracy = geo.get('results')[0]['accuracy']
                        lon = geo.get('results')[0]['location']['lng']
                        lat = geo.get('results')[0]['location']['lat']

                        detail_list = [accuracy_type, accuracy, lon, lat]

                    row = list(record) + detail_list
                    df_data.append(row)

                df = pd.DataFrame(data=df_data, columns=['ops_street',
                                                         'ops_city',
                                                         'ops_state',
                                                         'ops_zip5',
                                                         'organization_name',
                                                         'type',
                                                         'accuracy',
                                                         'lon',
                                                         'lat'])
                write_pd_to_db(df)

                count_processed = len(ret) + count_processed

                if count_processed % 100 == 0:
                    logger.info("{0} records processed".format(count_processed))

        logger.info("Total {0} records processed".format(count_processed))
    except Exception as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()


def save_to_db(ret, geo_details_list):
    if not ret:
        return
    try:
        sql = f"""INSERT INTO dnb.dim_location_cache(ops_street, ops_city, ops_state, ops_zip5, organization_name, """ \
              "type, accuracy, lon, lat) "
        temp_str = ""
        for i in range(len(ret)):
            record = ret[i]
            if geo_details_list[i] is not None and len(geo_details_list[i]) > 0:
                accuracy_type = geo_details_list[i]['accuracy_type']
                accuracy = geo_details_list[i]['accuracy']
                lon = geo_details_list[i]['location']['lng']
                lat = geo_details_list[i]['location']['lat']

                temp_str = "SELECT '{0}','{1}','{2}','{3}','{4}','{5}',{6},{7}, {8} ".format(record[0], record[1],
                                                                                             record[2], record[3],
                                                                                             query.escape(record[4]),
                                                                                             accuracy_type, accuracy,
                                                                                             lon, lat)

                sql = sql + temp_str

                logger.info(sql)

                if i < len(ret) - 1:
                    sql = sql + ' UNION ALL '

        logger.info(sql)
        conn = persistence.get_conn()
        conn.cursor().execute(sql)
    except Exception as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()


def write_pd_to_db(df):
    logger.info(df.info())
    logger.info(df.head(10))
    if df.empty:
        return
    with persistence.get_conn() as conn:
        logger.info('Writing dataframe to dnb.dim_location_cache')
        write_pandas(conn, df, table_name='DIM_LOCATION_CACHE', schema='dnb', quote_identifiers=False)


if __name__ == "__main__":
    ops_state_test = 'MS'
    organization_name_test = 'CATERPILLAR'
    cache_location(ops_state=ops_state_test, organization_name=organization_name_test)
    logger.info("geocode_cache completed")
