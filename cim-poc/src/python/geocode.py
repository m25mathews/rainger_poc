import pandas as pd
from geocodio import GeocodioClient
from snowflake.connector.pandas_tools import write_pandas
from timeout_decorator import timeout
import persistence

import environment
from loggers import get_logger, timer
import query


logger = get_logger("GEOCODE")

# pd.set_option("display.max_columns", None)

"""
To avoid Geocodio API call throttling.

(1) For given (state, organization), try to load geocode information from cached database
(2) Use geocode from cached database if available
(3) Call Geocodio API for those not existing in cached database
(4) Return DataFrame with (accuracy, type, lon, lat, formatted_addresss) columns
"""


class GeocodeTableCache:

    def __init__(self, input_df, id_column, street_column, city_column, state_column, zip_column, organization_column):
        self.input_df = input_df
        self.id_column = id_column
        self.street_column = street_column
        self.city_column = city_column
        self.state_column = state_column
        self.zip_column = zip_column
        self.organization_column = organization_column

        self.ops_states = self.input_df[self.state_column].unique()
        self.organizations = self.input_df[self.organization_column].unique()

        logger.info('Original dataframe shape {0} '.format(self.input_df.shape))
        self.df_from_cache = self.__load_from_cache__()
        logger.info('Cached from database dataframe shape {0} '.format(self.df_from_cache.shape))
        self.df_without_value_from_cache = None

    """
    Load from cache table to create a DataFrame for the given query filters.
    """

    def __load_from_cache__(self):

        states_sql = ",".join(f"'{state}'" for state in self.ops_states)
        organizations_sql = ",".join(f"'{query.escape(org)}'" for org in self.organizations)

        sql = "SELECT ops_street as {0}, ops_city as {1}, ops_state as {2}, ops_zip5 as {3}, organization_name, type, " \
              "accuracy, lon, lat, FORMATTED_ADDRESS FROM " \
              "TEMP.DIM_LOCATION_CACHE ".format(
                  self.street_column, self.city_column, self.state_column, self.zip_column
            )
        
        sql = sql + "WHERE ops_state in ({0}) AND organization_name in ({1}) ".format(
            states_sql,
            organizations_sql
        )

        sql = sql + " AND FORMATTED_ADDRESS IS NOT NULL "

        logger.debug(sql)

        df = persistence.get_df(sql)

        return df

    def df_from_db(self):
        return self.df_from_cache

    def save_to_db(self, df_from_api):

        if df_from_api.empty:
            return

        df = pd.merge(left=df_from_api,
                      right=self.df_without_value_from_cache,
                      on=[self.id_column],
                      how='inner').drop_duplicates()

        df = df[[self.id_column, self.street_column, self.city_column, self.state_column, self.zip_column,
                 'type', 'accuracy', 'lon', 'lat', 'ORGANIZATION_NAME', 'formatted_address_from_api']]

        df.rename({self.street_column: 'OPS_STREET',
                   self.city_column: "OPS_CITY",
                   self.state_column: "OPS_STATE",
                   self.zip_column: "OPS_ZIP5",
                   'formatted_address_from_api': "FORMATTED_ADDRESS"},
                  axis='columns', inplace=True)

        df_original_with_duplicate = df.copy()
        df.drop_duplicates(subset=None, keep="first", inplace=True)
        if df.empty:
            return
        with persistence.get_conn() as conn:
            logger.info('Writing dataframe to TEMP.dim_location_cache rows {0}'.format(df.shape[0]))
            write_pandas(conn, df.drop(columns=[self.id_column]), table_name='DIM_LOCATION_CACHE', schema='TEMP', quote_identifiers=False)

        return df_original_with_duplicate

    def df_difference(self):
        df = pd.merge(left=self.input_df,
                      right=self.df_from_cache,
                      on=[self.street_column, self.city_column, self.state_column, self.zip_column, self.organization_column],
                      how='left').drop_duplicates()
        logger.info('Merged dataframe shape-----{0} '.format(df.shape))

        is_null = df['ACCURACY'].isnull()

        df_with_value_from_cache = df[~is_null]
        self.df_without_value_from_cache = df[is_null]

        logger.info("total rows of cache {0}, total rows of input dataframe {1},  "
                    "total rows available from cache {2}, total rows not available from cache {3}"
                    .format(self.df_from_cache.shape[0], self.input_df.shape[0],
                            df_with_value_from_cache.shape[0],
                            self.df_without_value_from_cache.shape[0]))
        return df_with_value_from_cache, self.df_without_value_from_cache


def reverse_tuple(x):
    try:
        x = x[::-1]
        return x
    except Exception:
        logger.error('reverse_tuple failed')
        return None


@timeout(15 * 60)
@timer(logger)
def geocode_df(input_df, id_column, street_column, city_column, state_column, zip_column, organization_column):
    df_cache = GeocodeTableCache(input_df,
                                 id_column,
                                 street_column,
                                 city_column,
                                 state_column,
                                 zip_column,
                                 organization_column)

    df_with_value_from_cache, df_without_value_from_cache = df_cache.df_difference()

    df_with_value_from_cache.rename({'LAT': 'lat',
                                     'LON': 'lon',
                                     'TYPE': 'type',
                                     'ACCURACY': 'accuracy'},
                                    axis='columns', inplace=True)

    df_from_api = geocode_df_from_api(df_without_value_from_cache, id_column, street_column, city_column, state_column,
                                      zip_column)

    logger.info("To be saved to cache database rows {0}".format(df_from_api.shape[0]))

    df_from_api = df_cache.save_to_db(df_from_api)

    # combine
    df_combined = pd.concat([df_with_value_from_cache[[id_column,
                                                       'type',
                                                       'accuracy',
                                                       'lon',
                                                       'lat',
                                                       'FORMATTED_ADDRESS']], df_from_api])

    df_combined = df_combined[[id_column, 'type', 'accuracy', 'lon', 'lat', 'FORMATTED_ADDRESS']].reset_index()

    return df_combined


@timeout(60 * 60)
@timer(logger)
def geocode_df_from_api(input_df, id_column, street_column, city_column, state_column, zip_column):

    if input_df.empty:
        return input_df

    # set up client
    params = environment.read()
    geocodio_api_key = params['apikey']

    # create address list input
    id_list = input_df[id_column]

    if street_column is not None:
        street_list = input_df[street_column].astype(str).str.strip()
    else:
        street_list = ''

    if city_column is not None:
        city_list = input_df[city_column].astype(str).str.strip()
    else:
        city_list = ''

    if state_column is not None:
        state_list = input_df[state_column].astype(str).str.strip()
    else:
        state_list = ''

    if zip_column is not None:
        zip_list = input_df[zip_column].astype(str).str.strip()
    else:
        zip_list = ''

    address_list = (
            street_list + " " +
            city_list + ", " +
            state_list + " " +
            zip_list
    ).tolist()

    id_list = id_list
    address_list = address_list

    if len(address_list) <= 10000:
        # open client & geocode
        client = GeocodioClient(geocodio_api_key)
        geocodio_res = client.geocode(address_list)

        # format results
        ret_df = pd.DataFrame(
            {
                'id': id_list,
                'lat_lon': [reverse_tuple(i) for i in geocodio_res.coords],
                'type': [i.best_match.get('accuracy_type') for i in geocodio_res],
                'accuracy': [i.best_match.get('accuracy') for i in geocodio_res],
                'formatted_address_from_api': geocodio_res.formatted_addresses
            }
        )
    else:
        # chunk up the address list
        n = 10000
        id_chunks = [id_list[i * n:(i + 1) * n] for i in range((len(id_list) + n - 1) // n)]
        address_chunks = [address_list[i * n:(i + 1) * n] for i in range((len(address_list) + n - 1) // n)]
        num_chunks = len(address_chunks)

        ret_list = []
        for nc in range(num_chunks):
            # open client & geocode
            client = GeocodioClient(geocodio_api_key)
            geocodio_res = client.geocode(address_chunks[nc])

            # format results
            ret_df = pd.DataFrame(
                {
                    'id': id_chunks[nc],
                    'lat_lon': [reverse_tuple(i) for i in geocodio_res.coords],
                    'type': [i.best_match.get('accuracy_type') for i in geocodio_res],
                    'accuracy': [i.best_match.get('accuracy') for i in geocodio_res],
                    'formatted_address_from_api': geocodio_res.formatted_addresses
                }
            )
            ret_list.append(ret_df)

        ret_df = pd.concat(ret_list)

    ret_df[['lon', 'lat']] = ret_df['lat_lon'].astype(
        str
    ).str.replace(
        '(',
        '',
        regex=False
    ).str.replace(
        ')',
        '',
        regex=False
    ).str.split(
        ",",
        expand=True
    )

    ret_df.drop(
        columns=['lat_lon'],
        inplace=True
    )

    ret_df.rename(
        columns={
            'id': id_column
        },
        inplace=True
    )

    ret_df['lon'] = ret_df['lon'].replace('None', 0)
    ret_df['lon'] = ret_df['lon'].fillna(0)
    ret_df['lat'] = ret_df['lat'].replace('None', 0)
    ret_df['lat'] = ret_df['lat'].fillna(0)
    ret_df = ret_df.astype({'lon': 'float', 'lat': 'float'})

    return ret_df


def clean_and_geocode_dim_locations(dimloc_df):
    uniq_address_dimloc_df = dimloc_df.drop_duplicates(subset=['STREET', 'CITY', 'STATE', 'ZIP5']).copy().reset_index()
    uniq_address_dimloc_df = uniq_address_dimloc_df[['index', 'STREET', 'CITY', 'STATE', 'ZIP5']].copy()

    geocoded_uniq_address_dimloc_df = geocode_df(uniq_address_dimloc_df, 'index', 'STREET',
                                                 'CITY', 'STATE', 'ZIP5')
    geocoded_uniq_address_dimloc_df = geocoded_uniq_address_dimloc_df[['index', 'lat',
                                                                       'lon', 'type', 'accuracy']].copy()
    geocoded_uniq_address_dimloc_df.rename({'lat': 'LATITUDE',
                                            'lon': 'LONGITUDE',
                                            'type': 'GEOCODE_LEVEL',
                                            'accuracy': 'GEOCODE_ACCURACY'},
                                           axis='columns', inplace=True)

    uniq_address_dimloc_df = uniq_address_dimloc_df.merge(geocoded_uniq_address_dimloc_df, how='left', on='index')
    geocoded_dimloc_df = dimloc_df.merge(uniq_address_dimloc_df, how='inner', on=['STREET', 'CITY', 'STATE', 'ZIP5'])
    geocoded_dimloc_df.drop_duplicates(subset='ID', inplace=True)

    return geocoded_dimloc_df
