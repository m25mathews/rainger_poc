import os

import environment
import persistence
from geocodio import GeocodioClient
import geocode
import query
from scopes import ScopeSalesOrderOrgIdState
from curation_wizard import CurationSalesOrder


from loggers import get_logger

logger = get_logger("TEST_GEOCODIO")

def dim_location_scope(state: str, organization: str, limit: int = 10) -> str:
    sql = f"""select
        dl.ID,
        dl.STREET, 
        dl.CITY, 
        dl.STATE,
        dl.ZIP5, 
        dl.COUNTRY,
        oo.ORGANIZATION_NAME,
        oo.ID as ORGANIZATION_ID
    from cido.SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    inner join cido.CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa on oa.ACCOUNT = dl.SOLD_ACCOUNT 
    left join cido.CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION oo on oa.ORGANIZATION_ID = oo.ID
    where 
        STATE = '{state}'
        and oa.ORGANIZATION = '{query.escape(organization)}' 
    LIMIT {limit}"""

    logger.info(sql)
    return sql


def test_geocode_connection():
    env = environment.read()
    apikey = env['apikey']
    logger.info(apikey)
    client = GeocodioClient(apikey)

    list = [
        '100 grainger parkway, lake forecast, il 60045',
        '1600 pennsylvania avenue NW, washington, dc 20500',
        '131 E Selma St Ste 1, Dothan, AL 363013692',
        '2205 CEDAR STREET, ROLLING MEADOWS, IL 60008'
    ]

    results = client.batch_geocode(list)
    for r in results:
        if r is not None:
            logger.info(r.get('results')[0].get('location'))


def test_geoclient():
    df = persistence.get_df(dim_location_scope('MS', 'CATERPILLAR', limit=12415))
    logger.info(df.info())
    logger.info(df.head())
    df_geo = geocode.geocode_df(df, "ID", "STREET", "CITY", "STATE", "ZIP5", "ORGANIZATION_NAME")
    logger.info('Results dataframe shape={0}'.format(df_geo.shape))
    logger.info(df_geo.head())


def test_cache():
    organization_ids = ["61bffce53c7864ae782ff798fac4de46", "23f80fc84cbce7c18218eac5d7c5dd05"]
    states = ["TX", "SC"]
    scope = ScopeSalesOrderOrgIdState(organization_ids = organization_ids, states = states)
    wizard = CurationSalesOrder(scope)
    df = wizard.autocurate()
    print(df.info())
    print(df.head())


if __name__ == '__main__':
    # test_geocode_connection()
    # test_geoclient()
    test_cache()
