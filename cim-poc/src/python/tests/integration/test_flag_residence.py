from uuid import uuid1

import pandas as pd

import environment
import persistence
from client.pingclient import PingClient
from client.svsclient_oauth import SvsClient_OAuth
from client.upsclient import UpsClient

from loggers import get_logger

logger = get_logger("TEST_FLAG_RESIDENCE")

def test_cfg():
    cfg = environment.read()
    logger.info(cfg)
    return cfg


def test_ups_client():
    ups_client = UpsClient(password='Pump#It#Up!')

    data = {
      "name1": ['Caterpillar', ''],
      "streetaddress": ['100 NE Adams St', '1528 E Olive St'],
      "streetaddress2": ['', ''],
      "city": ['Peoria', 'Palatine'],
      "state": ['IL', 'IL'],
      "zipcode": ['61629', '60074']
    }
    df = pd.DataFrame(data)
    df = ups_client.get_UPS_RDI_async(df)
    logger.info(df)
    return df


def test_svs_client():
    ping_url = 'https://pingf.grainger.com'
    client_id = 'mim_ui'
    client_secret = 'HL5qPFJ01evmdAZGJxa3MdY4q4vykmBf6pXllD4hIfAIvaMNZTj5OtifCo6tErv9'
    environment = 'prod'
    access_token = PingClient(client_id, client_secret, ping_url).login()
    svs_client = SvsClient_OAuth(token=access_token, env=environment)
    data = {
        "name1": ['Caterpillar', ''],
        "streetaddress": ['100 NE Adams St', '1528 E Olive St'],
        "streetaddress2": ['', ''],
        "city": ['Peoria', 'Palatine'],
        "state": ['IL', 'IL'],
        "zipcode": ['61629', '60074']
    }
    df = pd.DataFrame(data)

    for i, _row in df.iterrows():
        df.at[i, "id"] = uuid1()

    df = svs_client.validate_address_batch(
        df,
        column_companyName="name1",
        column_streetname="streetaddress",
        column_streetname2="streetaddress2",
        column_city="city",
        column_state="state",
        column_zipcode="zipcode"
    )
    logger.info(df)
    return df


def test_is_credential():
    cfg = test_cfg()
    ups_client = UpsClient(password=cfg['ups_password'])
    access_token = PingClient(cfg['client_id'], cfg['client_secret'], cfg['ping_url']).login()
    svs_client = SvsClient_OAuth(token=access_token, env=cfg['env'])

    data = {
        "name1": ['RICHARDSON INC', ''],
        "streetaddress": ['21141 State Highway 59', '1528 E Olive St'],
        "streetaddress2": ['', ''],
        "city": ['Robertsdale', 'Palatine'],
        "state": ['AL', 'IL'],
        "zipcode": ['36567', '60074']
    }
    df = pd.DataFrame(data)
    df = ups_client.get_UPS_RDI_async(df)

    for i, _row in df.iterrows():
        df.at[i, "id"] = uuid1()

    df = svs_client.validate_address_batch(
        df,
        column_companyName="name1",
        column_streetname="streetaddress",
        column_streetname2="streetaddress2",
        column_city="city",
        column_state="state",
        column_zipcode="zipcode"
    )

    logger.info(df)

    df.loc[((df['svs_rdi'] == 'R') & (df['ups_rdi'] == 'Residential')), "is_residential"] = True

    logger.info(df)

    for i, _row in df.iterrows():
        logger.info('-------' + str(i))
        # print(df['svs_rdi'])
        # print(df['ups_rdi'])
        logger.info(df['is_residential'])
        # print(df['sequester_reason'])
        # print(df['svs_rdi']=='R')
        # print(df['ups_rdi']=='Residential')
        # print((df['svs_rdi']=='R') & (df['ups_rdi']=='Residential'))
        #
        # print(df.loc[((df['svs_rdi'] == 'R') & (df['ups_rdi'] == 'Residential')), "is_residental"])


    # df['is_residential'] = df.apply(lambda r: is_residential(r.svs_rdi, r.ups_rdi), axis=1)
    #
    # df.loc[((df['svs_rdi'] == 'R') & (df['ups_rdi'] == 'Residential')), 'sequester_reason'] = 'Residential'


if __name__ == '__main__':
    # test_cfg()
    # test_ups_client()
    # test_svs_client()
    test_is_credential()