import os
from uuid import uuid1
import pandas as pd

import persistence
import environment

from client.pingclient import PingClient
from client.svsclient_oauth import SvsClient_OAuth
from client.upsclient import UpsClient

from loggers import get_logger, timer


logger = get_logger("FLAG-RESIDENTIAL")


@timer(logger)
def flag_residential_df(df: pd.DataFrame) -> pd.DataFrame:
    """Check for residential ops locations in a dataframe

    Args:
        df (DataFrame): contains OPS rows

    Returns:
        DataFrame: OSP rows with is_residential flagged
    """
    if 'ID' in df:
        df.index = df['ID']
    else:
        df['ID'] = df.index

    df['name1'] = ''
    df['streetaddress2'] = ''
    df["row_index_id"] = df["ID"]

    temp_names = {
        "OPS_STREET": "streetaddress",
        "OPS_CITY": "city",
        "OPS_STATE": "state",
        "OPS_ZIP5": "zipcode"
    }

    df = df.rename(columns=temp_names)

    cfg = environment.read()
    ups_client = UpsClient(password=cfg['ups_password'])
    access_token = PingClient(cfg['client_id'], cfg['client_secret'], cfg['ping_url']).login()
    svs_client = SvsClient_OAuth(token=access_token, env=cfg['env'])

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

    df.loc[((df['svs_rdi'] == 'R') | (df['ups_rdi'] == 'Residential')), "IS_RESIDENTIAL"] = True
    df["IS_RESIDENTIAL"] = df["IS_RESIDENTIAL"].fillna(False)

    return df.rename(columns={v: k for k, v in temp_names.items()})


def flag_residential():

    """ Query from location table to create location hierarchy """
    """ Return dictionary with key as main_loc_name, and value the list of locations"""

    sql = f"""SELECT id,ops_street,ops_city,ops_state,ops_zip5 FROM CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location"""
    df = persistence.get_df(sql)

    df = flag_residential_df(df)

    residential_list = []
    for i, _row in df.iterrows():
        if _row['is_residential'] == True:
            residential_list.append(_row['row_index_id'])

    logger.info('Total rows with is_residential=true ' + str(len(residential_list)))

    update_residential(residential_list)


def update_residential(residential_list):

    # Set all rows to false
    conn = persistence.get_conn()
    try:
        sql = f'''update CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION set is_residential = false'''
        conn.cursor().execute(sql)
    except Exception as error:
        logger.error(error)

    # update 100 rows in a batch
    sub_list = []
    for index in range(len(residential_list)):
        sub_list.append(residential_list[index])
        if index % 100 == 0:
            try:
                id_list_str = str(sub_list).replace('[', '(')
                id_list_str = id_list_str.replace(']', ')')
                sql = f"""UPDATE CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location SET is_residential=1 WHERE ID in """ + id_list_str
                conn.cursor().execute(sql)
            except Exception as error:
                logger.error(error)
            sub_list = []

    # update remaining
    if len(sub_list) > 0:
        id_list_str = str(sub_list).replace('[', '(')
        id_list_str = id_list_str.replace(']', ')')
        try:
            sql = f"""UPDATE CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location SET is_residential=1 WHERE ID in """ + id_list_str
            logger.debug(sql)
            conn.cursor().execute(sql)
        except Exception as error:
            logger.error(error)

    conn.close()


if __name__ == "__main__":
    flag_residential()
    logger.info('flag_residence completed')



