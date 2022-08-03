import os
from typing import List

import pandas as pd
from pandas import Series

import persistence

from loggers import get_logger, timer

logger = get_logger("POPULATE_BRIDGE_LOCATION")


def truncate_table():
    with persistence.get_conn() as conn:
        sql = f"""truncate CIM{os.getenv("RUN_SCHEMA_NAME", '')}.BRG_LOCATION"""
        logger.info(f"""Truncating table CIM{os.getenv("RUN_SCHEMA_NAME", '')}.BRG_LOCATION""")
        with conn.cursor() as cur:
            cur.execute(sql)


def insert_stand_alone():
    with persistence.get_conn() as conn:
        sql = f""" INSERT INTO CIM{os.getenv("RUN_SCHEMA_NAME", '')}.brg_location (location_id_p, location_id_c, levels_removed, \
                       parent_is_top, parent_is_bottom, child_is_top,  child_is_bottom) \
                   SELECT id, id, 0, true, true, true, true \
            FROM   CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location \
            WHERE  main_loc_name IS NULL \
            """
        logger.info(sql)
        with conn.cursor() as cur:
            cur.execute(sql)


def get_location():

    truncate_table()

    """ Query from location table to create location hierarchy """
    """ Return dictionary with key as main_loc_name, and value the list of locations"""

    sql = f"""SELECT ID, MAIN_LOC_NAME, OPS_LOC_NAME FROM CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location WHERE MAIN_LOC_NAME IS NOT NULL"""
    df = persistence.get_df(sql)

    loc_dict = {}

    for index, row in df.iterrows():
        if loc_dict.get(row['MAIN_LOC_NAME']) is None:
            loc_dict[row['MAIN_LOC_NAME']] = []
        if row['MAIN_LOC_NAME'] is not None:
            loc_dict.get(row['MAIN_LOC_NAME']).append(row)

    bridge_df = pd.DataFrame(data=[], columns=['LOCATION_ID_P',
                                               'LOCATION_ID_C',
                                               'LEVELS_REMOVED',
                                               'PARENT_IS_TOP',
                                               'PARENT_IS_BOTTOM',
                                               'CHILD_IS_TOP',
                                               'CHILD_IS_BOTTOM'])

    for key in loc_dict.keys():
        to_append_list = find_main_loc(key, loc_dict[key])
        for item in to_append_list:
            bridge_df.loc[len(bridge_df)] = item

    logger.info(bridge_df.info())

    if bridge_df is not None and bridge_df.shape[0] > 0:
        persistence.insert_to_db(bridge_df, 'BRG_LOCATION')

    insert_stand_alone()

    return bridge_df


def find_main_loc(main_loc_name: str, location_cluster: List[Series]):
    parent_id = None
    for item in location_cluster:
        if item['MAIN_LOC_NAME'] == main_loc_name:
            parent_id = item['ID']
            break

    bridge_list = []
    for item in location_cluster:
        if parent_id != item['ID']:
            bridge_list.append([item['ID'], item['ID'], 0, False, True, False, True])
        else:
            bridge_list.append([item['ID'], item['ID'], 0, True, False, True, False])

    for item in location_cluster:
        if parent_id != item['ID']:
            bridge_list.append([parent_id, item['ID'], 1, True, False, False, True])

    return bridge_list


def main():
    get_location()


if __name__ == "__main__":
    main()