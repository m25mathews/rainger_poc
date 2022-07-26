import os

import pandas as pd
import hashlib
from uuid import uuid1

import persistence
import query
from loggers import get_logger, timer


logger = get_logger("IMPORT-OPS")

OPS_LOCATIONS_PATH = '../../data/curated/locations/'

RESET_OPS_LOCATION_SQL = [
    f'''truncate CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION;''',
    f'''truncate CIM{os.getenv("RUN_SCHEMA_NAME", '')}.BRG_LOCATION;''',
    f'''update SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION_SALES_ORDER set ops_location_id = null;''',
    f'''update KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION_KEEPSTOCK set ops_location_id = null;'''
]

RESET_SOLDTO_OPS_LOCATION_SQL = [
    f'''truncate CIM{os.getenv("RUN_SCHEMA_NAME", '')}.SOLDTO_LOCATION;''',
    f'''update SOLDTO_ACCOUNT{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION_SOLDTO set OPS_LOCATION_ID = NULL;'''
]

RESET_OPS_ACCOUNT_SQL = [
    f'''truncate CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT;''',
]


def reset_ops_locations(organization: str = None):
    if organization is None:
        return reset_ops(RESET_OPS_LOCATION_SQL)
    return reset_ops([
        query.delete_dim_location_ops_id_for_org(organization),
        query.delete_ops_locations_for_org(organization),
    ])


def reset_ops_soldto_locations(organization: str = None):
    if organization is None:
        return reset_ops(RESET_SOLDTO_OPS_LOCATION_SQL)
    else:
        raise NotImplementedError


@timer(logger)
def reset_ops(sql_statements):
    with persistence.get_conn() as conn:
        logger.info('Cleaning and preparing')
        for sql in sql_statements:
            with conn.cursor() as cur:
                logger.debug('Executing SQL: ', sql)
                cur.execute(sql)
        logger.info('Ready for upload')


def generate_ops_location_id_from_hash(row: pd.Series):
    row = row.fillna('')
    hash_str = ' '.join([
        row.OPS_STREET,
        row.OPS_SUBLOCATION,
        row.OPS_CITY,
        row.OPS_STATE,
        row.OPS_ZIP5,
        row.ORGANIZATION_NAME,
    ])

    hash_obj = hashlib.sha256(bytes(hash_str, "utf-8"))
    return hash_obj.hexdigest()

def generate_ops_location_id(row: pd.Series):
    return str(uuid1())

def generate_ops_location_ids(ops_df: pd.DataFrame):
    return ops_df.apply(generate_ops_location_id, axis=1)
