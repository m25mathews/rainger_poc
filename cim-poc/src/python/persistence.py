import os
import urllib
import snowflake.connector as sfc
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine

import environment

from loggers import get_logger, timer

logger = get_logger("PERSISTENCE")
static_connection = None


def get_conn():
    global static_connection
    logger.debug('Connecting to the Snowflake database...')
    params = environment.read()
    if static_connection is None or static_connection.is_closed():
        static_connection = sfc.connect(**params)

    return static_connection


def engine(cfg=None, schema=None):
    """
    For use with a SQL Alchemy engine
    :param cfg: Override, will default
    :param schema: Override, otherwise will read from config
    :return: SQLAlchemy engine
    """
    if cfg is None:
        cfg = environment.read()

    result = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=cfg['user'],
            password=urllib.parse.quote(cfg['password']),
            account=cfg['account'],
            database=cfg['database'],
            schema=cfg['schema'] if schema is None else schema,
            warehouse=cfg['warehouse']
        )
    )
    return result


@timer(logger)
def get_df(sql):
    conn = None
    try:
        conn = get_conn()

        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetch_pandas_all()
            return result

    except Exception as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()
            logger.debug('Database connection closed.')


@timer(logger)
def update_df(df, func):
    conn = None
    try:
        conn = get_conn()

        with conn.cursor() as cur:
            df.apply(lambda r: func(r, cur), axis=1)

    except Exception as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()
            logger.debug('Database connection closed.')


@timer(logger)
def insert_to_db(df, table_name, truncate_table: bool = False):
    conn = None
    try:
        conn = get_conn()

        if truncate_table:
            conn.cursor().execute(f"TRUNCATE TABLE CIM"+os.getenv("RUN_SCHEMA_NAME", '')+"." + table_name)

        success, n_chunks, n_rows, _ = write_pandas(conn=conn,
                                                    df=df,
                                                    schema='CIM'+os.getenv("RUN_SCHEMA_NAME", ''),
                                                    table_name=table_name,
                                                    quote_identifiers=False)
        logger.debug("Insert table {0} succeed, total {1} rows inserted".format(table_name, n_rows))
    except Exception as error:
        logger.error(error)
        logger.error(df.head())
        print(df.head())
    finally:
        if conn is not None:
            conn.close()
            logger.debug('Database connection closed.')


@timer(logger)
def upload_df(df, schema="TEMP", table="STG_LOCATION_ASS", with_full_schema=False):
    if(with_full_schema):
        full_schema=schema
    else:
        full_schema=schema+os.getenv("RUN_SCHEMA_NAME", '')
    with get_conn() as conn:
        write_pandas(
            conn,
            df,
            table_name=table,
            schema=full_schema,
            quote_identifiers=False,
        )


@timer(logger)
def truncate_table(schema, table):
    schema_with_run = schema + os.getenv("RUN_SCHEMA_NAME", '')
    with get_conn() as conn:
        with conn.cursor() as cur:
            sql = f'''truncate {schema_with_run}.{table};'''
            logger.info(f'''Truncating {schema_with_run}.{table}''')
            cur.execute(sql)
