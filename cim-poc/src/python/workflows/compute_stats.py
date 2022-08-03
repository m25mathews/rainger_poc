from utils import mother_query
import persistence
from loggers import get_logger

logger = get_logger("METASTORE")


def compute_stats_table():
    sql = " INSERT INTO METASTORE.RUN_METRICS(CATEGORY, ENTITY, FULLNAME, METRIC, RESULT, INMILLIONS, RUN_ID) "
    sql += mother_query.get_insert_sql()

    # print(sql)

    conn = None
    try:
        conn = persistence.get_conn()
        conn.cursor().execute(sql)
        logger.info("metastore.run_metric inserted / updated")
    except Exception as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()
            logger.debug('Database connection closed.')


if __name__ == '__main__':
    compute_stats_table()