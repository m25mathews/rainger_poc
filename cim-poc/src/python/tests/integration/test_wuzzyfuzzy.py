from multiprocessing import Pool, freeze_support

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

from loggers import get_logger

logger = get_logger("TEST_WUZZYFUZZY")

OPS_STR = [
    'Caterpillar Mossville Technical Center Building G 14009 N Old Galena Rd Bldg G Mossville IL 61552',
    '501 SW Jefferson Ave Peoria IL 61605'
]

DIM_STR_S = [
    '14009 N Old Galena Rd Bldg G Mossville IL 61552',
    'Caterpillar Peoria Building LC 501 SW Jefferson Ave Peoria IL 61605'
]


df = pd.DataFrame(list(zip(OPS_STR, DIM_STR_S)), columns=['OPS_STR', 'DIM_STR_S'])


def match(df):
    df['FUZZY_MATCH'] = df.apply(lambda r: fuzz.token_set_ratio(r.DIM_STR_S, r.OPS_STR), axis=1)
    # df['SECOND_MATCH'] = df.apply(lambda r: matcher.marker_match(r.OPS_STR, r.DIM_STR_S), axis=1)
    return df


def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


if __name__ == '__main__':
    freeze_support()
    # df = parallelize_dataframe(df, match, cpu_count())
    df = match(df)
    logger.info(df.head(10))