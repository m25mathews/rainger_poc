import pandas as pd

import persistence

from loggers import get_logger

logger = get_logger("UPLOAD_SELLER")

df = pd.read_csv('Seller_Account_Catterpillar.csv')

logger.info(df.info())

persistence.upload_df(
    df,
    table='seller_assignment',
    schema='cim'
)


