import uuid
import os
from abc import abstractmethod
import persistence
from loggers import get_logger, timer

logger = get_logger("UploadDFBase")


class UploadDFBase:

    def __init__(self):
        self.temp_table_name = self.get_temp_table_name_prefix() + uuid.uuid4().hex
        self.ops_columns = list(self.get_table_column_type_dict().keys())

        """Create temp table with random name"""
        sql = "CREATE TRANSIENT TABLE {}.{}".format(self.get_temp_table_schema()+os.getenv("RUN_SCHEMA_NAME", ''), self.temp_table_name)
        sql += "("
        sql += ",".join('{} {}'.format(*p) for p in self.get_table_column_type_dict().items())
        sql += ")"

        with persistence.get_conn() as conn:
            # create temp table
            logger.info(sql)
            conn.cursor().execute(sql)

    def __del__(self):
        with persistence.get_conn() as conn:
            # drop temp table
            drop_temp_table = "DROP TABLE {}.{}".format(self.get_temp_table_schema()+os.getenv("RUN_SCHEMA_NAME", ''), self.temp_table_name)
            logger.info(drop_temp_table)
            conn.cursor().execute(drop_temp_table)

    @abstractmethod
    def get_temp_table_name_prefix(self) -> str:
        pass

    @abstractmethod
    def get_destination_table_name(self) -> str:
        pass

    @abstractmethod
    def get_table_column_type_dict(self) -> dict:
        pass

    def get_temp_table_schema(self) -> str:
        return "TEMP"

    def get_destination_table_schema(self) -> str:
        return "CIM"

    @timer(logger)
    def merge_ops_locations(self):
        """ To avoid duplicated records, first load to temporary table,
            then get the difference between temp table and destination table, and then insert
        """
        column_list = self.ops_columns.copy()
        # column_list.remove("ID")


        sql = "INSERT INTO {}.{}".format(self.get_destination_table_schema()+os.getenv("RUN_SCHEMA_NAME", ''),
                                         self.get_destination_table_name()) + "(" + ",".join(column_list) + ") " \
              "SELECT " + ",".join(column_list) + " FROM {}.{} ".format(self.get_temp_table_schema()+os.getenv("RUN_SCHEMA_NAME", ''),
                                                                        self.temp_table_name) + \
              " MINUS " \
              "SELECT " + ",".join(column_list) + " FROM {}.{}".format(self.get_destination_table_schema()+os.getenv("RUN_SCHEMA_NAME", ''),
                                                                       self.get_destination_table_name())

        with persistence.get_conn() as conn:
            # insert records into ops_location that don't yet exist in ops_location
            conn.cursor().execute(sql)

    @timer(logger)
    def upload_ops_location_preload_temp_table(self, ops_df):
        """ load to temp table first, and then compare with ops_location to remove duplicates"""
        persistence.upload_df(
            ops_df[self.ops_columns],
            schema=self.get_temp_table_schema(),
            table=self.temp_table_name
        )

