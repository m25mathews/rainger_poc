import os
from typing import Union

from .base import ScopeBase

KST_SCHEMA = "KEEPSTOCK"+os.getenv("RUN_SCHEMA_NAME", '')
DIM_LOCATION = "DIM_LOCATION_KEEPSTOCK"
FCT_PROGRAM = "FCT_PROGRAM_KEEPSTOCK"

class ScopeKeepstockAccount(ScopeBase):

    def __init__(
        self,
        accounts: Union[list, str] = None,
        incremental: bool = True,
    ):
        if accounts is None:
            raise ValueError
        
        if isinstance(accounts, str):
            accounts = [accounts]

        self.accounts = accounts
        self.incremental = incremental

    def _dim_query(self):
        accounts = ",".join(self.accounts)
        return f"""select distinct
            dl.ID,
            dl.ADDRESS1,
            dl.CITY,
            dl.PROVINCE,
            dl.ZIP5,
            fp.CUSTOMER_ACCOUNT as ACCOUNT
        from {KST_SCHEMA}.{DIM_LOCATION} dl
        left join {KST_SCHEMA}.{FCT_PROGRAM} fp
            on dl.id = fp.dim_location_id
        where ACCOUNT in ({accounts})
        {'and OPS_LOCATION_ID is NULL' if self.incremental else ''}
        """

    def _ops_query(self):
        accounts = ",".join(self.accounts)
        return f"""select distinct
            ol.ID,
            ol.OPS_STREET,
            ol.OPS_SUBLOCATION,
            ol.OPS_CITY,
            ol.OPS_STATE,
            ol.OPS_ZIP5,
            oa.ACCOUNT::varchar as ACCOUNT
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION ol
        left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
            on oa.ORGANIZATION_ID::varchar = ol.ORGANIZATION_ID
        where ACCOUNT in ({accounts})
        """

    @staticmethod
    def _dim_size_query(between: tuple = None, incremental: bool = True):
        btw_clause = null_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"

        if incremental:
            null_clause = "where OPS_LOCATION_ID is NULL"
    
        return f"""select
                fp.CUSTOMER_ACCOUNT as ACCOUNT,
                COUNT(distinct dl.id) as SIZE
            from {KST_SCHEMA}.{DIM_LOCATION} dl
            left join {KST_SCHEMA}.{FCT_PROGRAM} fp
                on dl.id = fp.dim_location_id
            {null_clause}
            group by 1
            {btw_clause}
            order by 2 desc
        """

    @staticmethod
    def _ops_size_query(between: tuple = None, incremental: bool = False):
        btw_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"
        
        return f"""select
            oa.ACCOUNT as ACCOUNT,
            COUNT(*) as SIZE
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION ol
        left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
            on ol.ORGANIZATION_ID = oa.ORGANIZATION_ID
        group by 1
        {btw_clause}
        order by 2 desc
        """
