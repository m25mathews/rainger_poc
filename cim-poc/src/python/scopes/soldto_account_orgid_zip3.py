import os
from typing import Union

from .base import MAGIC_SEPARATOR, ScopeBase

ST_SCHEMA = "SOLDTO_ACCOUNT"+os.getenv("RUN_SCHEMA_NAME", '')
DIM_LOCATION = "DIM_LOCATION_SOLDTO"
FCT_ACCOUNT = "FCT_ACCOUNT_SOLDTO"
ACTIVE_ACCOUNT_YEAR = 2017

class ScopeSoldToAccountOrgIdZip3(ScopeBase):

    def __init__(
        self,
        organization_ids: Union[list, str],
        zip3s: Union[list, str],
        incremental: bool = True,
    ):
        if organization_ids is None or zip3s is None:
            raise ValueError

        if isinstance(organization_ids, str):
            organization_ids = [organization_ids]
        if isinstance(zip3s, str):
            zip3s = zip3s

        if len(organization_ids) != len(zip3s):
            raise ValueError("Organizations and Zip3s must be the same length!")

        self.organization_ids = organization_ids
        self.zip3s = zip3s
        self.incremental = incremental

    def _prepare_where_clauses(self):
        orgids = ','.join(f"'{orgid}'" for orgid in self.organization_ids)
        zip3s = ','.join(f"'{zip3}'" for zip3 in self.zip3s)
        orgzips = ','.join(f"'{org}{MAGIC_SEPARATOR}{zip3}'" for org, zip3 in zip(self.organization_ids, self.zip3s))
        return orgids, zip3s, orgzips

    def _dim_query(self):
        orgids, zip3s, orgzips = self._prepare_where_clauses()
        return f"""SELECT DISTINCT
            NULL AS STREET_NUM,
            NULL AS DEPARTMENT,
            NULL AS ATTENTION,
            NULL AS SUPPLEMENTAL,
            NULL AS RECEIVER,
            TRIM(DIM.ID) AS ID,
            TRIM(DIM.STREET) AS STREET,
            TRIM(DIM.CITY) AS CITY,
            TRIM(DIM.REGION) AS STATE,
            TRIM(DIM.ZIP5) AS ZIP5,
            TRIM(OPS.ORGANIZATION) AS ORGANIZATION ,
            OPS.ORGANIZATION_ID AS ORGANIZATION_ID
        FROM {ST_SCHEMA}.{DIM_LOCATION} DIM
        JOIN {ST_SCHEMA}.{FCT_ACCOUNT} FCT
        ON DIM.id = FCT.dim_location_id
        JOIN CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT OPS
        ON FCT.account = OPS.account
        where OPS.ORGANIZATION_ID in ({orgids}) -- more efficient subset
        and LEFT(DIM.ZIP5, 3) in ({zip3s})
        and CONCAT(OPS.ORGANIZATION_ID, '{MAGIC_SEPARATOR}', LEFT(DIM.ZIP5, 3)) in ({orgzips})
        and FCT.LAST_BILL_DATE is not NULL
        and FCT.LAST_BILL_DATE <> ''
        and YEAR(FCT.LAST_BILL_DATE::DATE) >= {ACTIVE_ACCOUNT_YEAR}
        {'and OPS_LOCATION_ID is NULL' if self.incremental else ''}
        """

    def _ops_query(self):
        orgids, zip3s, orgzips = self._prepare_where_clauses()
        return f"""SELECT
            *
        FROM CIM{os.getenv("RUN_SCHEMA_NAME", '')}.SOLDTO_LOCATION
        where ORGANIZATION_ID in ({orgids}) -- more efficient subset
        and LEFT(OPS_ZIP5, 3) in ({zip3s})
        and CONCAT(ORGANIZATION_ID, '{MAGIC_SEPARATOR}', LEFT(OPS_ZIP5, 3)) in ({orgzips})
        """

    @staticmethod
    def _dim_size_query(between: tuple = None, incremental: bool = False):
        btw_clause = null_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"

        if incremental:
            null_clause = "and OPS_LOCATION_ID is NULL"

        return f"""select 
            ORGANIZATION_ID as ORGANIZATION_ID,
            LEFT(ZIP5, 3) AS ZIP3,
            COUNT(distinct DIM.ID) AS SIZE
        FROM {ST_SCHEMA}.{DIM_LOCATION} DIM
        JOIN {ST_SCHEMA}.{FCT_ACCOUNT} FCT
        ON DIM.id = FCT.dim_location_id
        JOIN cim{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT OPS
        ON FCT.account = OPS.account
        where FCT.LAST_BILL_DATE is not NULL
        and FCT.LAST_BILL_DATE <> ''
        and YEAR(FCT.LAST_BILL_DATE::DATE) >= {ACTIVE_ACCOUNT_YEAR}
        {null_clause}
        group by 1, 2
        {btw_clause}
        order by 3 desc
        """

    @staticmethod
    def _ops_size_query(between: tuple = None, incremental: bool = False):
        btw_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"
        
        return f"""select
            ORGANIZATION_ID,
            LEFT(OPS_ZIP5, 3) as ZIP3,
            COUNT(*) as SIZE
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.SOLDTO_LOCATION
        group by 1, 2
        {btw_clause}
        order by 3 desc
        """
