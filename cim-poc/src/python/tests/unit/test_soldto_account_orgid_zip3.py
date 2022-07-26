import os

from scopes import soldto_account_orgid_zip3 as sa
from scopes.base import MAGIC_SEPARATOR

ST_SCHEMA = "SOLDTO_ACCOUNT"
DIM_LOCATION = "DIM_LOCATION_SOLDTO"
FCT_ACCOUNT = "FCT_ACCOUNT_SOLDTO"
ACTIVE_ACCOUNT_YEAR = 2017

incremental = "True"
orgids, zip3s, orgzips = ['1234'],['789'],'1234@$!?789'

expected_sql = f"""SELECT DISTINCT
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
        where OPS.ORGANIZATION_ID in ('{orgids[0]}') -- more efficient subset
        and LEFT(DIM.ZIP5, 3) in ('{zip3s[0]}')
        and CONCAT(OPS.ORGANIZATION_ID, '{MAGIC_SEPARATOR}', LEFT(DIM.ZIP5, 3)) in ('{orgzips}')
        and FCT.LAST_BILL_DATE is not NULL
        and FCT.LAST_BILL_DATE <> ''
        and YEAR(FCT.LAST_BILL_DATE::DATE) >= {ACTIVE_ACCOUNT_YEAR}
        {'and OPS_LOCATION_ID is NULL' if incremental else ''}
        """


def test_dim_query():
    sao = sa.ScopeSoldToAccountOrgIdZip3(orgids, zip3s)
    result = sao._dim_query()
    assert result == expected_sql




