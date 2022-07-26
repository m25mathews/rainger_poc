import os

import environment


def escape(organization: str):
    return organization.replace("'", "\\'")

def dim_location_scope(state: str, organization: str) -> str:
    return f"""select 
        dl.ID,
        dl.SOLD_ACCOUNT, 
        dl.SHIP_ACCOUNT, 
        dl.TRACK_CODE,
        dl.SUB_TRACK_CODE, 
        dl.DEPARTMENT, 
        dl.ATTENTION, 
        dl.SUPPLEMENTAL,
        dl.RECEIVER, 
        dl.STREET_NUM, 
        dl.STREET, 
        dl.CITY, 
        dl.STATE,
        dl.ZIP5, 
        dl.COUNTRY,
        oo.ORGANIZATION_NAME,
        oo.ID as ORGANIZATION_ID
    from SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    inner join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ACCOUNT oa on oa.ACCOUNT = dl.SOLD_ACCOUNT 
    left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION oo on oa.ORGANIZATION_ID = oo.ID
    where 
        STATE = '{state}'
        and oa.ORGANIZATION_ID = {organization}
    order by STREET"""


def soldto_account_dim_location_scope() -> str:
    return f"""SELECT 
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
            OPS.ORGANIZATION_ID AS ORGANIZATION_ID,
            TRIM(FCT.ACCOUNT) AS SOLDTO_ACCOUNT,
            OPS.ID AS ACCOUNT_ID
        FROM soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.dim_location DIM
        JOIN soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.fct_account FCT
        ON DIM.id = FCT.dim_location_id
        JOIN CIM{os.getenv("RUN_SCHEMA_NAME", '')}.organization_soldto_account OPS
        ON FCT.account = OPS.account
        WHERE FCT.account NOT IN ( SELECT ACCOUNT FROM {environment.read()["invalid_accounts_table"]})
        """


def soldto_account_dim_location_scope_multiple(scopes) -> str:
    orgids = ','.join(str(org) for org, _ in scopes)
    zip3s = ','.join(f"'{zip3}'" for _, zip3 in scopes)
    orgzips = ','.join(f"'{org}@{zip3}'" for org, zip3 in scopes)
    return f"""SELECT 
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
            OPS.ORGANIZATION_ID AS ORGANIZATION_ID,
            TRIM(FCT.ACCOUNT) AS SOLDTO_ACCOUNT,
            OPS.ID AS ACCOUNT_ID
        FROM soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.dim_location DIM
        JOIN soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.fct_account FCT
        ON DIM.id = FCT.dim_location_id
        JOIN CIM{os.getenv("RUN_SCHEMA_NAME", '')}.organization_soldto_account OPS
        ON FCT.account = OPS.account
        where OPS.ORGANIZATION_ID in ({orgids}) -- more efficient subset
        and LEFT(DIM.ZIP5, 3) in ({zip3s})
        and CONCAT(OPS.ORGANIZATION_ID, '@', LEFT(DIM.ZIP5, 3)) in ({orgzips})
        """


def dim_location_scope_multiple(scopes: list) -> str:
    orgs = ','.join(str(org) for org, _ in scopes)
    states = ','.join(f"'{state}'" for _, state in scopes)
    orgstates = ','.join(f"'{org}@{state}'" for org, state in scopes)
    return f"""select 
        dl.ID,
        dl.SOLD_ACCOUNT, 
        dl.SHIP_ACCOUNT, 
        dl.TRACK_CODE,
        dl.SUB_TRACK_CODE, 
        dl.DEPARTMENT, 
        dl.ATTENTION, 
        dl.SUPPLEMENTAL,
        dl.RECEIVER, 
        dl.STREET_NUM, 
        dl.STREET, 
        dl.CITY, 
        dl.STATE,
        dl.ZIP5, 
        dl.COUNTRY,
        oo.ORGANIZATION_NAME,
        oo.ID as ORGANIZATION_ID
    from SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    inner join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa on oa.ACCOUNT = dl.SOLD_ACCOUNT 
    left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION oo on oa.ORGANIZATION_ID = oo.ID
    where oa.ORGANIZATION_ID in ({orgs}) -- more efficient subset
    and dl.STATE in ({states})
    and concat(oa.ORGANIZATION_ID, '@', dl.STATE) in ({orgstates})
    order by STREET"""


def dim_location_scope_stateless(organization: int) -> str:
    return f"""select 
        case when dl.STREET_NUM is null then dl.STREET else dl.STREET_NUM || ' ' || dl.STREET end as ops_loc_name,
        case when dl.STREET_NUM is null then dl.STREET else dl.STREET_NUM || ' ' || dl.STREET end as ops_street,
        dl.CITY as ops_city,
        dl.STATE as ops_state,
        dl.ZIP5 as ops_zip5
    from SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    inner join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
        on oa.ACCOUNT = dl.SOLD_ACCOUNT 
    where 
        oa.ORGANIZATION_ID = {organization}
    order by STREET"""

def ops_location_scope(state: str, organization: int) -> str:
    return f"""select *
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
    where STATE = '{state}'
        and ORGANIZATION_ID = {organization}
    """

def ops_location_nonsite_scope(state: str, organization: int) -> str:
    return ops_location_scope(state, organization) + " AND IS_SITE = FALSE"

def ops_location_all() -> str:
    return f"""select
        ID,
        OPS_STREET,
        OPS_SUBLOCATION,
        OPS_CITY,
        OPS_STATE,
        OPS_ZIP5
    from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
    """

def ops_location_with_accounts(accounts) -> str:
    accounts = ",".join(f"'{a}'" for a in accounts)
    return f"""select distinct
        ol.ID,
        ol.OPS_STREET,
        ol.OPS_SUBLOCATION,
        ol.OPS_CITY,
        ol.OPS_STATE,
        ol.OPS_ZIP5,
        oa.ACCOUNT::varchar as ACCOUNT
    from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION ol
    left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ACCOUNT oa
        on oa.ORGANIZATION_ID::varchar = ol.ORGANIZATION_ID
    where ACCOUNT in ({accounts})
    """

def ops_nonresidential_address_scope(state: str, organization: int) -> str:
    return ops_location_scope(state, organization) + " and IS_RESIDENTIAL = FALSE and IS_ADDRESS = TRUE"

def ops_location_scope_organizationless(state: str) -> str:
    return f"""select
        ID,
        ORGANIZATION_NAME,
        OPS_STREET,
        OPS_CITY,
        OPS_STATE,
        OPS_SUBLOCATION,
        OPS_ZIP5
    from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
    where OPS_STATE = '{state}';
    """

def ops_location_scope_state_zip3(state: str, zip3: str) -> str:
    return f"""select
        ID,
        ORGANIZATION_NAME,
        OPS_STREET,
        OPS_CITY,
        OPS_STATE,
        OPS_SUBLOCATION,
        OPS_ZIP5
    from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
    where OPS_STATE = '{state}'
        and left(OPS_ZIP5, 3) = '{zip3}'
    """.format(state, zip3)

def dnb_location_scope_state_zip3(state: str, zip3: str) -> str:
    return f"""select
        ID,
        PHYS_STRT_AD,
        PHYS_CTY,
        PHYS_ST_ABRV,
        left(PHYS_ZIP, 5) as PHYS_ZIP5
    from DNB{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION
    where PHYS_ST_ABRV = '{state}'
        and left(PHYS_ZIP, 3) = '{zip3}'
        and PHYS_STRT_AD is not NULL
        and PHYS_CTY is not NULL
        and PHYS_ST_ABRV is not NULL
        and PHYS_ZIP is not NULL
    """

def keepstock_location_all():
    return f"""select distinct
        dl.ID,
        dl.ADDRESS1,
        dl.CITY,
        dl.PROVINCE,
        dl.ZIP5,
        fp.CUSTOMER_ACCOUNT as ACCOUNT
    from KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    left join KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.FCT_PROGRAM fp
        on dl.id = fp.dim_location_id
    """

def keepstock_location_accounts(accounts):
    accounts = ",".join(f"'{a}'" for a in accounts)
    return f"""select distinct
        dl.ID,
        dl.ADDRESS1,
        dl.CITY,
        dl.PROVINCE,
        dl.ZIP5,
        fp.CUSTOMER_ACCOUNT as ACCOUNT
    from KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl
    left join KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.FCT_PROGRAM fp
        on dl.id = fp.dim_location_id
    where ACCOUNT in ({accounts})
    """

# TODO: standardize ops organization_id data type. it's a string 
def delete_dim_location_ops_id_for_org(organization: int) -> str:
    return f"""update SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION
        set ops_location_id = null
    where ops_location_id in (
        select id
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.location
        where organization_id = '{organization}';
    );
    """

def delete_ops_locations_for_org(organization: int) -> str: 
    return f"""delete from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
    where ORGANIZATION_ID = '{organization}';
    """

def dnb_scope_sizes() -> str:
    return f"""with ops as (
        select
            left(OPS_ZIP5, 3) as ZIP3,
            OPS_STATE, count(*) as ops_cnt
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
        group by 1, 2
    ), dnb as (
        select
            left(PHYS_ZIP, 3) as ZIP3,
            PHYS_ST_ABRV,
            count(*) as dnb_cnt
        from dnb{os.getenv("RUN_SCHEMA_NAME", '')}.dim_location
        where PHYS_ST_ABRV is not NULL 
        and PHYS_STRT_AD is not NULL
        group by 1, 2
    )
    select
        ops.ZIP3,
        ops.OPS_STATE,
        ops.ops_cnt * dnb.dnb_cnt as size
    from ops
    left join dnb
    on ops.ZIP3 = dnb.ZIP3
    and ops.OPS_STATE = dnb.PHYS_ST_ABRV;"""

def soldto_accounts_by_revenue() -> str:
    return f"""
    SELECT
        dloc.SOLD_ACCOUNT as soldto_account,
        trim(cust.customer_desc) AS ACCOUNT_NAME,
        trim(cust.name2) AS ACCOUNT_NAME2,
        trim(cust.street60) AS SOLDTO_ADDRESS,
        trim(cust.city_1) AS SOLDTO_CITY,
        cust.region AS SOLDTO_STATE,
        left(cust.postalcode, 5) AS SOLDTO_ZIP5,
        trim(cust.zzna_cd) AS SOLDTO_TRACK_CD,
        trim(cust.zzna_cd_desc) AS SOLDTO_TRACK_CD_NAME,
        trim(cust.zzna_scd) AS SOLDTO_SUBTRACK_CD,
        trim(cust.zzna_scd_desc) AS SOLDTO_SUBTRACK_CD_NAME,
        COUNT(DISTINCT fo.ORDER_NUM) AS num_orders,
        SUM(fo.SUBTOTAL_2) AS total_order_dollars,
        ROUND(SUM(fo.SUBTOTAL_2) / COUNT(DISTINCT fo.ORDER_num), 2) AS avg_order_value
    FROM sales_order{os.getenv("RUN_SCHEMA_NAME", '')}.fct_order fo
        INNER JOIN sales_order{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dloc
            ON fo.DIM_LOCATION_ID = dloc.id
        INNER JOIN CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT opsacct
            ON opsacct."ACCOUNT"  = dloc.SOLD_ACCOUNT 
        INNER JOIN analytics.base.customer_v cust
            ON opsacct."ACCOUNT" = cust.CUSTOMER
        INNER JOIN public.dim_date ddate
            ON fo.dim_date_id = ddate.id
        INNER JOIN analytics.sap.sales_order_v sov
            ON sov.s_ord_num = fo.order_num
                AND sov.s_ord_item = fo.order_item
    WHERE ORDER_NUM LIKE '1%%'
        AND SOLD_ACCOUNT NOT IN (SELECT ACCOUNT FROM {environment.read()["invalid_accounts_table"]})
        AND ddate.calendar_year = 2021
        AND sov.REASON_REJ IS NULL --no rejections
        AND sov.comp_code = '0300' --Grainger US orders
        AND sov.accnt_asgn IN ('01', '20') --This is "real" Business and will exclude intercompany, etc.
        AND fo.subtotal_2 >= 0
    GROUP BY
        opsacct.ACCOUNT_NAME, 
        dloc.sold_account,
        trim(cust.customer_desc),
        trim(cust.name2),
        trim(cust.street60),
        trim(cust.city_1),
        cust.region,
        left(cust.postalcode, 5),
        cust.zzna_cd,
        cust.zzna_cd_desc,
        cust.zzna_scd,
        cust.zzna_scd_desc
    HAVING total_order_dollars >= 0
    ORDER BY total_order_dollars DESC;"""


def sf_contacts_email_domain_frequency(accounts, excluded_domains) -> str:
    accounts = ",".join(f"'{a}'" for a in accounts)
    excluded_domains = ",".join(f"'{ed}'" for ed in excluded_domains)
    return f"""
    SELECT 
      a.sap_account_number__c as SOLDTO_ACCOUNT,
      split_part(lower(c.email), '@', 2) as EMAIL_DOMAIN,
      COUNT(split_part(lower(c.email), '@', 2)) as EMAIL_CT
      FROM analytics.salesforce.contact_v c
          INNER JOIN analytics.salesforce.account_v a
              ON a.id = c.account_id
      WHERE a.sap_account_number__c IN ({accounts})
        AND email_domain NOT IN ({excluded_domains})
      GROUP BY SOLDTO_ACCOUNT, EMAIL_DOMAIN
      ORDER BY EMAIL_CT DESC
    """
