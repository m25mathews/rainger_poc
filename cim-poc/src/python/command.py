import os
from argparse import ArgumentParser

import association_wizard
import environment
import persistence
from query import escape
from loggers import get_logger, timer

logger = get_logger("COMMAND")

def all_scopes(organization: int = None):
    org_clause = f"AND oa.ORGANIZATION = {organization}" if organization else ""

    sql = f"""
    with base as (
        select
            oa.ORGANIZATION_ID,
            dl.STATE,
            count(dl.STATE) as N_RECORDS
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
        inner join SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl on dl.SOLD_ACCOUNT = oa.ACCOUNT
        where oa.account not in ( 
            SELECT ACCOUNT FROM {environment.read()["invalid_accounts_table"]}
        )
        {org_clause}
        group by 1, 2
        order by oa.ORGANIZATION_ID, dl.STATE
    )
    select distinct
        ORGANIZATION_ID,
        STATE
    from base
    """

    df = persistence.get_df(sql.format(""))

    return list(df.itertuples(index=False, name=None))

def all_scopes_between(min_dims, max_dims):
    sql = f"""
    with base as (
        select
            oa.ORGANIZATION_ID,
            dl.STATE,
            count(*) as N_RECORDS
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
        inner join SALES_ORDER{os.getenv("RUN_SCHEMA_NAME", '')}.DIM_LOCATION dl on dl.SOLD_ACCOUNT = oa.ACCOUNT
        where oa.account not in (
            SELECT ACCOUNT FROM {environment.read()["invalid_accounts_table"]}
        )
        group by 1, 2
        order by 3 desc
    )
    select distinct
        ORGANIZATION_ID,
        STATE
    from base
    where N_RECORDS BETWEEN {min_dims} and {max_dims}
    """
    df = persistence.get_df(sql.format(min_dims, max_dims))
    return list(df.itertuples(index=False, name=None))

def all_soldto_account_scopes():
    sql = f"""
    with base as (
        select 
            TRIM(DIM.STREET) AS STREET,
            TRIM(DIM.CITY) AS CITY,
            TRIM(DIM.REGION) AS STATE,
            TRIM(DIM.ZIP5) AS ZIP5,
            TRIM(OPS.ORGANIZATION) AS ORGANIZATION,
            OPS.ORGANIZATION_ID AS ORGANIZATION_ID,
            TRIM(FCT.ACCOUNT) AS SOLDTO_ACCOUNT,
            OPS.ID AS ACCOUNT_ID
        FROM soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.dim_location DIM
        JOIN soldto_account{os.getenv("RUN_SCHEMA_NAME", '')}.fct_account FCT
            ON DIM.id = FCT.dim_location_id
        JOIN CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT OPS
            ON FCT.account = OPS.account
        WHERE FCT.account NOT IN (
            SELECT ACCOUNT FROM {environment.read()["invalid_accounts_table"]}
        )
    )
    select distinct ORGANIZATION_ID, LEFT(ZIP5, 3) as ZIP3 from base;
    """
    df = persistence.get_df(sql)
    return list(df.itertuples(index=False, name=None))

def all_state_zip3_scopes():
    sql = f"""select distinct
        OPS_STATE as STATE,
        left(OPS_ZIP5, 3) as ZIP3
    from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION 
    """
    df = persistence.get_df(sql)
    return list(df.itertuples(index=False, name=None))

def all_keepstock_account_scopes():
    sql = f"""select distinct
        CUSTOMER_ACCOUNT::varchar as ACCOUNT
    from KEEPSTOCK{os.getenv("RUN_SCHEMA_NAME", '')}.FCT_PROGRAM
    """
    df = persistence.get_df(sql)
    return df["ACCOUNT"].values


def rebuild_associations(organization: str):
    association_wizard.clear_associations()

    scopes = all_scopes(organization)

    for i, (organization, state) in enumerate(scopes):
        logger.info(f"Processing scope {i+1}/{len(scopes)}: {organization} @ {state}")
        association_wizard.run_scope(organization, state)
    logger.info("Completed scopes.")

    association_wizard.commit_associations()
    logger.info("Committed associations.")


if __name__ == "__main__":

    # CLI arguments
    parser = ArgumentParser()
    parser.add_argument("--organization", type=str, default=None)
    args = parser.parse_args()
    
    # runtime
    rebuild_associations(args.organization)
