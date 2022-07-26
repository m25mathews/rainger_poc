import os
from typing import Union

import environment
from .base import MAGIC_SEPARATOR, ScopeBase

SO_SCHEMA = "SALES_ORDER"+os.getenv("RUN_SCHEMA_NAME", '')
DIM_LOCATION = "DIM_LOCATION_SALES_ORDER"
FCT_ORDER = "FCT_SALES_ORDER"

class ScopeSalesOrderOrgIdState(ScopeBase):

    def __init__(
        self,
        states: Union[list, str] = None,
        organization_ids: Union[list, str] = None,
        incremental: bool = True
    ):
        if states is None:
            raise ValueError
        if organization_ids is None:
            raise ValueError
        if isinstance(states, str):
            states = [states]
        
        if isinstance(organization_ids, str):
            organization_ids = [organization_ids]

        if len(states) != len(organization_ids):
            raise ValueError("States and Organizations must be the same length!")

        self.states = states
        self.organization_ids = organization_ids
        self.incremental = incremental
        self.reference_table = environment.read()["invalid_accounts_table"]

    def _prepare_where_clauses(self):
        orgs = ','.join(f"'{orgid}'" for orgid in self.organization_ids)
        states = ','.join(f"'{state}'" for state in self.states)
        orgstates = ','.join(f"'{org}{MAGIC_SEPARATOR}{state}'" for org, state in zip(self.organization_ids, self.states))
        return orgs, states, orgstates
    
    def _dim_query(self):
        orgs, states, orgstates = self._prepare_where_clauses()

        return f"""select distinct
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
        from {SO_SCHEMA}.{DIM_LOCATION} dl
        inner join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa on oa.ACCOUNT = dl.SOLD_ACCOUNT 
        left join CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION oo on oa.ORGANIZATION_ID = oo.ID
        where oa.ACCOUNT not in (
            SELECT ACCOUNT FROM {self.reference_table}
        )
        and oa.ORGANIZATION_ID in ({orgs}) -- more efficient subset
        and dl.STATE in ({states})
        and concat(oa.ORGANIZATION_ID, '{MAGIC_SEPARATOR}', dl.STATE) in ({orgstates})
        {'and OPS_LOCATION_ID is NULL' if self.incremental else ''}
        order by STREET
        """

    def _ops_query(self):
        orgs, states, orgstates = self._prepare_where_clauses()

        return f"""select
            *
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
        where IS_SITE = FALSE
        and ORGANIZATION_ID in ({orgs}) -- more efficient subset
        and OPS_STATE in ({states})
        and concat(ORGANIZATION_ID, '{MAGIC_SEPARATOR}', OPS_STATE) in ({orgstates})
        """

    @staticmethod
    def _dim_size_query(between: tuple = None, incremental: bool = False):
        reference_table = environment.read()["invalid_accounts_table"]
        btw_clause = null_clause = ""
        if between is not None:
            btw_clause = f"having SIZE BETWEEN {between[0]} and {between[1]}"

        if incremental:
            null_clause = "and OPS_LOCATION_ID is NULL"

        return f"""select
                oa.ORGANIZATION_ID as ORGANIZATION_ID,
                dl.STATE,
                count(distinct dl.ID) as SIZE
            from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.ORGANIZATION_SOLDTO_ACCOUNT oa
            inner join {SO_SCHEMA}.{DIM_LOCATION} dl on dl.SOLD_ACCOUNT = oa.ACCOUNT
            where oa.account not in (
                SELECT ACCOUNT FROM {reference_table}
            )
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
            OPS_STATE as STATE,
            COUNT(*) as SIZE
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
        where IS_SITE = FALSE
        group by 1, 2
        {btw_clause}
        order by 3 desc
        """
