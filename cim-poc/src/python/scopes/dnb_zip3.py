import os
from typing import Union

from .base import ScopeBase, MAGIC_SEPARATOR

DNB_SCHEMA = "DNB"+os.getenv("RUN_SCHEMA_NAME", '')
DIM_LOCATION = "DIM_LOCATION_DNB"

class ScopeDnbStateZip3(ScopeBase):

    def __init__(
        self,
        states: Union[list, str] = None,
        zip3s: Union[list, str] = None,
        incremental: bool = True
    ):
        if states is None:
            raise ValueError
        if zip3s is None:
            raise ValueError

        if isinstance(states, str):
            states = [states]
        
        if isinstance(zip3s, str):
            zip3s = [zip3s]

        if len(states) != len(zip3s):
            raise ValueError("States and Organizations must be the same length!")

        self.states = states
        self.zip3s = zip3s
        self.incremental = incremental

    def _prepare_where_clauses(self):
        states = ','.join(f"'{state}'" for state in self.states)
        zip3s = ','.join(f"'{zip3}'" for zip3 in self.zip3s)
        zipstates = ','.join(f"'{state}{MAGIC_SEPARATOR}{zip3}'" for state, zip3 in zip(self.states, self.zip3s))
        return states, zip3s, zipstates

    def _dim_query(self):
        states, zip3s, zipstates = self._prepare_where_clauses()
        return f"""select distinct
                ID,
                PHYS_STRT_AD,
                PHYS_CTY,
                PHYS_ST_ABRV,
                left(PHYS_ZIP, 5) as PHYS_ZIP5
            from {DNB_SCHEMA}.{DIM_LOCATION}
            where PHYS_ST_ABRV in ({states})
                and left(PHYS_ZIP, 3) in ({zip3s})
                and concat(PHYS_ST_ABRV, '{MAGIC_SEPARATOR}', left(PHYS_ZIP, 3)) in ({zipstates})
                and PHYS_STRT_AD is not NULL
                and PHYS_CTY is not NULL
                and PHYS_ST_ABRV is not NULL
                and PHYS_ZIP is not NULL
            """

    def _ops_query(self):
        states, zip3s, zipstates = self._prepare_where_clauses()
        return f"""select
            ID,
            ORGANIZATION_NAME,
            OPS_STREET,
            OPS_CITY,
            OPS_STATE,
            OPS_SUBLOCATION,
            OPS_ZIP5
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
            where OPS_STATE in ({states})
            and left(OPS_ZIP5, 3) in ({zip3s})
            and concat(OPS_STATE, '{MAGIC_SEPARATOR}', LEFT(OPS_ZIP5, 3)) in ({zipstates})
            {'and DNB_DIM_LOCATION_ID is NULL' if self.incremental else ''}
        """

    @staticmethod
    def _dim_size_query(between: tuple = None, incremental: bool = False):
        btw_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"

        return f"""select
            PHYS_ST_ABRV as STATE,
            LEFT(PHYS_ZIP, 3) as ZIP3,
            COUNT(distinct ID) AS SIZE
        from {DNB_SCHEMA}.{DIM_LOCATION}
        group by 1, 2
        {btw_clause}
        order by 3 desc
        """
    
    @staticmethod
    def _ops_size_query(between: tuple = None, incremental: bool = False):
        btw_clause = null_clause = ""
        if between is not None:
            btw_clause = f"having SIZE between {between[0]} and {between[1]}"

        if incremental:
            null_clause = "where DNB_DIM_LOCATION_ID is NULL"        
    
        return f"""select
            OPS_STATE as STATE,
            LEFT(OPS_ZIP5, 3) as ZIP3,
            COUNT(*) as SIZE
        from CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION
        {null_clause}
        group by 1, 2
        {btw_clause}
        order by 3 desc
        """