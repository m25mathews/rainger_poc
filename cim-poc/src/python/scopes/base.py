from abc import abstractmethod, abstractstaticmethod
import pandas as pd


import persistence


MAGIC_SEPARATOR = "@$!?"


class ScopeBase:

    def get_ops(self):
        """
        Returns a dataframe containing the ops records for the given scope
        """
        return persistence.get_df(
            self._ops_query()
        )

    def get_dim(self):
        """
        Returns a dataframe containing the dim records for the given scope
        """
        return persistence.get_df(
            self._dim_query()
        )

    @classmethod
    def get_dim_sizes(cls, **kws):
        """
        Returns a dataframe containing the size of dim scopes
        """
        return persistence.get_df(
            cls._dim_size_query(**kws)
        )

    @classmethod
    def get_ops_sizes(cls, **kws):
        """
        Returns a dataframe containing the size of dim scopes
        """
        return persistence.get_df(
            cls._ops_size_query(**kws)
        )

    @classmethod
    def get_match_sizes(cls, **kws):
        """
        Returns a dataframe containing the size matching tasks per scope,
        computed as N_OPS * N_DIM
        """
        dim_df = cls.get_dim_sizes(**kws)
        ops_df = cls.get_ops_sizes(**kws)
        scope_vars = [z for z in dim_df if z != 'SIZE']

        return (
            (dim_df.set_index(scope_vars) * ops_df.set_index(scope_vars))
            .fillna(0)
            .astype(int)
            .reset_index()
            .sort_values(by="SIZE", ascending=False)
        )

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, incremental: bool = False):
        col_to_arg = {
            "ACCOUNT": "accounts",
            "ORGANIZATION_ID": "organization_ids",
            "STATE": "states",
            "ZIP3": "zip3s",
        }
        data = df.rename(columns={
            k: v
            for k, v in col_to_arg.items()
            if k in df.columns
        })
        return cls(**data.to_dict(orient='list'), incremental=incremental)

    @abstractmethod
    def _dim_query(self):
        pass

    @abstractmethod
    def _ops_query(self):
        pass

    @abstractstaticmethod
    def _dim_size_query(**kws):
        pass

    @abstractstaticmethod
    def _ops_size_query(**kws):
        pass