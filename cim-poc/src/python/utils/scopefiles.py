import numpy as np
import pandas as pd
import pickle

import scopes
from environment import SCOPES_DIR
from loggers import get_logger, timer

logger = get_logger("SCOPEFILES")


def get_name(identifier: str, group: str, pid: int) -> str:
    base = f"{SCOPES_DIR}/{identifier}_scopes"
    if group is None and pid is None:
        return f"{base}.pkl"
    elif group is None:
        return f"{base}_{pid}.pkl"
    elif pid is None:
        return f"{base}_{group}.pkl"
    else:
        return f"{base}_{group}_{pid}.pkl"

def load(identifier: str, group: str, pid: int):
    file = get_name(identifier, group, pid)
    with open(file, "rb") as f:
        scopes = pickle.load(f)
    return scopes


class ScopePickler():

    def __init__(self,
        incremental: bool = True,
        min_size: int = None,
        max_size: int = None,
        size_method: str = "get_dim_sizes",
        identifier: str = None,
        scope_class: scopes.ScopeBase = None,
        max_scopes: int = None,
    ):
        self.incremental = incremental
        self.min_size = min_size
        self.max_size = max_size
        self.size_method = size_method
        self.scope_class = scope_class
        self.identifier = identifier
        self.max_scopes = max_scopes


    @timer(logger)
    def write_pickles(self, n_parallel: int, chunk_size: int):

        between = None
        if self.min_size is not None and self.max_size is not None:
            between = (self.min_size, self.max_size)

        scope_sizes = getattr(self.scope_class, self.size_method)(
            between=between,
            incremental=self.incremental
        )

        scope_sizes = scope_sizes.sample(frac=1.0) # shuffle df

        if self.max_scopes > 0:
            scope_sizes = scope_sizes.head(self.max_scopes)

        self._split_and_pickle(scope_sizes, n_parallel, None, chunk_size)

    def write_pickles_groups(self, group_defs: dict):

        bin_bounds = [g["min_size"] for g in group_defs.values()]
        bin_labels = list(group_defs.keys())

        scope_sizes = getattr(self.scope_class, self.size_method)(
            incremental=self.incremental
        )
        scope_sizes["GROUP"] = pd.cut(
            scope_sizes["SIZE"].astype(float),
            [*bin_bounds, np.inf],
            labels=bin_labels
        )

        for group, df in scope_sizes.groupby("GROUP"):
            if self.max_scopes > 0:
                df = df.head(self.max_scopes)
            self._split_and_pickle(
                df.drop(["GROUP"], axis=1).sample(frac=1.0),
                group_defs[group]["n_parallel"],
                group,
                group_defs[group]["chunk_size"]
            )

    def _split_and_pickle(self, scope_sizes: pd.DataFrame, n_parallel: int, group: str, chunk_size: int):
        pod_scope_dfs = np.array_split(scope_sizes.drop("SIZE", axis=1), n_parallel)

        pod_scopes = [
            [
                self.scope_class.from_dataframe(pod.iloc[i:i+chunk_size], incremental=self.incremental)
                for i in range(0, len(pod), chunk_size)
            ]
            for pod in pod_scope_dfs
        ]

        for i, pod_scope in enumerate(pod_scopes):
            with open(get_name(self.identifier, group, i), "wb") as f:
                pickle.dump(pod_scope, f)