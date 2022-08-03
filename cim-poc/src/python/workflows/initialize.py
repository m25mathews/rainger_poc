import numpy as np
import pickle

import scopes
import ops_entities
import association_wizard
import ops_clustering
from loggers import get_logger, timer
from utils import scopefiles


logger = get_logger("INITIALIZE")


def salesorder(
    n_parallel_small: int = None,
    n_parallel_medium: int = None,
    n_parallel_large: int = None,
    n_parallel_huge: int = None,
    max_scopes: int = 0,
    min_dims_medium: int = None,
    max_dims_medium: int = None,
    incremental: bool = True,
):
    """ Resets ops, imports curated accounts, and pickles scopes for downstream tasks """

    association_wizard.AssociationSalesOrder().clear_associations()  # clears temp table
    ops_clustering.clear_cluster_temp_table()                        # clears temp table

    if not incremental:
        ops_entities.reset_ops_locations()

    pickler = scopefiles.ScopePickler(
        identifier="salesorder",
        incremental=incremental,
        scope_class=scopes.ScopeSalesOrderOrgIdState,
        size_method="get_dim_sizes",
        max_scopes=max_scopes,
    )

    group_defs = {
        "small":  {"n_parallel": n_parallel_small, "min_size": -1, "chunk_size": 16_000},
        "medium": {"n_parallel": n_parallel_medium, "min_size": min_dims_medium, "chunk_size": 100},
        "large":  {"n_parallel": n_parallel_large, "min_size": max_dims_medium, "chunk_size": 5},
    }

    # incremental runs should not need support for massive scopes of unlabeled dims
    if not incremental:
        group_defs["huge"] = {"n_parallel": n_parallel_huge, "min_size": 30_000, "chunk_size": 1}


    pickler.write_pickles_groups(group_defs)
    logger.info("Initialized salesorder scopes")


def soldto(
    incremental: bool = True,
    n_parallel: int = None,
    max_scopes: int = None,
):
    association_wizard.AssociationSoldToAccount().clear_associations()
    if not incremental:
        ops_entities.reset_ops_soldto_locations()

    pickler = scopefiles.ScopePickler(
        identifier="soldto",
        incremental=incremental,
        scope_class=scopes.ScopeSoldToAccountOrgIdZip3,
        size_method="get_dim_sizes",
        max_scopes=max_scopes,
    ) 

    pickler.write_pickles(n_parallel, chunk_size=3_000)
    logger.info("Initialized sold-to scopes")


def dnb(
    incremental: bool = True,
    n_parallel_small: int = None,
    n_parallel_medium: int = None,
    n_parallel_large: int = None,
    n_parallel_huge: int = None,
    max_scopes: int = None,
):
    pickler = scopefiles.ScopePickler(
        identifier="dnb",
        incremental=incremental,
        scope_class=scopes.ScopeDnbStateZip3,
        max_scopes=max_scopes,
        size_method="get_match_sizes"
    )
    group_defs = {
        "SM": {"n_parallel": n_parallel_small, "min_size": 0, "chunk_size": 1},
        "MD": {"n_parallel": n_parallel_medium, "min_size": 8.3e7, "chunk_size": 1},
        "LG": {"n_parallel": n_parallel_large, "min_size": 4.09e8, "chunk_size": 1},
        "HG": {"n_parallel": n_parallel_huge, "min_size": 1.5e9, "chunk_size": 1}
    }
    pickler.write_pickles_groups(group_defs)
    logger.info("Initialized DnB")


def keepstock(
    incremental: bool = True,
    n_parallel: int = None,
    max_scopes: int = None
):
    association_wizard.AssociationKeepStock().clear_associations()
    pickler = scopefiles.ScopePickler(
        identifier="keepstock",
        incremental=incremental,
        scope_class=scopes.ScopeKeepstockAccount,
        size_method="get_dim_sizes",  # TODO: make keepstock parallelism more like dnb
        max_scopes=max_scopes
    )
    pickler.write_pickles(n_parallel, chunk_size=100)
    logger.info("Initialized keepstock")
