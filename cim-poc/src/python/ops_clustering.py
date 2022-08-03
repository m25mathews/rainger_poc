import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import haversine_distances
from sklearn.cluster import AgglomerativeClustering
import os
if "ARM64" not in str(os.uname()):
    from sparse_dot_mkl import dot_product_mkl
from sklearn.preprocessing import OneHotEncoder

from uuid import uuid1

import persistence
import query
from scopes.base import ScopeBase
from loggers import get_logger, timer

EARTH_RADIUS_M = 6371007.2
SEARCH_RADIUS_M = 100
logger = get_logger("OPS-CLUSTERING")


def cluster_desc(df):
    """ Determine the name of a parent ops row by cluster 

        arguments:
            df (DataFrame): dataframe for a single cluster

        returns:
            str: name of cluster
    """
    clustix = df.CLUSTER.iloc[0]
    city = df.OPS_CITY.value_counts().idxmax()
    street = df.OPS_STREET.value_counts().idxmax()
    state = df.OPS_STATE.value_counts().idxmax()
    zip5 = df.OPS_ZIP5.value_counts().idxmax()
    orgid = df.ORGANIZATION_ID.value_counts().idxmax()
    orgname = df.ORGANIZATION_NAME.value_counts().idxmax()
    return clustix, city, state, street, zip5, orgid, orgname


def cluster_to_ops(df):
    if len(df) > 1:
        ix, city, state, street, zip5, orgid, orgname = cluster_desc(df)
        lat = df["LATITUDE"].mean()
        lon = df["LONGITUDE"].mean()
        children = df["ID"].values.tolist()

        record = pd.Series(dict(
            ID=str(uuid1()),
            OPS_LOC_NAME=f"{orgname} {state} P{ix} {street} {city}",
            MAIN_LOC_NAME=f"{orgname} {state} P{ix}",
            OPS_STREET = street,
            OPS_CITY = city,
            OPS_STATE = state,
            OPS_ZIP5 = zip5,
            OPS_MARKER = None,
            OPS_SUBLOCATION = "",
            ORGANIZATION_ID = orgid,
            ORGANIZATION_NAME = orgname,
            LATITUDE = lat,
            LONGITUDE = lon,
            CLUSTER = ix,
            children = children,
            GEOCODE_ACCURACY = None,
            GEOCODE_LEVEL = None,
            IS_RESIDENTIAL = False,
            CURATED = False,
            IS_SITE = True,
            IS_ADDRESS = False,
            IS_BUILDING = False
        ))
        return record.to_frame().T


@timer(logger)
def cluster_ops(ops_df: pd.DataFrame) -> pd.DataFrame:
    """Cluster OPS locations to determine parent nodes

    Args:
        ops_df (pd.DataFrame): dataframe of OPS rows

    Returns:
        pd.DataFrame: Parent node OPS records
    """
    onehot_encoding = OneHotEncoder().fit_transform(ops_df[["ORGANIZATION_ID"]])
    if "ARM64" not in str(os.uname()):
        organisation_mask = dot_product_mkl(onehot_encoding,onehot_encoding.T).toarray()
    else:
        organisation_mask = np.dot(onehot_encoding, onehot_encoding.T).toarray()
    organisation_mask = organisation_mask.astype(bool)

    dists = haversine_distances(ops_df[["LATITUDE", "LONGITUDE"]].apply(np.deg2rad)) * EARTH_RADIUS_M
    dists[~organisation_mask] = 1e8

    agg = AgglomerativeClustering(
        affinity="precomputed",
        linkage="single",
        n_clusters=None,
        distance_threshold = SEARCH_RADIUS_M,
        compute_full_tree=True
    ).fit(dists)

    ops_df = ops_df.assign(CLUSTER=agg.labels_)
    cluster_df = ops_df.groupby("CLUSTER").apply(cluster_to_ops)

    if len(cluster_df) > 0:
        return cluster_df.dropna(subset=["OPS_LOC_NAME"])
    return cluster_df


@timer(logger)
def cluster_ops_from_snowflake(scope: ScopeBase):

    if scope.incremental:
        scope.incremental = False

    df = scope.get_ops().query("IS_RESIDENTIAL == False")

    if len(df) <= 1:
        logger.warning(f"Not enough OPS locations to cluster")
        return

    parents = cluster_ops(df)

    if len(parents) > 0:
        
        # write clusters to temp

        persistence.upload_df(
            parents[["OPS_LOC_NAME", "children"]].rename(columns={
                "OPS_LOC_NAME": "parent_loc_name",
                "children": "child_location_ids",
            }),
            table='STG_PARENT_LOC',
            schema='TEMP'
        )
        return parents

    else:
        logger.warning(f"No valid parents found in scope")


def clear_cluster_temp_table():
    persistence.truncate_table("TEMP", "STG_PARENT_LOC")


def delete_ops_location_clusters():
    with persistence.get_conn() as conn:
        sql = f"""DELETE FROM CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION WHERE IS_SITE = TRUE;"""
        with conn.cursor() as cur:
            cur.execute(sql)


def commit_clusters():
    with persistence.get_conn() as conn:
        sql = f"""
        merge into CIM{os.getenv("RUN_SCHEMA_NAME", '')}.LOCATION as ops_loc
        using (
            select
                parent_loc_name, C.value as child_location_id
            from TEMP{os.getenv("RUN_SCHEMA_NAME", '')}.STG_PARENT_LOC stg,
            Table(Flatten(stg.child_location_ids)) C
        ) as upd on ops_loc.id = upd.child_location_id
        when matched then update set ops_loc.main_loc_name = upd.parent_loc_name;"""
        logger.info("Merging parents into ops rows")
        with conn.cursor() as cur:
            cur.execute(sql)
