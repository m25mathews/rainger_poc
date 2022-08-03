import pickle
import pandas as pd
import traceback


from utils import scopefiles
from loggers import get_logger
from flag_residence import flag_residential_df
import curation_wizard as cw
import ops_clustering as oc
import persistence
from upload_df import UploadOpsLocation, UploadOpsSoldToLocation

logger = get_logger("GENERATE")


def generate_ops_locations_small(
    group: str = "small",
    pid: int = None,
):
    logger.info(f"Generating OPS locations: pid {pid}")
    _scopes = scopefiles.load("salesorder", group, pid)

    n_scopes = len(_scopes)

    uploader = UploadOpsLocation()

    for i, scope in enumerate(_scopes):
        logger.info(f"Working on {i+1}/{n_scopes}")

        try:
            ops_df = cw.CurationSalesOrder(scope).autocurate(
                auto_label=True,
                simple_mode=True,
            )

            if ops_df is not None and len(ops_df) > 0:
                # logger.info(f"Flagging residential locations {organization} @ {state}")
                # ops_df = flag_residential_df(ops_df)
                ops_df = ops_df.assign(IS_RESIDENTIAL=pd.NA)

                logger.info(f"Loading ops locations")

                uploader.upload_ops_location_preload_temp_table(ops_df)

            else:
                logger.warning("No OPS locations to load")
        except Exception as e:
            logger.error("EXCEPTION: ", e)
            traceback.print_exc()

    uploader.merge_ops_locations()

    logger.info("Done.")


def generate_ops_locations(
    group: str = None,
    pid: int = None,
):
    logger.info(f"Generating OPS locations: pid {pid}")
    _scopes = scopefiles.load("salesorder", group, pid)

    n_scopes = len(_scopes)

    uploader = UploadOpsLocation()

    for i, scope in enumerate(_scopes):
        logger.info(f"Working on {i+1}/{n_scopes}")

        try:
            ops_df = cw.CurationSalesOrder(scope).autocurate(
                auto_label=True,
                simple_mode=False
            )

            if ops_df is not None and len(ops_df) > 0:
                logger.info(f"Flagging residential locations")
                ops_df = flag_residential_df(ops_df)

                logger.info(f"Loading")

                uploader.upload_ops_location_preload_temp_table(ops_df)

            else:
                logger.warning("No OPS locations to load")
        except Exception as e:
            logger.error("EXCEPTION: ", e)
            traceback.print_exc()

    uploader.merge_ops_locations()

    logger.info("Done.")


def generate_ops_soldto_locations(
    pid: int = None,
):
    _scopes = scopefiles.load("soldto", None, pid)
    
    nscopes = len(_scopes)

    uploader = UploadOpsSoldToLocation()

    for i, scope in enumerate(_scopes):
        logger.info(f"Working on {i+1}/{nscopes}")
        try:
            ops_df =cw.CurationSoldToAccount(scope).autocurate(
                simple_mode=False,
                auto_label=True,
            )

            uploader.upload_ops_location_preload_temp_table(ops_df)

        except Exception as e:
            logger.error(e)
            traceback.print_exc()

    uploader.merge_ops_locations()

    logger.info("Done.")


def generate_parent_ops_locations(
    pid: int = None,
    group: str = None,
):
    _scopes = scopefiles.load("salesorder", group, pid)
    uploader = UploadOpsLocation()

    nscopes = len(_scopes)
    for i, scope in enumerate(_scopes):
        logger.info(f"Working on {i+1}/{nscopes}")

        try:
            
            cluster_df = oc.cluster_ops_from_snowflake(scope)
            if cluster_df is not None and len(cluster_df) > 0:
                uploader.upload_ops_location_preload_temp_table(cluster_df)

        except Exception as e:
            logger.error("EXCEPTION: " + e)
            traceback.print_exc()
    
    uploader.merge_ops_locations()
