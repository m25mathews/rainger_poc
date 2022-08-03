import pickle
import traceback

import association_wizard as aw
from loggers import get_logger
from utils import scopefiles

logger = get_logger("ASSOCIATE")


def build_associations_dnb(group: str, pid: int = None):
    
    # TODO: standardize group names
    # dnb currently has SM, MD, LG, HG
    # USE Enum class to organize the available scope groups
    _scopes = scopefiles.load("dnb", group, pid)
    
    nscopes = len(_scopes)
    for i, scope in enumerate(_scopes):
        try:
            logger.info(f"Running {i+1}/{nscopes}")
            aw.AssociationDnb(scope).run(nchunks=10 if group == "HG" else 1)
        except Exception as e:
            logger.error(e)
    logger.info("Finished DNB scopes")


def build_associations_keepstock(
    pid: int = None,
):
    
    _scopes = scopefiles.load("keepstock", None, pid)
    
    nscopes = len(_scopes)
    for i, scope in enumerate(_scopes):
        try:
            logger.info(f"Running {i+1}/{nscopes}")
            aw.AssociationKeepStock(scope).run()
        except Exception as e:
            logger.error(e)
    logger.info("Finished Keepstock scopes")


def build_associations(
    pid: int = None,
    group: str = None,
):
    _scopes = scopefiles.load("salesorder", group, pid)
   
    n_scopes = len(_scopes)
    for i, scope in enumerate(_scopes):
        logger.info(f"{i+1}/{n_scopes}")
        try:
            aw.AssociationSalesOrder(scope).run(nchunks=100 if group == "large" else 1)
        except Exception as e:
            logger.error(e)
            traceback.print_exc()


def build_associations_soldto(
    pid: int = None,
):
    _scopes = scopefiles.load("soldto", None, pid)
   
    n_scopes = len(_scopes)
    for i, scope in enumerate(_scopes):
        logger.info(f"{i+1}/{n_scopes}")
        try:
            aw.AssociationSoldToAccount(scope).run(nchunks=10)
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
