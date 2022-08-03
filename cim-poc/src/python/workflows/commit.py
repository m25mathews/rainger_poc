
import ops_clustering
import association_wizard as aw
import populate_bridge_location
from loggers import get_logger


logger = get_logger("COMMIT")


def populate_bridge_table():
    logger.info("Clearing existing clusters")
    # TODO: figure out a better way to do incremental clustering
    ops_clustering.delete_ops_location_clusters()
    logger.info("Committing new clusters")
    ops_clustering.commit_clusters()
    logger.info("Populating BRG_OPS_LOCATION")
    populate_bridge_location.get_location()
    logger.info("All done!")


def commit_associations(identifier: str):
    logger.info("Committing associations...")

    if identifier == "salesorder":
        wizard = aw.AssociationSalesOrder()
    elif identifier == "soldto":
        wizard = aw.AssociationSoldToAccount()
    elif identifier == "dnb":
        wizard = aw.AssociationDnb()
    elif identifier == "keepstock":
        wizard = aw.AssociationKeepStock()
    else:
        raise ValueError(f"Unknown wizard identifier: {identifier}")

    wizard.commit_associations()
    logger.info("Committed")