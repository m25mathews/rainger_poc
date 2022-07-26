from multiprocessing import freeze_support

import association_wizard
import curation_wizard
import persistence
import query
from loggers import get_logger

logger = get_logger("TEST_ASSOCIATION_WIZARD")

def test():

    organization = "CATERPILLAR INC"
    state = "IL"

    dim_df, ops_df, cross_df = association_wizard.load_raw_data(organization, state)
    logger.info(cross_df)
    df = association_wizard.infer_training_data(cross_df, organization, state)
    return df


def test_curation_wizard(organization="CATERPILLAR INC", state="IL"):

    df = persistence.get_df(
        query.dim_location_scope(state, organization)
    )

    df = curation_wizard.precurate_df(df)

    return df


if __name__ == "__main__":
    freeze_support()
    test()
    test_curation_wizard()
