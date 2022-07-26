import pandas as pd
import numpy as np
import re

import association_wizard as aw
import curation_wizard as cw
import transform


TEST_OPS = pd.DataFrame({
    "ID": ["1", "2", "3"],
    "OPS_LOC_NAME": ["123 Main Street", "456 Broad Street", "1989 Ehemann Drive"],
    "OPS_STREET": ["123 Main Street", "456 Broad Street", "1989 Ehemann Drive"],
    "OPS_CITY": ["Tullahoma", "Columbus", "Antioch"],
    "OPS_STATE": ["TN", "OH", "CA"],
    "OPS_ZIP5": ["37130", "43081", "94509"],
    "OPS_SUBLOCATION": ["", "" , "Building 1"],
    "ACCOUNT": ["08123", "08234", "08345"],
    "ORGANIZATION_ID": ["ORG1", "ORG1", "ORG1"],
}).sample(frac=1.0, replace=False)  # shuffling


TEST_DNB = pd.DataFrame({
    "ID": [123, 456, 789],
    "PHYS_STRT_AD": ["123 Main St", "456 Broad Ave", "1988 Eheman Dr"],
    "PHYS_CTY": ["Tullahoma", "Columbus", "Antioch"],
    "PHYS_ST_ABRV": ["TN", "OH", "CA"],
    "PHYS_ZIP5": ["37130", "43081", "94509"]
}).sample(frac=1.0, replace=False)


TEST_DIM = pd.DataFrame({
    "ID": np.arange(10),
    "STREET": ["123main stret", "123 Maine St", "123 main strt", "Main Street"] + \
              ["456 Broad St", "45 Broad Street", '456 Brod Street SEE GEORGE'] + \
              ["1989 Ehemann Dr bldg1", "aasdfefj 1989 Ehemann Drive 31fasd", "1989eman dr"],
    "CITY": 4 * ["Tullahoma"] + 3 * ["Columbus"] + 3 * ["Antioch"],
    "STATE": 4 * ["TN"] + 3 * ["OH"] + 3 * ["CA"],
    "ZIP5": ["37130", "37131", "37311", "37130", "43081", "40318", "43210", "94509", "90210", "94510"],
    "DEPARTMENT": 8 * [""] + ["Building 1"] + [""],
    "ATTENTION": 9 * [""] + ["Bldg 1"],
}).assign(
    RECEIVER="",
    STREET_NUM="",
    SUPPLEMENTAL="",
    COUNTRY="US",
    SOLD_ACCOUNT="0813141981",
    SHIP_ACCOUNT="0819314108",
    TRACK_CODE="ABCDEFG",
    SUB_TRACK_CODE="HIGJKLMNOP",
    ORGANIZATION_ID="ORG1",
).sample(frac=1.0, replace=False)


TEST_KST = pd.DataFrame({
    "ID": np.arange(4),
    "ADDRESS1": ["123 Main Street", "12 Main St", "45 Broad Ave", "1989 Ehemann Dr"],
    "CITY": ["Tullahoma", "Tullahoma", "Columbus", "Antioch"],
    "PROVINCE": ["TN", "TN", "OH", "CA"],
    "ZIP5": ["37130", "37130", "43081", "94509"],
    "ACCOUNT": ["08123", "08123", "08234", "08345"],
})


TEST_OPS["OPS_MARKER"] = TEST_OPS["OPS_SUBLOCATION"].apply(cw.CurationSalesOrder.infer_ops_marker).replace({pd.NA: None})
TEST_OPS["OPS_STR"] = TEST_OPS.apply(transform.ops_loc_str_simple, axis=1)
TEST_OPS["OPS_LOC_STR"] = TEST_OPS.apply(transform.ops_loc_str_simple, axis=1)
TEST_DNB["DNB_LOC_STR"] = TEST_DNB.apply(transform.dnb_loc_str_simple, axis=1)
TEST_DIM["DIM_STR_S"] = TEST_DIM.apply(transform.dim_loc_str_simple, axis=1)
TEST_DIM["DIM_STR"] = TEST_DIM.apply(transform.dim_loc_str_row, axis=1)
TEST_DIM["DIM_STR_ADDR"] = TEST_DIM.apply(transform.dim_loc_str_address, axis=1)
TEST_KST["KST_LOC_STR"] = TEST_KST.apply(transform.keepstock_loc_str_simple, axis=1)

def test_sales_order_rank():
    ixes, _ = aw.AssociationSalesOrder()._rank(TEST_OPS, TEST_DIM)

    opsids = TEST_OPS.iloc[ixes]["ID"]
    dimids = TEST_DIM["ID"]

    assert np.all(
        pd.DataFrame({
            "OPS_LOCATION_ID": opsids.values,
            "DIM_LOCATION_ID": dimids.values
        }).sort_values(by="DIM_LOCATION_ID").values == pd.DataFrame({
            "OPS_LOCATION_ID": list("1111222333"),
            "DIM_LOCATION_ID": np.arange(10)
        }).values
    )


def test_sales_order_rank_chunks():
    ixes, _ = aw.AssociationSalesOrder()._rank_chunks(TEST_OPS, TEST_DIM, nchunks=10)

    opsids = TEST_OPS.iloc[ixes]["ID"]
    dimids = TEST_DIM["ID"]

    assert np.all(
        pd.DataFrame({
            "OPS_LOCATION_ID": opsids.values,
            "DIM_LOCATION_ID": dimids.values
        }).sort_values(by="DIM_LOCATION_ID").values == pd.DataFrame({
            "OPS_LOCATION_ID": list("1111222333"),
            "DIM_LOCATION_ID": np.arange(10)
        }).values
    )


def test_sales_order_marker_to_dims():
    marker = re.compile(TEST_OPS.OPS_MARKER.sort_values().iloc[0], flags=re.IGNORECASE) # Building 1
    marked = np.asarray(aw.AssociationSalesOrder()._marker_to_dims(marker, TEST_DIM.DIM_STR_S).todense())[0, :]
    dimids = TEST_DIM[marked == 1]["ID"]

    assert np.all(dimids.isin([7, 8, 9]))


def test_sales_order_match_ops_markers():
    matrix = np.asarray(aw.AssociationSalesOrder()._match_ops_markers(TEST_OPS, TEST_DIM).todense())

    assert matrix.shape == (len(TEST_OPS), len(TEST_DIM))

    opsix, dimix = np.where(matrix > 0)
    assert np.all(TEST_OPS.iloc[opsix]["ID"] == "3")
    assert np.all(TEST_DIM.iloc[dimix]["ID"].isin([7, 8, 9]))


def test_sales_order_infer_training_data():
    df = aw.AssociationSalesOrder()._infer_training_data(TEST_OPS, TEST_DIM, rank_thresh=0.0)

    assert np.all(
        df.sort_values(by="ID")[["ID_y", "ID"]].values == pd.DataFrame({
            "OPS_LOCATION_ID": list("1111222333"),
            "DIM_LOCATION_ID": np.arange(10)
        }).values
    )


def test_sales_order_apply_machine_learning():
    wizard =  aw.AssociationSalesOrder()
    train_df = wizard._infer_training_data(TEST_OPS, TEST_DIM, rank_thresh=0.8)
    predictions = wizard._apply_machine_learning(
        train_df,
        TEST_DIM,
        TEST_OPS,
        organization="TEST",
        state="TEST"
    )

    assert np.all(
        predictions.sort_values(by="DIM_LOCATION_ID").values == pd.DataFrame({
            "DIM_LOCATION_ID": np.arange(10),
            "OPS_LOCATION_ID": list("1111222333")
        }).values
    )


def test_get_dnb_matches():
    matches = aw.AssociationDnb().get_matches(TEST_OPS, TEST_DNB)

    assert np.all(
        matches[["OPS_LOCATION_ID", "DIM_LOCATION_ID"]].sort_values(
            by="OPS_LOCATION_ID"
        ).values == pd.DataFrame({
            "OPS_LOCATION_ID": ["1", "2", "3"],
            "DIM_LOCATION_ID": [123, 456, 789],
        }).values
    )


def test_get_dnb_matches_chunks():
    matches = aw.AssociationDnb().get_matches(TEST_OPS, TEST_DNB, nchunks=10)

    assert np.all(
        matches[["OPS_LOCATION_ID", "DIM_LOCATION_ID"]].sort_values(
            by="OPS_LOCATION_ID"
        ).values == pd.DataFrame({
            "OPS_LOCATION_ID": ["1", "2", "3"],
            "DIM_LOCATION_ID": [123, 456, 789],
        }).values
    )


def test_get_keepstock_matches():
    matches = aw.AssociationKeepStock().get_matches(TEST_OPS, TEST_KST, nchunks=1)
    assert np.all(
        matches[["OPS_LOCATION_ID", "DIM_LOCATION_ID"]].sort_values(
            by="DIM_LOCATION_ID"
        ).values == pd.DataFrame({
            "OPS_LOCATION_ID": ["1", "1", "2", "3"],
            "DIM_LOCATION_ID": [0, 1, 2, 3],
        }).values
    )


def test_get_soldto_account_matches_exact_match():
    matches = aw.AssociationSoldToAccount().get_matches(TEST_OPS, TEST_DIM, nchunks=1)
    assert [4, 2, 1.000000] in matches.values


def test_get_soldto_account_matches_different_orgs():
    matches = aw.AssociationSoldToAccount().get_matches(
        TEST_OPS,
        TEST_DIM.assign(ORGANIZATION_ID="ORG2"),
        nchunks=1
    )
    assert matches["OPS_MATCH_SCORE"].max() == 0