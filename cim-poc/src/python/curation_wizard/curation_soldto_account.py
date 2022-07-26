import os

import pandas as pd
from uuid import uuid1
import re
import uszipcode

import persistence
from loggers import get_logger, timer
from curation_wizard import preprocess_loc_row
from curation_wizard.preprocess_loc_row import USPS_DICT
from scopes.soldto_account_orgid_zip3 import ScopeSoldToAccountOrgIdZip3
from .curation_wizard import CurationWizard, VALID_SUFFIXLESS_STREETNAMES
import ops_entities
from upload_df import UploadOpsSoldToLocation

logger = get_logger("CURATION-WIZARD")

class CurationSoldToAccount(CurationWizard):

    GROUP_COLS = [
        "OPS_STREET",
        "OPS_CITY",
        "OPS_STATE",
        "OPS_ZIP5",
        "ORGANIZATION_ID",
        "ORGANIZATION_NAME"
    ]

    GROUP_COLS_2 = [
        "OPS_LOC_NAME",
        "OPS_STREET",
        "OPS_CITY",
        "OPS_STATE",
        "OPS_ZIP5",
        "ORGANIZATION_ID",
        "ORGANIZATION_NAME",
        "LATITUDE",
        "LONGITUDE",
        "GEOCODE_LEVEL",
        "GEOCODE_ACCURACY",
    ]

    def __init__(self, scope: ScopeSoldToAccountOrgIdZip3 = None):
        super().__init__(scope)

        logger.info("CurationSoldToAccount Initialized")

    @timer(logger)
    def preprocess_dim(self, df):

        uszipcode_engine = uszipcode.SearchEngine()

        def get_zip(row: pd.Series):
            if pd.isnull(row["STATE"]) or len(row["STATE"]) == 0:
                suggested = uszipcode_engine.by_zipcode(row['ZIP5'])
                if not pd.isnull(suggested):
                    return suggested
            return row["STATE"]

        df["STATE"] = df.apply(get_zip, axis=1)

        return df


    @timer(logger)
    def autocurate_df(self, df, geocode_accuracy_thresh=0.0, auto_label: bool = False, simple_mode: bool = False):

        RENAME_COLS = {
                "ADDRESS": "OPS_STREET",
                "CITY": "OPS_CITY",
                "STATE": "OPS_STATE",
                "ZIP5": "OPS_ZIP5",
                "SUBLOCATION_LVL1": "OPS_SUBLOCATION",
                "ADDRESS_cnt": "OPS_STREET_COUNT",
                "ADDRESS_RAW_cnt": "RAW_STREET_COUNT",
                "ID": "DIM_LOCATION_ID",
                "ORGANIZATION_ID": "ORGANIZATION_ID",
                "ORGANIZATION": "ORGANIZATION_NAME"
            }

        DROP_COLS=["OPS_SUBLOCATION"]

        OUTPUT_COLS = list(UploadOpsSoldToLocation.OPS_SOLDTO_LOCATION_COLUMN_TYPE_DICT.keys())

        STAGING_TABLE_NAME="STG_SOLDTO_LOC_ASS"

        # BEGIN
        df = df.rename(
            columns=RENAME_COLS
        )

        df = df.drop(columns=DROP_COLS).drop_duplicates()

        df = (
            df.groupby(self.GROUP_COLS)
            .agg({"RAW_STREET_COUNT": "sum", "DIM_LOCATION_ID": tuple})
            .reset_index()
        )

        logger.info(f"Autocurating {df.shape[0]} records")
        df = df.drop_duplicates()

        # Filter out rows that can't be correct
        df = df[df["OPS_STREET"] != ""]

        is_pobox_mask = df["OPS_STREET"].str.startswith("PO Box")

        contains_street_suffix_mask = CurationWizard.smart_apply(
            df.OPS_STREET,
            CurationWizard.contains_street_suffix,
        )

        first_token_valid_streetnum_mask = super().smart_apply(df.OPS_STREET, super().first_token_is_valid_streetnum)

        df = df[(contains_street_suffix_mask & first_token_valid_streetnum_mask) | is_pobox_mask]

        df["OPS_STREET"] = super().smart_apply(
            df["OPS_STREET"],
            super().remove_garbage_after_suffix,
        )

        if not simple_mode:
            df = super().apply_geocoding_steps(df, geocode_accuracy_thresh)
            if df is None:
                return None
        else:
            df = df.assign(
                LATITUDE=pd.NA,
                LONGITUDE=pd.NA,
                GEOCODE_LEVEL=pd.NA,
                GEOCODE_ACCURACY=pd.NA
            )

        road_names = set(USPS_DICT.values())
        df["OPS_STREET"] = super().smart_apply(df["OPS_STREET"], super().clean_final_street_address, args=(road_names,))
        df["OPS_CITY"] = df["OPS_CITY"].str.title()

        df["OPS_LOC_NAME"] = (
                df["ORGANIZATION_ID"].astype(str) + " @ " + \
                df["OPS_STREET"] + " " + \
                df["OPS_CITY"] + " " + \
                df["OPS_STATE"] + " " + \
                df["OPS_ZIP5"].astype(str)
        ).str.strip()

        df = df.groupby(self.GROUP_COLS_2, dropna=False).agg({"DIM_LOCATION_ID": CurationWizard.coalesce}).reset_index()

        df["ID"] = df.apply(ops_entities.generate_ops_location_id, axis=1)

        if auto_label:
            associations = (
                df[["ID", "DIM_LOCATION_ID"]]
                .rename(columns={"ID": "OPS_LOCATION_ID"})
                .explode("DIM_LOCATION_ID")
                .assign(OPS_MATCH_SCORE=1)
            )
            persistence.upload_df(
                associations,
                schema="TEMP",
                table=STAGING_TABLE_NAME
            )

        # export
        logger.info(f"Autocuration output {df.shape[0]} rows")

        return df[OUTPUT_COLS]
