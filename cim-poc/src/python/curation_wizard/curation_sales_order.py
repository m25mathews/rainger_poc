import os

import pandas as pd
from uuid import uuid1

import persistence
from scopes.sales_order_orgid_state import ScopeSalesOrderOrgIdState
from loggers import get_logger, timer
from curation_wizard import preprocess_loc_row
from curation_wizard.preprocess_loc_row import USPS_DICT

from .curation_wizard import CurationWizard, VALID_SUFFIXLESS_STREETNAMES
import ops_entities

logger = get_logger("CURATION-WIZARD")


class CurationSalesOrder(CurationWizard):

    def __init__(self, scope: ScopeSalesOrderOrgIdState = None):
        super().__init__(scope)
        logger.info("CurationSalesOrder Initialized.")

    def preprocess_dim(self, df):
        return df

    @timer(logger)
    def autocurate_df(self, df, geocode_accuracy_thresh=0.0, auto_label: bool = False, simple_mode: bool = False):

        # SET UP SOME VARIABLES FOR OPERATIONS
        RENAME_COLS = {
            "ADDRESS": "OPS_STREET",
            "CITY": "OPS_CITY",
            "STATE": "OPS_STATE",
            "ZIP5": "OPS_ZIP5",
            "SUBLOCATION_LVL1": "OPS_SUBLOCATION",
            "ADDRESS_cnt": "OPS_STREET_COUNT",
            "ADDRESS_RAW_cnt": "RAW_STREET_COUNT",
            "ID": "DIM_LOCATION_ID",
        }

        GROUP_COLS = [
            "OPS_STREET",
            "OPS_CITY",
            "OPS_STATE",
            "OPS_ZIP5",
            "OPS_SUBLOCATION",
            "ORGANIZATION_ID",
            "ORGANIZATION_NAME",
        ]

        GROUP_COLS_2 = [
            "OPS_LOC_NAME",
            "OPS_STREET",
            "OPS_SUBLOCATION",
            "OPS_CITY",
            "OPS_STATE",
            "OPS_ZIP5",
            "OPS_MARKER",
            "ORGANIZATION_ID",
            "ORGANIZATION_NAME",
            "LATITUDE",
            "LONGITUDE",
            "GEOCODE_LEVEL",
            "GEOCODE_ACCURACY",
        ]

        GROUP_COLS_3 = [
            "OPS_STREET",
            "OPS_CITY",
            "OPS_STATE",
            "OPS_ZIP5",
            "ORGANIZATION_ID",
            "ORGANIZATION_NAME",
        ]

        OUTPUT_COLS = [
            "ID",
            "OPS_LOC_NAME",
            "MAIN_LOC_NAME",
            "OPS_STREET",
            "OPS_SUBLOCATION",
            "OPS_CITY",
            "OPS_STATE",
            "OPS_ZIP5",
            "ORGANIZATION_ID",
            "ORGANIZATION_NAME",
            "COMP",
            "OPS_MARKER",
            "CURATED",
            "LATITUDE",
            "LONGITUDE",
            "GEOCODE_LEVEL",
            "GEOCODE_ACCURACY",
            "IS_BUILDING",
            "IS_ADDRESS",
            "IS_SITE",
            "DIM_LOCATION_ID",
        ]

        STAGING_TABLE_NAME = "STG_LOCATION_ASS"

        # BEGIN
        df = df.rename(columns=RENAME_COLS)

        # df = (
        #     df.groupby(GROUP_COLS)
        #     .agg({"RAW_STREET_COUNT": "sum", "OPS_MARKER": "first", "DIM_LOCATION_ID": tuple})
        #     .reset_index()
        # )

        df = (
            df.groupby(GROUP_COLS)
            .agg({"RAW_STREET_COUNT": "sum", "OPS_MARKER": "first", "DIM_LOCATION_ID": tuple,
                  "IS_INTERSECTION": "first"})
            .reset_index()
        )

        logger.info(f"Autocurating {df.shape[0]} records")
        df = df.drop_duplicates()

        # Filter out rows that can't be correct
        df = df[df["OPS_STREET"] != ""]

        is_pobox_mask = df["OPS_STREET"].str.startswith("PO Box")

        contains_street_suffix_mask = super().smart_apply(
            df.OPS_STREET,
            super().contains_street_suffix,
        )

        first_token_valid_streetnum_mask = super().smart_apply(df.OPS_STREET, super().first_token_is_valid_streetnum)

        is_intersection_mask = df["IS_INTERSECTION"]

        df = df[(contains_street_suffix_mask & first_token_valid_streetnum_mask) |
                is_pobox_mask |
                is_intersection_mask
                ]

        df = df.drop(["IS_INTERSECTION"], axis=1)

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

        df["OPS_LOC_NAME"] = (
                df["ORGANIZATION_ID"].astype(str) + " @ " + df["OPS_STREET"] + " " + df["OPS_SUBLOCATION"].fillna("")
        ).str.strip()

        df = df.groupby(GROUP_COLS_2, dropna=False).agg({"DIM_LOCATION_ID": CurationWizard.coalesce}).reset_index()

        # generate boilerplate columns
        df["MAIN_LOC_NAME"] = pd.NA  # to be filled by curator
        df["IS_ADDRESS"] = df["OPS_MARKER"].isnull()
        df["IS_BUILDING"] = ~df["OPS_MARKER"].isnull()
        df["IS_SITE"] = False
        df["COMP"] = pd.NA
        df["CURATED"] = False

        df = df.groupby(GROUP_COLS_3, dropna=False).apply(CurationSalesOrder.set_parent_locations).reset_index()

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
        df = super().sanitize(df)
        return df[OUTPUT_COLS]

    @staticmethod
    def set_parent_locations(df):
        if not df.IS_BUILDING.any():
            return df
        else:
            if df.IS_ADDRESS.sum() == 1:
                main = df[df.IS_ADDRESS].iloc[0]
                df.loc[df.IS_BUILDING, "MAIN_LOC_NAME"] = main.OPS_LOC_NAME
            elif not df.IS_ADDRESS.any():
                logger.warning("No address-level parent for buildings")
            else:
                logger.warning("Multiple address-level parents for buildings")
            return df
