from abc import abstractmethod
from curses.ascii import US
import re
import time

import numpy as np
import pandarallel
import pandas as pd
from fuzzywuzzy.fuzz import ratio
from timeout_decorator import timeout
from uszipcode import SearchEngine
from toolz import compose

import geocode
from curation_wizard import preprocess_loc_row
from curation_wizard.preprocess_loc_row import ADDRESS_DICT, SPANISH_STREET_SUFFIXES, VALID_STREET_SUFFIXES, \
    VALID_DIRECTIONS
from loggers import get_logger, timer
from scopes import ScopeBase
from utils.usaddress_util import get_number_street

pandarallel.pandarallel.initialize(use_memory_fs=False)

logger = get_logger("CURATION-WIZARD")

MIN_LEN_PARALLEL = 100000
USZIP_ENG = SearchEngine()

# construct regular expressions
MARKER_BASE_DICT = {
    "BUILDING": r"BU??I??LD??I??N??G??",
    "WAREHOUSE": r"WA??R??E??HSE",
    "FACILITY": r"FACI??L??I??T??Y??",
    "PLANT": r"PLA??N??T",
    "DOCK": r"DO??C??K",
    "APARTMENT": r"APA??R??T??M??E??N??T",
    "SUITE": r"SU??I??TE",
    "ROOM": r"RO??O??M",
    "GATE": r"GA??TE??",
    "UNIT": r"UNIT",
    "DOOR": r"DOOR",
    "GSTORE": r"GSTORE",
}

POBOX_PATTERN = re.compile(r"^\s*P\.?\s?O\.?\s?B\s?[0O]?\s?X?\s?(\d+)\s*$", flags=re.IGNORECASE)
INTERSTATE_PATTERN = re.compile(r"^(.*)\sIN?T?E?R?S?T?A?T?E?\s(\d+)", flags=re.IGNORECASE)
VALID_SUFFIXLESS_STREETNAMES = [
    "broadway",
    "interstate",
    "highway",
    "concourse"
]


class CurationWizard:
    """ Super class for all curation wizards """

    def __init__(self, scope: ScopeBase = None):
        self.scope = scope

    def autocurate(
            self,
            geocode_accuracy_thresh=0.0,
            auto_label: bool = False,
            simple_mode: bool = False
    ):

        df = self.scope.get_dim()
        df = self.preprocess_dim(df)
        df = self.precurate_df(df)

        return self.autocurate_df(
            df,
            geocode_accuracy_thresh=geocode_accuracy_thresh,
            auto_label=auto_label,
            simple_mode=simple_mode
        )

    @abstractmethod
    def preprocess_dim(self):
        pass

    @abstractmethod
    def autocurate_df(self):
        pass

    @staticmethod
    def smart_apply(series: pd.Series, func: callable, **kws):
        if len(series) < MIN_LEN_PARALLEL:
            return series.apply(func, **kws)
        return series.parallel_apply(func, **kws)

    @staticmethod
    def precurate_df(df):
        """
        Ingests a dataframe of dim_location data and runs both 1) row-level
        inference of normalized address, sublocation_lvl1 and sublocation_lvl2,
        and 2) generates and applies statistical best guesses
        to fix inferred addresses to the most common format.
        """

        logger.info(f"Precurating {df.shape[0]} records")
        t0 = time.time()
        if df.shape[0] == 0:
            raise ValueError("Empty dataframe cannot be precurated")

        df = df.fillna("").reset_index(drop=True)

        df["ADDRESS"], df["SUBLOCATION_LVL1"], df["SUBLOCATION_LVL2"] = zip(
            *CurationWizard.smart_apply(df,
                                        lambda row: preprocess_loc_row.infer_address_and_sublocations(
                                            row.STREET_NUM,
                                            row.STREET,
                                            row.DEPARTMENT,
                                            row.ATTENTION,
                                            row.SUPPLEMENTAL,
                                            row.RECEIVER,
                                        ),
                                        axis=1,
                                        )
        )

        df["ADDRESS"] = CurationWizard.smart_apply(
            df["ADDRESS"],
            CurationWizard.handle_special_addresses
        )

        df["IS_INTERSECTION"] = CurationWizard.smart_apply(
            df["ADDRESS"],
            CurationWizard.handle_intersections
        )

        statistical_dict = CurationWizard.generate_statistical_dict(
            df["ADDRESS"],
            usps_dict=preprocess_loc_row.USPS_DICT,
            numeric=True,
            order_switch=True,
        )

        df["ADDRESS"] = CurationWizard.smart_apply(df["ADDRESS"],
                                                   lambda address: CurationWizard.apply_statistical_dict(address,
                                                                                                         statistical_dict)
                                                   )

        # Add OPS_MARKER
        df["OPS_MARKER"] = CurationWizard.smart_apply(df["SUBLOCATION_LVL1"], CurationWizard.infer_ops_marker)

        # Adds columns representing how many times value is seen in data
        df["ADDRESS_cnt"] = df.groupby("ADDRESS").STREET.transform("count")

        df["ADDRESS_CITY_ZIP5_cnt"] = df.groupby(
            ["ADDRESS", "CITY", "ZIP5"]
        ).STREET.transform("count")

        df["SUBLOCATION_LVL1_cnt"] = df.groupby(
            ["ADDRESS", "SUBLOCATION_LVL1"]
        ).STREET.transform("count")
        df["ADDRESS_RAW_cnt"] = df.groupby(
            ["STREET", "CITY", "STATE", "ZIP5"]
        ).STREET.transform("count")

        df = df.sort_values(
            [
                "ADDRESS_cnt",
                "ADDRESS",
                "SUBLOCATION_LVL1_cnt",
                "SUBLOCATION_LVL1",
                "SUBLOCATION_LVL2",
                "STREET",
            ],
            ascending=False,
        ).reset_index(drop=True)

        logger.info(f"Precurated in {time.time() - t0:.1f} seconds")
        return df

    @staticmethod
    def infer_ops_marker(
            sublocation_lvl1, marker_base_dict=MARKER_BASE_DICT,
    ):
        if (
                sublocation_lvl1 != ""
                and sublocation_lvl1.strip() not in marker_base_dict.keys()
        ):
            tokens = preprocess_loc_row.tokenize(sublocation_lvl1)
            delimiters = [
                ".",
                "-",
                ":",
                "#",
            ]

            ops_marker = r"\b("
            for i, token in enumerate(tokens):
                if token.upper() in marker_base_dict.keys():
                    if i == len(tokens) - 1:
                        ops_marker = pd.NA
                        break
                    ops_marker += marker_base_dict[token.upper()]
                    for j, char in enumerate(delimiters):
                        ops_marker += rf"[\s{char}]?"
                else:
                    for j, char in enumerate(token.lower()):
                        ops_marker += rf"[{char.upper()}]"
                    if i < len(tokens) - 1:
                        for j, char in enumerate(delimiters):
                            ops_marker += rf"[\s{char}]?"
            ops_marker += r")\b"
        else:
            ops_marker = pd.NA
        return ops_marker

    # Generates a dict of {tokenized_address -> fixed_address} based on the whole address population
    @staticmethod
    def generate_statistical_dict(
            address_series, usps_dict=None, numeric=False, order_switch=False,
    ):
        df = (
            address_series.value_counts()
            .rename("cnt")
            .to_frame()
            .reset_index()
            .rename({"index": "address"}, axis=1)
        )
        df["tokens"] = CurationWizard.smart_apply(df["address"],
                                                  lambda address: tuple(preprocess_loc_row.tokenize(address))
                                                  )

        # For tokens listed in the values of USPS_DICT (e.g. Avenue, Road, etc.)
        missing_usps_dict = (
            CurationWizard.generate_missing_token_dict(df, usps_dict) if usps_dict != None else {}
        )
        missing_street_num_dict = CurationWizard.generate_missing_street_num_dict(df) if numeric else {}
        order_switch_dict = CurationWizard.generate_order_switch_dict(df) if order_switch else {}

        return {
            **missing_usps_dict,
            **missing_street_num_dict,
            **order_switch_dict,
        }

    @staticmethod
    def override_zip_and_city_based_on_geocodio(frame, address_and_county):

        address, *_ = address_and_county

        condensed_alternatives = frame[
            [
                "OPS_CITY",
                "OPS_ZIP5",
                "OPS_STATE",
                "LATITUDE",
                "LONGITUDE",
                "GEOCODE_ACCURACY",
                "GEOCODE_LEVEL",
            ]
        ].drop_duplicates()
        condensed_alternatives["OPS_STREET"] = address

        result = frame

        # If only one alternative, nothing is transformed
        # If includes "Route", nothing is transformed, since the same Street
        # often appears across multiple Cities/Zips in case of e.g. state routes

        if (condensed_alternatives.shape[0] > 1) and ("Route" not in address):
            # Popularity based choice if multiple 1.00 Accuracies
            if (
                    condensed_alternatives[
                        condensed_alternatives["GEOCODE_ACCURACY"] > 0.99
                    ].shape[0]
                    > 1
            ):
                most_accurate_row = (
                    frame[frame["GEOCODE_ACCURACY"] > 0.99]
                    .sort_values("RAW_STREET_COUNT", ascending=False)
                    .head(1)
                    .squeeze()
                )
            # Geocode accuracy based choice otherwise
            else:
                most_accurate_row = (
                    frame.sort_values("GEOCODE_ACCURACY", ascending=False).head(1).squeeze()
                )
            result["OPS_CITY"] = most_accurate_row["OPS_CITY"]
            result["OPS_ZIP5"] = most_accurate_row["OPS_ZIP5"]
            result["LATITUDE"] = most_accurate_row["LATITUDE"]
            result["LONGITUDE"] = most_accurate_row["LONGITUDE"]
            result["GEOCODE_ACCURACY"] = most_accurate_row["GEOCODE_ACCURACY"]
            result["GEOCODE_LEVEL"] = most_accurate_row["GEOCODE_LEVEL"]

        return result

    @staticmethod
    def override_street_based_on_geocodio(frame, df):

        condensed_alternatives = frame[
            [
                "OPS_STREET",
                "OPS_CITY",
                "OPS_ZIP5",
                "OPS_STATE",
                "GEOCODE_ACCURACY",
                "GEOCODE_LEVEL",
            ]
        ].drop_duplicates()

        result = frame

        # If only one alternative, nothing is transformed
        # Transform only if rather certain the geocodes are pointing to the same place,
        # i.e. rooftop, range_interpolation or nearest_rooftop_match.

        if (condensed_alternatives.shape[0] > 1) & (
                condensed_alternatives.head(1).squeeze()["GEOCODE_LEVEL"]
                in ["rooftop", "range_interpolation", "nearest_rooftop_match"]
        ):
            row = condensed_alternatives.head(1).squeeze()

            geocoding_results_df = df.loc[(df['OPS_STREET'] == row.OPS_STREET) &
                                          (df['OPS_CITY'] == row.OPS_CITY) &
                                          (df['OPS_ZIP5'] == row.OPS_ZIP5) &
                                          (df['OPS_STATE'] == row.OPS_STATE)]

            formatted_address = geocoding_results_df["FORMATTED_ADDRESS"].tolist()
            if len(formatted_address) == 0:
                return frame

            formatted_address = formatted_address[0]
            if formatted_address is None:
                return frame

            try:
                street_number, formatted_street = get_number_street(formatted_address)
            except Exception as e:
                return frame

            formatted_street = preprocess_loc_row.detokenize(
                [street_number]
                + [
                    preprocess_loc_row.ADDRESS_DICT.get(token.lower(), token)
                    for token in preprocess_loc_row.tokenize(formatted_street)
                ]
            )

            condensed_alternatives["fuzz_ratio"] = condensed_alternatives[
                "OPS_STREET"
            ].apply(lambda street: ratio(street, formatted_street))
            most_accurate_row = (
                condensed_alternatives.sort_values("fuzz_ratio", ascending=False)
                .head(1)
                .squeeze()
            )
            result["OPS_STREET"] = most_accurate_row["OPS_STREET"]
            result["OPS_CITY"] = most_accurate_row["OPS_CITY"]
            result["OPS_ZIP5"] = most_accurate_row["OPS_ZIP5"]
            result["GEOCODE_ACCURACY"] = most_accurate_row["GEOCODE_ACCURACY"]
            result["GEOCODE_LEVEL"] = most_accurate_row["GEOCODE_LEVEL"]

        return result

    @staticmethod
    def contains_street_suffix(address):
        street_suffixes = [suffix.lower() for suffix in VALID_STREET_SUFFIXES | VALID_DIRECTIONS]
        tokens = preprocess_loc_row.tokenize(address)
        any_token_is_street_suffix = any(
            [token.lower() in street_suffixes for token in tokens]
        ) or any(
            [token.lower() in VALID_SUFFIXLESS_STREETNAMES for token in tokens]
        )
        return any_token_is_street_suffix

    @staticmethod
    def first_token_is_valid_streetnum(address):
        tokens = preprocess_loc_row.tokenize(address)
        first_token_is_numeric = tokens[0].isnumeric()
        first_token_is_coord = preprocess_loc_row.COORD_REGEX.match(tokens[0]) is not None
        return first_token_is_numeric or first_token_is_coord

    @staticmethod
    def remove_garbage_overrides(tokens):
        return any(token.lower() in [
            *["state", "route", "county", "fm", "highway", "interstate"],
            *[*map(str.lower, SPANISH_STREET_SUFFIXES)]
        ] for token in tokens)

    @staticmethod
    def remove_garbage_after_suffix(address):
        tokens = preprocess_loc_row.tokenize(address)

        if len(tokens) == 0:
            result = address
        # Don't remove anything if contains State or Route. They tend to have specifiers in the end.
        elif CurationWizard.remove_garbage_overrides(tokens):
            result = address
        elif tokens[-1] in VALID_STREET_SUFFIXES:
            result = address
        elif tokens[1] in VALID_DIRECTIONS:
            result = address  # no suffix, but directions -- e.g. 123 South Alloy (real example, valid)
        elif any(token in VALID_STREET_SUFFIXES | VALID_DIRECTIONS for token in tokens):
            last_loc_of_suffix = (
                    max(i for i, token in enumerate(tokens) if token in ADDRESS_DICT.values())
                    + 1
            )
            result = preprocess_loc_row.detokenize(tokens[:last_loc_of_suffix])
        else:
            result = address
        return result

    @staticmethod
    def sanitize(ops_df):
        dtypes = ops_df.dtypes
        for col in ["LATITUDE", "LONGITUDE"]:
            if dtypes[col] == object:
                logger.warning(f"object data type for {col}", extra={"bad_dtype": True})
                ops_df[col] = ops_df[col].apply(pd.to_numeric)
        for col in ["ORGANIZATION_ID"]:
            if dtypes[col] != object:
                ops_df[col] = ops_df[col].astype(str)
        return ops_df

    @timeout(60 * 30)
    def apply_geocoding_steps(self, df, geocode_accuracy_thresh):
        # check if addresses are geocodable
        logger.info(f"Batch geocoding {df.shape[0]} records")
        t0 = time.time()
        df["id"] = np.arange(len(df))
        if len(df) > 0:
            geocoded = geocode.geocode_df(
                df,
                "id",
                "OPS_STREET",
                "OPS_CITY",
                "OPS_STATE",
                "OPS_ZIP5",
                "ORGANIZATION_NAME"
            )

        else:
            logger.warning(f"Empty ops df")
            return None

        df = df.merge(
            geocoded, on=["id"]
        ).rename(columns={
            "lat": "LATITUDE",
            "lon": "LONGITUDE",
            "accuracy": "GEOCODE_ACCURACY",
            "type": "GEOCODE_LEVEL"
        }).drop(columns=["id"]).sort_index().reset_index(drop=True)

        logger.info(f"Batch geocoded in {time.time() - t0:.1f} seconds")
        t0 = time.time()
        # Weird apply hack to get group key in the function arguments of the transform call
        # df["OPS_COUNTY"] = CurationWizard.get_county_from_zipcode(df["OPS_ZIP5"])
        # df = (
        #     df.groupby(["OPS_STREET", "OPS_COUNTY", "OPS_STATE", "ORGANIZATION_ID"])
        #         .apply(
        #         lambda frame: frame.transform(
        #             lambda x: CurationWizard.override_zip_and_city_based_on_geocodio(x, frame.name)
        #         )
        #     )
        #     .drop_duplicates()
        #     .reset_index(drop=True)
        # )

        # df = (
        #     df.groupby(["LONGITUDE", "LATITUDE", "GEOCODE_LEVEL", "ORGANIZATION_ID"]).apply(
        #         CurationWizard.override_street_based_on_geocodio,
        #         df
        #     ).drop_duplicates().reset_index(drop=True)
        # )

        # df = (
        #     df.groupby(["OPS_STREET", "OPS_COUNTY", "OPS_STATE", "ORGANIZATION_ID"])
        #     .apply(
        #         lambda frame: frame.transform(
        #             lambda x: CurationWizard.override_zip_and_city_based_on_geocodio(x, frame.name),
        #         )
        #     )
        #     .drop_duplicates()
        #     .reset_index(drop=True)
        # )
        # logger.info(f"Geocoded overrides in {time.time() - t0:.1f} seconds")

        return df

    @staticmethod
    def apply_statistical_dict(address, statistical_dict_dict):
        while tuple(preprocess_loc_row.tokenize(address)) in statistical_dict_dict.keys():
            address = statistical_dict_dict.get(
                tuple(preprocess_loc_row.tokenize(address)), address
            )
        return address

    # Generates a dict that adds a missing token to the address, if the version with the added token
    # is more common than the version without that token, for such tokens that exist in the input dict values.
    #
    # e.g. for input dict of DIRECTIONS_DICT, if both "140 North Jefferson Avenue" and "140 Jefferson Avenue"
    # are present in the data, and "140 North Jefferson Avenue" is more common, then an entry of
    # {("140", "Jefferson", "Avenue"): "140 North Jefferson Avenue"} is created.
    @staticmethod
    def generate_missing_token_dict(df, d):

        df["reduced"] = CurationWizard.smart_apply(df["tokens"],
                                                   lambda tokens: tuple(
                                                       token for token in tokens if token not in set(d.values()))
                                                   )

        df = df[df["reduced"].isin(set(df["tokens"]))]
        df = df[df["reduced"] != ()]
        df = df.sort_values("cnt", ascending=False).groupby("reduced").head(1)
        df = df[df["tokens"] != df["reduced"]]
        reduced_dict = dict(zip(df["reduced"], df["address"]))
        return reduced_dict

    # For cases where the dataframe only includes a single street number for a particular road,
    # add that street number to the addresses where it is missing,
    # e.g. {("North", "Jefferson", "Avenue"): "140 North Jefferson Avenue"}
    @staticmethod
    def generate_missing_street_num_dict(df):

        df["numeric_reduced"] = CurationWizard.smart_apply(df["tokens"],
                                                           lambda tokens: tuple(
                                                               token for token in tokens if token.isnumeric() == False)
                                                           )

        df = df[df["numeric_reduced"].isin(set(df["tokens"]))]
        df = df[df["numeric_reduced"] != ()]

        numeric_size_dict = df.groupby("numeric_reduced").size().to_dict()

        df = df[
            df["numeric_reduced"].apply(lambda tokens: numeric_size_dict[tokens])
            < 3
            ]

        df = df.sort_values("cnt", ascending=False).groupby("numeric_reduced").head(1)
        df = df[df["tokens"] != df["numeric_reduced"]]
        numeric_reduced_dict = dict(zip(df["numeric_reduced"], df["address"]))
        return numeric_reduced_dict

    # For cases where the address includes the same exact tokens but a switched order, map to the most common order
    # e.g. {("140", "Jefferson", "Avenue", "North"): "140 North Jefferson Avenue"}
    @staticmethod
    def generate_order_switch_dict(df):
        df["sorted"] = CurationWizard.smart_apply(df["tokens"], lambda tokens: tuple(sorted(tokens)))

        df["sorted_cnt"] = df.groupby("sorted").address.transform("count")
        df = df[df["sorted_cnt"] > 1]
        order_switch_dict = {}

        for index, row in df.iterrows():
            most_common_address_with_same_sorted = (
                df[df["sorted"] == row["sorted"]]
                .sort_values("cnt", ascending=False)
                .reset_index(drop=True)
                .iloc[0]
                .address
            )
            if row.address != most_common_address_with_same_sorted:
                order_switch_dict[row.tokens] = most_common_address_with_same_sorted

        return order_switch_dict

    @staticmethod
    def clean_final_street_address(street: str, roadnames):

        street = re.sub(rf"(\d+) Street ({'|'.join(roadnames)})", r"\1st \2", street)
        street = re.sub(rf"(\d+) Road ({'|'.join(roadnames)})", r"\1rd \2", street)
        street = re.sub(rf"(\d+) Th ({'|'.join(roadnames)})", r"\1th \2", street)
        street = re.sub(rf"(\d+) Nd ({'|'.join(roadnames)})", r"\1nd \2", street)

        return street

    @staticmethod
    def coalesce(tuples):
        return tuple(sum(map(list, tuples), start=list()))

    @staticmethod
    def handle_po_boxes(address: str):
        match = POBOX_PATTERN.match(address)
        if match:
            return f"PO Box {match.groups()[0]}"
        return address

    @staticmethod
    def handle_interstates(address: str):
        match = INTERSTATE_PATTERN.match(address)
        if match:
            return f"{match.groups()[0]} Interstate {match.groups()[1]}"
        return address

    @staticmethod
    def handle_intersections(address: str):
        lst_kw = "(st|street|ave|avenue|pier|blvd|rd|ln|lane|road|drive|dr|jn|junction|fm|farmtomarket|east|west|north|south)"

        # US - 191 & AZ - 264  |||   US - NUMBER AND & (2 OR 3 CHARS) - NUMBER(2 OR 3
        # Rule 1: Starts with US-NUMBER then any character any number of times
        if re.search("^US.?[0-9][0-9].*(and|&).*[0-9][0-9]$", address, re.IGNORECASE):
            return True

        # Hwy 59 & Conde St ||| hwy 59 & (CHARS)( kw - st / street / ave / avenue / peier / blvd / rd /
        # ln / lane / road / drive / dr / jn / junction / fm / farmtomarket)
        # Rule 2
        elif re.search(f"^(hwy|highway).?[0-9][0-9].*(and|&).*{lst_kw}$", address, re.IGNORECASE):
            return True

        # I 26 & Hwy 21 South
        # I(number, 2 or 3) & ({hwy - (number) / ave / junction / rd / lane….or (number-chars)
        # Rule 3
        elif re.search(f"^I.?[0-9][0-9].*(and|&).?(hwy|highway).?[0-9][0-9].?{lst_kw}$", address, re.IGNORECASE):
            return True

        # Rule 4
        # East Hwy 160 And Warrior Drive ||||
        elif re.search(f"^.*(hwy|highway).?[0-9][0-9].*(and|&).*$", address, re.IGNORECASE):
            return True

        # Rule 5: Starts with int
        elif address.lower().startswith("int"):
            return True

        else:
            return False

    @classmethod
    def handle_special_addresses(cls, address: str):
        special_cases = [
            cls.handle_po_boxes,
            cls.handle_interstates
        ]

        handler = compose(*special_cases)
        return handler(address)

    @staticmethod
    def get_county_from_zipcode(zipcodes: pd.Series):
        zips = zipcodes.unique()
        counties = [getattr(USZIP_ENG.by_zipcode(z), 'county', None) for z in zips]
        return zipcodes.map(dict(zip(zips, counties)))
