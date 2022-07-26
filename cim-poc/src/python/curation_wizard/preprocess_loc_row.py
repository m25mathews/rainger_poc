import json
import os
import re

import nltk
import pandas as pd
import numpy as np
from nltk.tokenize.treebank import TreebankWordDetokenizer

IGNORED_CHARS = [
    "c/o",
    "C/O",
    ".",
    "-",
    ";",
    ",",
    "#",
    "@",
   # "&",
    ":",
    "/",
    '''"''',
    "(",
    ")",
]

# IGNORED_TOKENS = ["receiving", "recv", "rec", "rcving", "and", "amp"]
IGNORED_TOKENS = ["receiving", "recv", "rec", "rcving", "amp"]

SUBLOCATION_DICT = {
    "building": "Building",
    "bldng": "Building",
    "bldg": "Building",
    "blding": "Building",
    "bld": "Building",
    "dock": "Dock",
    "dk": "Dock",
    "do": "Dock",
    "doc": "Dock",
    "gate": "Gate",
    "gt": "Gate",
    "facility": "Facility",
    "warehouse": "Warehouse",
    "warhse": "Warehouse",
    "whse": "Warehouse",
    "unit": "Unit",
    "door": "Door",
    "plant": "Plant",
    "plnt": "Plant",
    "room": "Room",
    "rm": "Room",
    "apartment": "Apartment",
    "apt": "Apartment",
    "suite": "Suite",
    "ste": "Suite",
    "gstore": "Gstore",
}

SUBLOCATION_LVL2_LIST = ["Dock", "Room", "Apartment", "Suite", "Gate", "Door"]

NUMERIC_STREET_SUFFIXES = ["th", "nd", "st", "rd"]

DIRECTIONS_DICT = {
    "n": "North",
    "s": "South",
    "e": "East",
    "w": "West",
    "ne": "North East",
    "nw": "North West",
    "se": "South East",
    "sw": "South West",
    "north": "North",
    "south": "South",
    "east": "East",
    "west": "West",
    "northeast": "North East",
    "northwest": "North West",
    "southeast": "South East",
    "southwest": "South West",
}

DIRECTIONS_ES_DICT = {
    "nrte": "Norte",
}

ADDITIONS_DICT = {
    "rte": "Route",
    "rt": "Route",
    "dri": "Drive",
    "d": "Drive",
    "hw": "Highway",
    "hyw": "Highway",
    "ro": "Road",
    "cr": "County Road",
    "sr": "State Road",
    "tpke": "Turnpike",
    "fm": "Fm",
    # HACK: MAP ED TO ROAD
    # TODO: PERFORM REAL ADDRESS VALIDATION
    # "ed": "Road",
}

REMOVED_ABBREVIATIONS = ["via"]

COORD_REGEX = re.compile(r"^([NSEW])?\s?(\d+)?\s?([NSEW])\s?(\d+)\s?(.*)$", flags=re.IGNORECASE)

USPS_JSON_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "usps.json")
USPS_ES_JSON_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "usps_es.json")

with open(USPS_JSON_FILE) as json_file:
    USPS_DICT = {k.strip(): v.strip() for k, v in json.load(json_file).items() if k.strip() not in REMOVED_ABBREVIATIONS}

with open(USPS_ES_JSON_FILE) as json_file:
    USPS_ES_DICT = {k.strip(): v.strip() for k, v in json.load(json_file).items()}

SUFFIX_DICT = {**USPS_DICT, **ADDITIONS_DICT}
ADDRESS_DICT = {**SUFFIX_DICT, **DIRECTIONS_DICT}
ADDRESS_ES_DICT = {**USPS_ES_DICT, **DIRECTIONS_ES_DICT}

SPANISH_STREET_SUFFIXES = set(USPS_ES_DICT.values())
VALID_STREET_SUFFIXES = set(SUFFIX_DICT.values()) | SPANISH_STREET_SUFFIXES
VALID_DIRECTIONS = set(DIRECTIONS_DICT.values())

def ignore_characters(street, ignored_chars):
    for char in ignored_chars:
        street = street.replace(char, " ")
    street = re.sub(
        r"([0-9]+(\.[0-9]+)?)", r" \1 ", street
    )  # Adds space between numerical and alphabetical chars
    return street


def tokenize(street):
    tokens = nltk.word_tokenize(street)
    return tokens


def apply_token_dict(tokens, d):
    new_tokens = [d.get(token.lower(), token) for token in tokens]
    return new_tokens


def ignore_tokens(tokens, ignored_tokens):
    tokens = [token for token in tokens if token.lower() not in ignored_tokens]
    return tokens


def combine_street_num_and_tokens(street_num, tokens):
    if (not pd.isna(street_num)) & (street_num not in tokens):
        address_tokens = [street_num] + tokens
    else:
        address_tokens = tokens
    return address_tokens


def sublocation_split(tokens, sublocation_dict):
    N = len(tokens)
    if N == 0:
        return [], []
    left_to_right = False if tokens[0].lower() in sublocation_dict.keys() else True
    if left_to_right:
        for i, token in enumerate(tokens):
            if token.lower() in sublocation_dict.keys() and (i < N-1) and tokens[i+1].lower() not in USPS_DICT.keys():
                left = tokens[:i]     # could rename left -> address tokens
                right = tokens[i:]    # and right -> sublocation tokens
                break
            else:
                left = tokens
                right = []
    else:
        for i, token in reversed(list(enumerate(tokens))):
            if token.lower() in sublocation_dict.keys():
                left = tokens[i + 2 :]
                right = tokens[: i + 2]
                break
            else:
                left = tokens
                right = []
    return left, right


def sublocation_lvl2_split(tokens, sublocation_lvl2_list):
    if any(token in tokens for token in sublocation_lvl2_list):
        for i, token in enumerate(tokens):
            if token in sublocation_lvl2_list:
                left = tokens[:i]
                right = tokens[i:]
                break
    else:
        left = tokens
        right = []
    return left, right


def grab_relevant_tokens(known_tokens, potential_tokens, sublocation_dict):
    """Function applied to supplemental fields (dept, receiver, etc.)

    Args:
        known_tokens (list): sublocation tokens we found in
            the address string (sublocation_split functions)
        potential_tokens (list): tokenized supplemental field
        sublocation_dict (dict): see above

    Returns:
        list: sublocation tokens
    """
    grabbed = []
    for i, token in enumerate(potential_tokens):
        if (token in sublocation_dict.values()) & (token not in known_tokens):
            if (i + 1) < len(potential_tokens):
                # here we can control how large the building label can be
                if (len(potential_tokens[i + 1]) < 3) or (
                    potential_tokens[i + 1].isnumeric()
                ):
                    # here we pick up stuff like BLDG 10-2
                    # we only look one extra token se wecant get BLDG 1-0-2 (we'd get BLDG 1 0)
                    if ((i + 2) < len(potential_tokens)) and (
                        potential_tokens[i + 2].isnumeric()
                    ):
                        grabbed += potential_tokens[i : i + 3]
                    else:
                        grabbed += potential_tokens[i : i + 2]
            # if the sublocation field contains EXACTLY "XX BLDG" this will pick up BLDG XX
            elif len(potential_tokens) == 2:
                if len(potential_tokens[0]) < 3:
                    grabbed += potential_tokens[::-1]

    return grabbed


def detokenize(tokens):
    tokens = [
        " ".join([word.upper() if COORD_REGEX.match(word) else word.capitalize() for word in token.split(" ")]) for token in tokens
    ]
    address = TreebankWordDetokenizer().detokenize(tokens)
    return address


# This function aims to infer a normalized address (e.g. 1600 Pennsylvania Avenue),
# and two levels of sublocation information, lvl1 (e.g. Building A) and lvl2 (e.g. Dock 11)
# for a single row of dim_location data only based on what is included on that row.
def infer_address_and_sublocations(
    street_num,
    street,
    department="",
    attention="",
    supplemental="",
    receiver="",
):

    cleaned_street = ignore_characters(
        street, IGNORED_CHARS
    )  # Replaces listed characters with spaces, e.g. "Bldg.A" -> "Bldg A"

    tokens = tokenize(
        cleaned_street
    )  # Transforms strings into tokens "Jefferson Ave   Bldg A" -> ("Jefferson", "Ave", "Bldg", "A")

    tokens = ignore_tokens(tokens, IGNORED_TOKENS)

    street_tokens, sublocation_tokens = sublocation_split(
        tokens, SUBLOCATION_DICT
    )  # Aims to separate address from other information, e.g. Building name

    address_tokens = combine_street_num_and_tokens(street_num, street_tokens)

    address_tokens = handle_coordinate_street_num_tokens(address_tokens)

    d_address_tokens = apply_token_dict(
        address_tokens, ADDRESS_DICT
    )  # Transforms abbreviations into full words

    if set(d_address_tokens) & set(SUFFIX_DICT.values()) == set(): # no english streets
        d_address_tokens = apply_token_dict(
            d_address_tokens, ADDRESS_ES_DICT
        )

    d_sublocation_tokens = apply_token_dict(
        sublocation_tokens, SUBLOCATION_DICT
    )  # Transforms abbreviations into full words

    for string in [
        department,
        attention,
        supplemental,
        receiver,
    ]:  # Looks for sublocation_lvl1/lvl2 information from other columns, e.g. ("Building", "A")
        cleaned_string = ignore_characters(string, IGNORED_CHARS)
        s_tokens = tokenize(cleaned_string)
        d_s_tokens = apply_token_dict(s_tokens, SUBLOCATION_DICT)
        d_sublocation_tokens += grab_relevant_tokens(
            d_sublocation_tokens, d_s_tokens, SUBLOCATION_DICT
        )

    # Splits the sublocation information (e.g. Building, Dock, Apt) into the abstraction levels of higher and lower importance
    d_sublocation_lvl1_tokens, d_sublocation_lvl2_tokens = sublocation_lvl2_split(
        d_sublocation_tokens, SUBLOCATION_LVL2_LIST
    )

    address = detokenize(d_address_tokens)
    sublocation_lvl1 = detokenize(d_sublocation_lvl1_tokens)
    sublocation_lvl2 = detokenize(d_sublocation_lvl2_tokens)

    return address, sublocation_lvl1, sublocation_lvl2


def clean_and_tokenize_field(field, ignored_chars=None, dict_to_apply=None):
    cleaned_field = (
        ignore_characters(field, ignored_chars) if ignored_chars != None else field
    )
    raw_tokens = tokenize(cleaned_field)
    cleaned_tokens = (
        apply_token_dict(raw_tokens, dict_to_apply)
        if dict_to_apply != None
        else raw_tokens
    )
    tokens = tokenize(detokenize(cleaned_tokens))
    return tokens

def handle_coordinate_street_num_tokens(address_tokens):
    # todo: modify COORD_REGEX to avoid this case
    if any(ns in address_tokens for ns in NUMERIC_STREET_SUFFIXES):
        return address_tokens
    address = detokenize(address_tokens)
    address = COORD_REGEX.sub(r"\1\2\3\4 \5", address)
    return address.split()
