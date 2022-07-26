import pandas as pd
import numpy as np


from curation_wizard import preprocess_loc_row as pp

TEST_ADDRESS = "123  Main Street Dude"
TEST_ADDRESS_2 = "123main Street! Duder##"
TEST_ADDRESS_3 = "Main Street"
TEST_ADDRESS_4 = "123 Main Street Building 1"
TEST_ADDRESS_5 = "Building 1 123 Main Street"
TEST_ADDRESS_6 = "123 Main Street Dock 2"


def test_ignore_characters():
    result = pp.ignore_characters(TEST_ADDRESS_2, ["!", "#"])
    assert result == " 123 main Street  Duder  "


def test_tokenize():
    tokens = pp.tokenize(TEST_ADDRESS)
    assert set(tokens) == set(['123', 'Main', 'Street', 'Dude'])


def test_ignore_tokens():
    ignored = ['dude']
    tokens = pp.tokenize(TEST_ADDRESS)
    tokens = pp.ignore_tokens(tokens, ignored)
    assert set(tokens) == set(['123', 'Main', 'Street'])


def test_apply_token_dict():
    tokens = pp.tokenize(TEST_ADDRESS)
    abbrevs = {"street": "st"}
    tokens = pp.apply_token_dict(tokens, abbrevs)
    assert set(tokens) == set(['123', 'Main', 'st', 'Dude'])


def test_combine_street_num_and_tokens():
    tokens = pp.tokenize(TEST_ADDRESS_3)
    street_num = "123"
    combined = pp.combine_street_num_and_tokens(street_num, tokens)
    assert set(combined) == set(["123", "Main", "Street"])

    street_num = np.nan
    combined = pp.combine_street_num_and_tokens(street_num, tokens)
    assert set(combined) == set(["Main", "Street"])


def test_sublocation_split():
    sublocations = {"building": "Building"}

    # address first
    tokens = pp.tokenize(TEST_ADDRESS_4)
    address, building = pp.sublocation_split(tokens, sublocations)
    assert set(address) == set(["123", "Main", "Street"])
    assert set(building) == set(["Building", "1"])

    # building first
    tokens = pp.tokenize(TEST_ADDRESS_5)
    address, building = pp.sublocation_split(tokens, sublocations)
    assert set(address) == set(["123", "Main", "Street"])
    assert set(building) == set(["Building", "1"])


def test_sublocation_lvl2_split():
    sublocations = ["Dock"]

    tokens = pp.tokenize(TEST_ADDRESS_6)
    address, building = pp.sublocation_lvl2_split(tokens, sublocations)
    assert set(address) == set(["123", "Main", "Street"])
    assert set(building) == set(["Dock", "2"])


def test_grab_relevant_tokens():
    sublocations = {"building": "Building"}

    # building info already found in address
    tokens = pp.tokenize(TEST_ADDRESS_4)
    _, building = pp.sublocation_split(tokens, sublocations)
    grabbed = pp.grab_relevant_tokens(building, ("See", "George"), sublocations)
    assert len(grabbed) == 0

    # building info not in address, simulate it as tokens from a supplemental field
    tokens = pp.tokenize(TEST_ADDRESS)
    _, building = pp.sublocation_split(tokens, sublocations)
    grabbed = pp.grab_relevant_tokens(building, ('Building', '1'), sublocations)
    assert set(grabbed) == set(["Building", "1"])


def test_infer_address_and_sublocations():

    address, subloc1, subloc2 = pp.infer_address_and_sublocations(
        street_num=np.nan,
        street="123 Main Street Building 1",
        department="",
        attention="",
        receiver="",
        supplemental="Dock 2"
    )

    assert address == "123 Main Street"
    assert subloc1 == "Building 1"
    assert subloc2 == "Dock 2"


def test_clean_and_tokenize_field():

    result = pp.clean_and_tokenize_field(
        TEST_ADDRESS_2,
        ignored_chars=["#", "!"],
        dict_to_apply={"duder": "Dude"}
    )

    assert set(result) == set(["123", "Main", "Street", "Dude"])