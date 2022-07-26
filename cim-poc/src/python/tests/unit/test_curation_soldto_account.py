import pandas as pd
import numpy as np
from curation_wizard import preprocess_loc_row
from curation_wizard import curation_soldto_account as csta
from scopes import ScopeSoldToAccountOrgIdZip3

EXPECTED_GROUP_COLS = [
    "OPS_STREET",
    "OPS_CITY",
    "OPS_STATE",
    "OPS_ZIP5",
    "ORGANIZATION_ID",
    "ORGANIZATION_NAME"
]

EXPECTED_GROUP_COLS_2 = [
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

scope = ScopeSoldToAccountOrgIdZip3(

    organization_ids=["3655db9e765cb6bd38eec559a64fcd38"],

    zip3s=["770"],

    incremental=False

    )


def test_group_cols():
    curation_soldto_account = csta.CurationSoldToAccount(scope)
    assert EXPECTED_GROUP_COLS == curation_soldto_account.GROUP_COLS
    assert EXPECTED_GROUP_COLS_2 == curation_soldto_account.GROUP_COLS_2


def test_pobox_locations():
    TEST_DF = pd.DataFrame({
        "STREET": ["P.O. Box 123", "PO BX 123", "P.O BX 456", "PO B0X 456"],        
    }).assign(
        STREET_NUM="",
        CITY="Nowhere",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(4)       
    )
    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 2
    assert result["OPS_STREET"].isin(["PO Box 123", "PO Box 456"]).all()


def test_wisconsin_streetnums():
    TEST_DF = pd.DataFrame({
        "STREET": [
            "N64W1024 Big Road",
            "N64W1024 Big Rd",
            "N 64W 1024 Big Road",
            "S96W4096 Sweet Way",
            "S 96 W 4096 Sweet Way",
            "S96 W4096 Sweet Wy",
            "N64 Mario Dr",
            "W40 Lubricant Rd",
            "S360 Switch St"
        ],        
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(9)
    )
    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 5
    assert result["OPS_STREET"].isin(["N64W1024 Big Road", "S96W4096 Sweet Way", "N64 Mario Drive", "W40 Lubricant Road", "S360 Switch Street"]).all()




def test_all_street_suffixes():
    suffix_dict = {**preprocess_loc_row.USPS_DICT, **preprocess_loc_row.ADDITIONS_DICT}
    base_address = "123 Main {suffix}"
    TEST_DF = pd.DataFrame({
        "STREET": [base_address.format(suffix=suffix) for suffix in suffix_dict.keys()]
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(len(suffix_dict))        
    )
    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    expected = [base_address.format(suffix=suffix) for suffix in set(suffix_dict.values())]

    assert set(result["OPS_STREET"].values).symmetric_difference(set(expected)) == set()
    assert len(result) == len(set(expected))
    #TODO: assert associations based on mapping between abbrevs (keys) and full spellings (values) of suffix_dict


def test_sublocation_exclusions():
    TEST_DF = pd.DataFrame({
        "STREET": ["45 Dock Road", "24 Big Building Way", "123 Sweet Suite Drive", "456 Warehouse Drive"]
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(4)        
    )

    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 4
    assert set(TEST_DF["STREET"]).symmetric_difference(set(result["OPS_STREET"])) == set()


def test_suffixless_roads():
    TEST_DF = pd.DataFrame({
        "STREET": ["123 I-24", "123 I 24", "123 Interstate 24", "123 Int 24", "1441 Broadway", "1441 broadway", "456 Highway 1", "456 hwy 1"]
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(8)        
    )

    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 3

    assert result.loc[0, "OPS_STREET"] == "123 Interstate 24"
    # assert set(result.loc[0, "DIM_LOCATION_ID"]) == set([0,1,2,3])

    assert result.loc[1, "OPS_STREET"] == "1441 Broadway"
    # assert set(result.loc[1, "DIM_LOCATION_ID"]) == set([4,5])

    assert result.loc[2, "OPS_STREET"] == "456 Highway 1"
    # assert set(result.loc[2, "DIM_LOCATION_ID"]) == set([6,7])

def test_highways():
    TEST_DF = pd.DataFrame({
        "STREET": ["123 US Hwy 10", "123 us Hiwy 10", "W456 Hway 9", "99 Hiway 9", "99 Highway 9", "W1234 US Hiway 1"]
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(6)        
    )

    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 4
    assert set(result["OPS_STREET"].values).symmetric_difference(
        set(["123 Us Highway 10", "W456 Highway 9", "99 Highway 9", "W1234 Us Highway 1"])
    ) == set()


def test_spanish_streetnames():

    expected = [
        "247 West El Camino Real",
        "340 El Camino Real South",
        "9558 Camino Ruiz",
        "661 West Calle Torres Blancas",
        "4781 West Calle Torim",
        "9 West Avenida Ramona",
        "5816 South Avenida Isla Contoy",
        "17309 Caminito Masada",
        "7281 East Caminito Feliz",
        "4415 East Cerrada Del Charro",
        "32106 Cerrada Del Coyote",
        "8254 West Circulo De Los Morteros",
        "817 North Calle Circulo",
        "806 Corte Entrada",
        "8045 Entrada De Luz East",
        "333 West Paseo Del Prado",
        "4751 South Paseo Don Rolando",
        "4655 West Placita Madre Isabella",
        "725 West Via Rancho Sahuarita",
        "13035 West Rancho Santa Fe Boulevard",
        "199 Vereda De Valencia",
        "526 Avenida De La Verda",
        "123 Pso Road",
        "456 North Cam Boulevard",
        "W64S128 Something Road",
    ]

    TEST_DF = pd.DataFrame({
        "STREET": [
            "247 W El Camino Real Ste 100",
            "247 West El Camino Real Suite 100",
            "340 El Camino Real S",
            "9558 Camino Ruiz",
            "661 W Calle Torres Blancas",
            "4781 W Calle Torim",
            "4781 W Cll Torim",
            "9 W Avenida Ramona",
            "5816 S Avenida Isla Contoy",
            "17309 Caminito Masada",
            "17309 Cmt Masada",
            "7281 E Caminito Feliz",
            "4415 E Cerrada Del Charro",
            "32106 Cerrada Del Coyote",
            "32106 Cer Del Coyote",
            "8254 West Circulo De Los Morteros",
            "8254 W Circulo De Los Morteros",
            "817 N Calle Circulo",
            "806 Corte Entrada",
            "8045 Entrada De Luz E",
            "333 W Paseo Del Prado",
            "4751 S Paseo Don Rolando",
            "4655 W Placita Madre Isabella",
            "725 W Via Rancho Sahuarita",
            "725 W Via Rch Sahuarita",
            "13035 W Rancho Santa Fe Blvd",
            "199 Vereda De Valencia",
            "199 Ver De Valencia",
            "526 Avenida De La Verda",
            "123 Pso Rd",           # should not be affected
            "456 N Cam Blvd",       # should not be affected
            "W64S128 Something Rd"  # should not be affected
        ]
    }).assign(
        STREET_NUM="",
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(32)        
    )

    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert set(expected) == set(result["OPS_STREET"].values)

def test_remove_garbage_suffixless_with_directions():

    expected = [
        "123 Alloy South",
        "456 West Broad",
        "999 Main South West"
    ]

    TEST_DF = pd.DataFrame({
        "STREET": [
            "123 Alloy S",
            "Alloy South",
            "123Alloy South blahblah",
            "456 West Broad",
            "W Broad",
            "Main SW",
            "999main southwest"
        ],
        "STREET_NUM": ["", "123", "", "", "456", "999", ""]
    }).assign(
        CITY="Somewhere",
        STATE="OH",
        ZIP5="43210",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(7)        
    )

    wizard = csta.CurationSoldToAccount()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    
    assert len(result) == len(expected)  # avoid dupes
    assert set(expected) == set(result["OPS_STREET"].values)
