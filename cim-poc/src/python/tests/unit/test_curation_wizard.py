import src.python.curation_wizard as cw
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)


def test_add_missing_street_num():
    TEST_DF = pd.DataFrame({
        "STREET": 9 * ["123 Main Street"] + ["Main Street"],
    }).assign(
        STREET_NUM="",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )
    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 1
    assert result.OPS_STREET.str.startswith("123").all()


def test_extract_sublocations():
    TEST_DF = pd.DataFrame({
        "SUPPLEMENTAL": 8 * [""] + ["BLDG 1"] + [""],
        "ATTENTION": 7 * [""] + ["WAREHOUSE A"] + 2 * [""],
        "RECEIVER": 6 * [""] + ["WAREHOUSE B"] + 3 * [""],
        "DEPARTMENT": 5 * [""] + ["BUILDING 2"] + 4 * [""]
    }).assign(
        STREET_NUM="",
        STREET="123 MAIN STREET",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        DIM_LOCATION_ID=np.arange(10)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 5
    assert set(result.OPS_SUBLOCATION) == set(["", "Building 1", "Building 2", "Warehouse A", "Warehouse B"])


def test_order_switch():
    TEST_DF = pd.DataFrame({
        "STREET": 5 * ["1 North High Street"] + 3 * ["1 High Street North"] + 2 * ["1 High North Street"]
    }).assign(
        STREET_NUM="",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 1
    assert set(result.OPS_STREET) == set(["1 North High Street"])


def test_remove_garbage_after_suffix():
    TEST_DF = pd.DataFrame({
        "STREET": 4 * ["45 Slowpoke Lane"] + ["45 Slowpoke Lane asdfwaf"] + 3 * ["46 Slowpoke Lane"] + 2 * [
            "46 Slowpoke Lane thisisgarbage"]
    }).assign(
        STREET_NUM="",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 2
    assert set(result.OPS_STREET) == set(["45 Slowpoke Lane", "46 Slowpoke Lane"])


def test_missing_usps_suffix():
    TEST_DF = pd.DataFrame({
        "STREET": 8 * ["1989 Ehemann Drive"] + 2 * ["1989 Ehemann"]
    }).assign(
        STREET_NUM="",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 1
    assert set(result.OPS_STREET) == set(["1989 Ehemann Drive"])


def test_collect_dim_location_ids():
    TEST_DF = pd.DataFrame({
        "STREET": 2 * ["123 Main Street"] + 2 * ["123 Main Street Bldg 2"] + 2 * ["435 Broad Ave"] + \
                  2 * ["12319 Wide Road"] + 2 * ["67 Nowhere Drive"]
    }).sort_values(by="STREET").assign(
        STREET_NUM="",
        CITY="OLYMPIA",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 5
    for i, (_, row) in enumerate(result.sort_values(by="OPS_STREET").iterrows()):
        assert set(row.DIM_LOCATION_ID) == set((2 * i, 2 * i + 1))


def test_set_parent_locations():
    TEST_DF = pd.DataFrame({
        "STREET": ["123 Main Street"] + 4 * ["123 Main Street Building 1"] + \
                  ["345 Broad Avenue"] + 4 * ["345 Broad Avenue Warehouse 2"],
        "CITY": 5 * ["OLYMPIA"] + 5 * ["SEATTLE"],
    }).sort_values(by="STREET").assign(
        STREET_NUM="",
        STATE="WA",
        ZIP5="12345",
        ORGANIZATION_NAME="TEST",
        ORGANIZATION_ID=1,
        RECEIVER="",
        DEPARTMENT="",
        SUPPLEMENTAL="",
        ATTENTION="",
        DIM_LOCATION_ID=np.arange(10)
    )
    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 4
    assert (result.query("OPS_LOC_NAME == '123 Main Street Building 1'").MAIN_LOC_NAME == "123 Main Street").all()
    assert (result.query("OPS_LOC_NAME == '345 Broad Avenue Warehouse 1'").MAIN_LOC_NAME == "345 Broad Avenue").all()


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
    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)
    assert len(result) == 2
    assert result["OPS_STREET"].isin(["PO Box 123", "PO Box 456"]).all()
    assert set(result.loc[result["OPS_STREET"] == "PO Box 123", "DIM_LOCATION_ID"].values[0]) == set([0, 1])
    assert set(result.loc[result["OPS_STREET"] == "PO Box 456", "DIM_LOCATION_ID"].values[0]) == set([2, 3])


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
            "S360 Switch St",
            "490 S 22nd Street",
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
        DIM_LOCATION_ID=np.arange(10)
    )
    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 6
    assert result["OPS_STREET"].isin([
        "N64W1024 Big Road",
        "S96W4096 Sweet Way",
        "N64 Mario Drive",
        "W40 Lubricant Road",
        "S360 Switch Street",
        "490 South 22nd Street"
    ]).all()
    assert set(result.loc[result["OPS_STREET"] == "N64W1024 Big Road", "DIM_LOCATION_ID"].values[0]) == set([0, 1, 2])
    assert set(result.loc[result["OPS_STREET"] == "S96W4096 Sweet Way", "DIM_LOCATION_ID"].values[0]) == set([3, 4, 5])
    assert set(result.loc[result["OPS_STREET"] == "490 South 22nd Street", "DIM_LOCATION_ID"].values[0]) == set([9])


def test_all_street_suffixes():
    suffix_dict = {**cw.preprocess_loc_row.USPS_DICT, **cw.preprocess_loc_row.ADDITIONS_DICT}
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
    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    expected = [base_address.format(suffix=suffix) for suffix in set(suffix_dict.values())]

    assert set(result["OPS_STREET"].values).symmetric_difference(set(expected)) == set()
    assert len(result) == len(set(expected))
    # TODO: assert associations based on mapping between abbrevs (keys) and full spellings (values) of suffix_dict


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

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 4
    assert set(TEST_DF["STREET"]).symmetric_difference(set(result["OPS_STREET"])) == set()
    assert result['OPS_SUBLOCATION'].replace({'': pd.NA}).isnull().all()


def test_last_token_sublocation():
    TEST_DF = pd.DataFrame({
        "STREET": ["123 Main Street Building"]
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
        DIM_LOCATION_ID=np.arange(1)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 1
    assert result.loc[0, "OPS_STREET"] == "123 Main Street"
    assert result['OPS_SUBLOCATION'].replace({'': pd.NA}).isnull().all()


def test_suffixless_roads():
    TEST_DF = pd.DataFrame({
        "STREET": [
            "123 I-24",
            "123 I 24",
            "123 Interstate 24",
            "123 Int 24",
            "1441 Broadway",
            "1441 broadway",
            "456 Highway 1",
            "456 hwy 1",
            "600 merchants concourse"
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
        DIM_LOCATION_ID=np.arange(9)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == 4

    assert result.loc[0, "OPS_STREET"] == "123 Interstate 24"
    assert set(result.loc[0, "DIM_LOCATION_ID"]) == set([0, 1, 2, 3])

    assert result.loc[1, "OPS_STREET"] == "1441 Broadway"
    assert set(result.loc[1, "DIM_LOCATION_ID"]) == set([4, 5])

    assert result.loc[2, "OPS_STREET"] == "456 Highway 1"
    assert set(result.loc[2, "DIM_LOCATION_ID"]) == set([6, 7])

    assert result.loc[3, "OPS_STREET"] == "600 Merchants Concourse"
    assert set(result.loc[3, "DIM_LOCATION_ID"]) == set([8])


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

    wizard = cw.CurationSalesOrder()
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
            "123 Pso Rd",  # should not be affected
            "456 N Cam Blvd",  # should not be affected
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

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert set(expected) == set(result["OPS_STREET"].values)

    def assert_dim_locs(ops_street: str, dim_locs: list):
        mask = result["OPS_STREET"] == ops_street
        assert set(result.loc[mask, "DIM_LOCATION_ID"].iloc[0]) == set(dim_locs)

    assert_dim_locs("247 West El Camino Real", [0, 1])
    assert_dim_locs("340 El Camino Real South", [2])
    assert_dim_locs("9558 Camino Ruiz", [3])
    assert_dim_locs("661 West Calle Torres Blancas", [4])
    assert_dim_locs("4781 West Calle Torim", [5, 6])
    assert_dim_locs("9 West Avenida Ramona", [7])
    assert_dim_locs("5816 South Avenida Isla Contoy", [8])
    assert_dim_locs("17309 Caminito Masada", [9, 10])
    assert_dim_locs("7281 East Caminito Feliz", [11])
    assert_dim_locs("4415 East Cerrada Del Charro", [12])
    assert_dim_locs("32106 Cerrada Del Coyote", [13, 14])
    assert_dim_locs("8254 West Circulo De Los Morteros", [15, 16])
    assert_dim_locs("817 North Calle Circulo", [17])
    assert_dim_locs("806 Corte Entrada", [18])
    assert_dim_locs("8045 Entrada De Luz East", [19])
    assert_dim_locs("333 West Paseo Del Prado", [20])
    assert_dim_locs("4751 South Paseo Don Rolando", [21])
    assert_dim_locs("4655 West Placita Madre Isabella", [22])
    assert_dim_locs("725 West Via Rancho Sahuarita", [23, 24])
    assert_dim_locs("13035 West Rancho Santa Fe Boulevard", [25])
    assert_dim_locs("199 Vereda De Valencia", [26, 27])
    assert_dim_locs("526 Avenida De La Verda", [28])
    assert_dim_locs("123 Pso Road", [29])
    assert_dim_locs("456 North Cam Boulevard", [30])
    assert_dim_locs("W64S128 Something Road", [31])


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

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    result = wizard.autocurate_df(df, simple_mode=True)

    assert len(result) == len(expected)  # avoid dupes
    assert set(expected) == set(result["OPS_STREET"].values)

    def assert_dim_locs(ops_street: str, dim_locs: list):
        mask = result["OPS_STREET"] == ops_street
        assert set(result.loc[mask, "DIM_LOCATION_ID"].iloc[0]) == set(dim_locs)

    assert_dim_locs("123 Alloy South", [0, 1, 2])
    assert_dim_locs("456 West Broad", [3, 4])
    assert_dim_locs("999 Main South West", [5, 6])


def test_intersections():
    assert "Hello"
    expected = [
        "5th and Main",
        "2nd and 4th",
        'Broad and High'
        "123 Big Fat Road",
    ]

    TEST_DF = pd.DataFrame({
        "STREET": [
            "Corner of 5th and Main",
            "5th & Main",
            '2nd and 4th',
            '2nd & 4 th',
            'rf and hkh',
            'Building 21 Corner of Broad and High',
            "123 Big Fat Road",
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
        DIM_LOCATION_ID=np.arange(7)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df)

    result = wizard.autocurate_df(df, simple_mode=True)
    print(result)

    # assert len(result) == len(expected)  # avoid dupes
    # assert set(expected) == set(result["OPS_STREET"].values)

    def assert_dim_locs(ops_street: str, dim_locs: list):
        mask = result["OPS_STREET"] == ops_street
        assert set(result.loc[mask, "DIM_LOCATION_ID"].iloc[0]) == set(dim_locs)

    assert_dim_locs("5th and Main", [0, 1])
    assert_dim_locs("2nd and 4th", [2, 3])
    # assert_dim_locs("Broad and High", [5])
    # assert_dim_locs("123 Big Fat Road", [6])

    # assert result.loc[result["OPS_STREET"] == "Broad and High", "OPS_SUBLOCATION"] == "Building 21"


if __name__ == "__main__":
    assert "Hello"
    expected = [
        "5th and Main",
        "2nd and 4th",
        'Broad and High'
        "123 Big Fat Road",
    ]

    TEST_DF = pd.DataFrame({

        "STREET": [
            '180 Second & Pagosa St',
            '161 Town And Country Dr',
            '999 Town & Country Rd',
            'Center And Lemon Street',
            '27th & Pershing Road',
            '3915 S L And N Tpke Rd',
            'Rt. 5 Box305 Hwy82 & Ponderos',
            'Interstate 81 & 901 W',
            'P.O BOX 281',
            'P.O BOX 281, 180 morning cl lane',
            'Interstate 81 & 901 W , PO BOX 589',
            'Highway 80 & Kings Highway',
            '4880 Hills And Dales Rd Nw',
            '1627 William And Hayes Ln',
            '6th & Logan Ave N',
            'Bldg 4077 Seldon And Harris St',
            'Route 76 East & Route 50',
            'Rt 4 & Canton Blg T22a',
            'Rte 191 And Chipperfield Dr',
            'Sawgrass Marriott Golf Resort & Spa',
            'Sc Highway 170 & Sc Highway 46',
            'Science And Research Bld 1, Eas Dep'
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
        DIM_LOCATION_ID=np.arange(22)
    )

    wizard = cw.CurationSalesOrder()
    df = wizard.precurate_df(TEST_DF)
    print(df)

    result = wizard.autocurate_df(df, simple_mode=True)
    print(result)


    # assert len(result) == len(expected)  # avoid dupes
    # assert set(expected) == set(result["OPS_STREET"].values)

    def assert_dim_locs(ops_street: str, dim_locs: list):
        mask = result["OPS_STREET"] == ops_street
        assert set(result.loc[mask, "DIM_LOCATION_ID"].iloc[0]) == set(dim_locs)

    # assert_dim_locs("5th and Main", [0, 1])
    # assert_dim_locs("2nd and 4th", [2, 3])
    # assert_dim_locs("Broad and High", [5])
    # assert_dim_locs("123 Big Fat Road", [6])

    # assert result.loc[result["OPS_STREET"] == "Broad and High", "OPS_SUBLOCATION"] == "Building 21"