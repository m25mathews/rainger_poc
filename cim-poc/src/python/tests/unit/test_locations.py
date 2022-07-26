from upload_df import locations as loc

RESULT_OPS_SOLDTO_LOCATION_COLUMN_TYPE_DICT = {
        "ID": "VARCHAR(36) DEFAULT uuid_string()",
        "ORGANIZATION_NAME": "VARCHAR(200)",
        "ORGANIZATION_ID": "VARCHAR(32)",
        "OPS_STREET": "VARCHAR(200)",
        "OPS_CITY": "VARCHAR(200)",
        "OPS_STATE": "VARCHAR(200)",
        "OPS_LOC_NAME": "VARCHAR(200)",
        "OPS_ZIP5": "VARCHAR(200)",
        "LATITUDE": "FLOAT",
        "LONGITUDE": "FLOAT",
        "GEOCODE_ACCURACY": "FLOAT",
        "GEOCODE_LEVEL": "VARCHAR(255)"
    }


# def test_get_table_column_type_dict():
#
#     sold_to_loc = loc.UploadOpsSoldToLocation()
#     result = sold_to_loc.get_table_column_type_dict()
#     assert result == RESULT_OPS_SOLDTO_LOCATION_COLUMN_TYPE_DICT
