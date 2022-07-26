from .base import UploadDFBase


class UploadOpsLocation(UploadDFBase):

    OPS_LOCATION_COLUMN_TYPE_DICT = {
        "ID": "VARCHAR(36) DEFAULT uuid_string()",
        "OPS_LOC_NAME": "VARCHAR(200)",
        "OPS_STREET": "VARCHAR(200)",
        "OPS_CITY": "VARCHAR(200)",
        "OPS_STATE": "VARCHAR(200)",
        "OPS_ZIP5": "VARCHAR(20)",
        "OPS_SUBLOCATION": "VARCHAR(200)",
        "OPS_MARKER": "VARCHAR(500)",
        "ORGANIZATION_ID": "VARCHAR(64)",
        "ORGANIZATION_NAME": "VARCHAR(50)",
        "CURATED": "BOOLEAN",
        "LATITUDE": "FLOAT",
        "LONGITUDE": "FLOAT",
        "IS_BUILDING": "BOOLEAN",
        "IS_ADDRESS": "BOOLEAN",
        "IS_SITE": "BOOLEAN",
        "IS_RESIDENTIAL": "BOOLEAN",
        "GEOCODE_ACCURACY": "FLOAT",
        "GEOCODE_LEVEL": "VARCHAR(255)"
    }

    def get_table_column_type_dict(self) -> dict:
        return UploadOpsLocation.OPS_LOCATION_COLUMN_TYPE_DICT

    def get_destination_table_name(self) -> str:
        return "LOCATION"

    def get_temp_table_name_prefix(self) -> str:
        return "LOCATION_PRELOAD"


class UploadOpsSoldToLocation(UploadDFBase):

    OPS_SOLDTO_LOCATION_COLUMN_TYPE_DICT = {
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

    def get_table_column_type_dict(self) -> dict:
        return UploadOpsSoldToLocation.OPS_SOLDTO_LOCATION_COLUMN_TYPE_DICT

    def get_destination_table_name(self) -> str:
        return "SOLDTO_LOCATION"

    def get_temp_table_name_prefix(self) -> str:
        return "SOLDTO_LOCATION_PRELOAD"

