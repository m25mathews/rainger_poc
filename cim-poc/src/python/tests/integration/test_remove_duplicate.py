import pandas as pd
import upload_df


def test_ops_location():
    df = pd.DataFrame([["test1", "ZZZZ", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
                       ["test2", "ZZZ1", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]],
                       columns=['ID', 'OPS_LOC_NAME', 'OPS_STREET', 'OPS_CITY', 'OPS_STATE', 'OPS_ZIP5', 'OPS_SUBLOCATION', 'OPS_MARKER', 'ORGANIZATION_ID',
                               'ORGANIZATION_NAME', 'CURATED', 'LATITUDE', 'LONGITUDE', 'IS_BUILDING', 'IS_ADDRESS', 'IS_SITE', 'IS_RESIDENTIAL',
                               'GEOCODE_ACCURACY', 'GEOCODE_LEVEL'])

    uploader = upload_df.UploadOpsLocation()
    uploader.upload_ops_location_preload_temp_table(df)
    uploader.merge_ops_locations()


def test_ops_soldto_location():
    df = pd.DataFrame([["test1", "ZZZZ", None, None, None, None, None, None, None, None, None, None, None, None],
                       ["test2", "ZZZ1", None, None, None, None, None, None, None, None, None, None, None, None]],
                      columns=['ID', 'ORGANIZATION_NAME', 'ORGANIZATION_ID', 'SOLDTO_ACCOUNT', 'ACCOUNT_ID',
                                'OPS_STREET', 'OPS_CITY', 'OPS_STATE', 'OPS_LOC_NAME', 'OPS_ZIP5',
                                'LATITUDE', 'LONGITUDE',
                               'GEOCODE_ACCURACY', 'GEOCODE_LEVEL'])

    uploader = upload_df.UploadOpsSoldToLocation()
    uploader.upload_ops_location_preload_temp_table(df)
    uploader.merge_ops_locations()


if __name__ == "__main__":
    test_ops_location()
    test_ops_soldto_location()