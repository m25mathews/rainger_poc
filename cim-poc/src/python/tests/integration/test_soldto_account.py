from scopes import ScopeSoldToAccountOrgIdZip3
from upload_df import UploadOpsSoldToLocation
import curation_wizard as cw

def test_location_size():
    organization_ids = ["7455bfab81dc6d8f251efa94b18dff1a", "20fd74179bff4322862c46463ed07e21"]
    zip3s = ["606", "104"]
    scope = ScopeSoldToAccountOrgIdZip3(organization_ids=organization_ids, zip3s=zip3s, incremental=False)

    print("--------------------ops-df====")
    ops_df = scope.get_ops()
    dim_df = scope.get_dim()
    print(ops_df.info())
    print(ops_df.head())
    print("-----------------------dim-df")
    print(dim_df.info())
    print(dim_df.head())
    _scopes = [scope]

    nscopes = len(_scopes)

    uploader = UploadOpsSoldToLocation()

    for i, scope in enumerate(_scopes):
        try:
            ops_df =cw.CurationSoldToAccount(scope).autocurate(
                simple_mode=False,
                auto_label=True,
            )

            uploader.upload_ops_location_preload_temp_table(ops_df)

        except Exception as e:
            print(e)

    uploader.merge_ops_locations()


if __name__ == '__main__':
    test_location_size()
