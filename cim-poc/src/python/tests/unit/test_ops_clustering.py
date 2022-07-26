import pandas as pd


import ops_clustering as oc


TEST_OPS = pd.DataFrame({
    "ID": ["1", "2", "3"],
    "ORGANIZATION_NAME": ["TEST", "TEST", "TEST"],
    "ORGANIZATION_ID": ["1", "1", "1"],
    "OPS_LOC_NAME": ["123 Main Street", "456 Broad Street", "1989 Ehemann Drive"],
    "OPS_STREET": ["123 Main Street", "456 Broad Street", "1989 Ehemann Drive"],
    "OPS_CITY": ["Tullahoma", "Columbus", "Antioch"],
    "OPS_STATE": ["OH", "OH", "OH"],
    "OPS_ZIP5": ["37130", "43081", "94509"],
    "OPS_SUBLOCATION": ["", "" , "Building 1"],
    "ACCOUNT": ["08123", "08234", "08345"],
    "LATITUDE": [41.458933, 41.45889, 41.6911],
    "LONGITUDE": [-82.012957, -82.012960, -83.5123],
})

def tests_cluster_ops():
    result = oc.cluster_ops(TEST_OPS)

    assert len(result) == 1
    cluster = result.iloc[0]

    assert cluster.OPS_LOC_NAME.startswith("TEST OH P")
    assert set(cluster.children) == set(['1', '2'])
    assert cluster.ORGANIZATION_NAME == "TEST"
    assert cluster.ORGANIZATION_ID == '1'
    assert cluster.IS_SITE
    assert not cluster.IS_ADDRESS
    assert not cluster.IS_BUILDING