import pandas as pd
import uszipcode


def test_state_from_zip():
    zipCodeSearchEngine = uszipcode.SearchEngine()
    df1 = pd.DataFrame({'address': ['address1'], 'state': ['AL'], 'zip': [60056]})
    df2 = pd.DataFrame({'address': ['address2'], 'zip': [60056]})
    df = pd.concat([df1, df2])
    df['state'] = df.apply(lambda row: zipCodeSearchEngine.by_zipcode(row['zip']).state if pd.isnull(row['state']) else row['state'], axis=1)
    assert df['state'].tolist() == ['AL', 'IL']