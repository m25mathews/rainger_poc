from curation_wizard import autocurate_df
from flag_residence import flag_residential_df
df = autocurate_df("IMPERIAL SUPPLIES LLC", "AR")
df = flag_residential_df(df)