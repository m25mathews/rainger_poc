#Architecture
[Overview](./arch/architecture.md)

#Environment
This is an SQL/Python project POC that runs against Snowflake

Install the required libraries:
`
pip install -U -r ./src/python/requirements.txt
`

Copy the .env-template file and then rename the copy to .env. .env holds 
users and sensitive keys and will be ignored by git. Fill in the .env 
file with the relevant keys and users.
IMPORTANT: existing environment variables of the same name will take 
precidence over those in the .env file.

## Datasource
To (re)create, under src/sql folder:
1. Run ddl_schema_sales_order
2. Run ddl_schema_cim
3. Run populate_sales_order
4. Run populate_cim

To simply re-run the machine learning:

Run src/sql/re-run to clear the DIM_LOCATION -> OPS_LOCATION relationship
## Solution
1. Run src/python/ops_locations.py

This loads the correct operational location data into OPS_LOCATIONS table
2. Run src/python/no_label_fuzzy.py


