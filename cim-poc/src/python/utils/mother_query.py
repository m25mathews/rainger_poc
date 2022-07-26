import os

schemaSuffix = ''

includeCount = True
includeDuplicates = True
includeMissingAssociations = True


run_id = os.getenv("RUN_ID", "")

templates = {
    'main': '''
With MetricData(Category, Entity, FullName, Metric, Result) as (
    {query}
)
select *, Round(Result/1000000) InMillions, '{run_id}' RUN_ID from MetricData order by Category, Entity, Metric;''',
    'count': '''select '{category}', '{entity}','{schema}.{table}', 'Count', count({count}) from {schema}.{table}''',
    'duplicates': '''select '{category}', '{entity}','{schema}.{table}', 'Duplicates', count(*) from 
        (select count(*) from {schema}.{table} group by {unique} having count(*) > 1)''',
    'missingAssociation': '''select '{category}', '{entity}','{schema}.{table}', 'Missing Association', count(*) from {schema}.{table} 
        left outer join {ref_schema}.{ref_table} 
        on {schema}.{table}.{column} =  {ref_schema}.{ref_table}.{ref_column} 
        where {ref_schema}.{ref_table}.{ref_column} is null '''
}

data = {
    'CIM': {
        'name': 'CIM',
        'tables': {
            'ORGANIZATION_SOLDTO_ACCOUNT': {
                'name': 'Account',
                'unique': 'account'
            },
            'LOCATION': {
                'name': 'Location',
                'unique': 'id'
            },
            'BRG_LOCATION': {
                'name': 'Location Hierarchy',
                'unique': 'id'
            },
            'ORGANIZATION': {
                'name': 'Organization',
                'unique': 'id'
            },
            'PERSON': {
                'name': 'Person',
                'unique': 'id'
            },
            'SOLDTO_LOCATION': {
                'name': 'Sold To Location',
                'unique': 'id'
            }
        }
    },
    'DNB': {
        'name': 'DNB',
        'tables': {
            'DIM_LOCATION_DNB': {
                'name': 'Location Dimension',
                'unique': 'id',
                'associations': [{
                    'ref_schema': 'CIM',
                    'ref_table': 'LOCATION',
                    'ref_column': 'DNB_DIM_LOCATION_ID',
                    'column': 'ID'
                }]
            },
            'DIM_CATEGORY_DNB': {
                'name': 'Category Dimension',
                'unique': 'id'
            },
            'FCT_DNB': {
                'name': 'DNB Fact',
                'unique': 'id'
            }
        }
    },
    'KEEPSTOCK': {
        'name': 'KEEPSTOCK',
        'tables': {
            'DIM_LOCATION_KEEPSTOCK': {
                'name': 'Location Dimension',
                'unique': 'id',
                'associations': [{
                    'ref_schema': 'CIM',
                    'ref_table': 'LOCATION',
                    'ref_column': 'ID',
                    'column': 'OPS_LOCATION_ID'
                }]
            },
            'FCT_PROGRAM_KEEPSTOCK': {
                'name': 'Keep Stock Fact',
                'unique': 'id'
            }
        }
    },
    'SELLER': {
        'name': 'SELLER',
        'tables': {
            'DIM_SELLER': {
                'name': 'Seller Dimension',
                'unique': 'id'
            },
            'DIM_TERRITORY': {
                'name': 'Territory Dimension',
                'unique': 'id'
            },
            'FCT_ASSIGNMENT': {
                'name': 'Seller Assignement Fact',
                'unique': 'account, dim_territory_id, dim_seller_id, from_dim_date_id, to_dim_date_id'
            }
        }
    },
    'SALES_ORDER': {
        'name': 'SALES_ORDER',
        'tables': {
            'DIM_LOCATION_SALES_ORDER': {
                'name': 'Location Sales Order Dimension',
                'unique': 'id',
                'associations': [{
                    'ref_schema': 'CIM',
                    'ref_table': 'LOCATION',
                    'ref_column': 'ID',
                    'column': 'OPS_LOCATION_ID'
                }]
            },
            'DIM_PARTY_SALES_ORDER': {
                'name': 'Party Sales Dimension',
                'unique': 'id'
            },
            'DIM_PRODUCT_SALES_ORDER': {
                'name': 'Product Sales Dimension',
                'unique': 'id'
            },
            'FCT_SALES_ORDER': {
                'name': 'Sales Order Fact',
                'unique': 'order_num, order_item, dim_product_id, dim_party_id, dim_location_id'
            }
        }
    },
    'SOLDTO_ACCOUNT': {
        'name': 'SOLDTO_ACCOUNT',
        'tables': {
            'DIM_LOCATION_SOLDTO': {
                'name': 'Location Dimension',
                'unique': 'id',
                'associations': [{
                    'ref_schema': 'CIM',
                    'ref_table': 'SOLDTO_LOCATION',
                    'ref_column': 'ID',
                    'column': 'OPS_LOCATION_ID'
                }]
            },
            'FCT_ACCOUNT_SOLDTO': {
                'name': 'Sold To Account Fact',
                'unique': 'account'
            }
        }
    }
}

output = []


def processTable(data, schema, table):

    run_id = os.getenv("RUN_ID", "")

    result = []
    table_dict = data[schema]['tables'][table]
    category = data[schema]['name']
    schema = schema + schemaSuffix

    if "name" in table_dict:
        entity = table_dict['name']
    else:
        entity = table

    # Count Query
    if includeCount:
        if "count" in table_dict:
            count = table_dict['count']
        else:
            count = "*"

        result.append(
            templates['count'].format(category=category, entity=entity, schema=schema, table=table, count=count))

    # Duplicate Query
    if includeDuplicates and "unique" in table_dict:
        unique = table_dict["unique"]
        if len(unique):
            result.append(templates['duplicates'].format(category=category, entity=entity, schema=schema, table=table,
                                                         unique=unique))

    # Missing Associations Query
    if includeMissingAssociations and "associations" in table_dict.keys():
        associations = table_dict["associations"]
        if len(associations):
            for association in associations:
                ref_schema = association['ref_schema']
                ref_table = association['ref_table']
                ref_column = association['ref_column']
                column = association['column']
                result.append(
                    templates['missingAssociation'].format(category=category, entity=entity, schema=schema, table=table,
                                                           ref_schema=ref_schema, ref_table=ref_table,
                                                           ref_column=ref_column, column=column))
    return result


def get_insert_sql():
    for schema in data:
        for table in data[schema]['tables']:
            output.extend(processTable(data, schema, table))
    query = "\n\tunion all\n\t".join(output)

    final_query = templates['main'].format(query=query, run_id=run_id)

    return final_query
