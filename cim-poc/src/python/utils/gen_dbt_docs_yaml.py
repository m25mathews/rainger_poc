import pandas as pd
from pytest import skip
from sqlalchemy import column

"""
Usage:
1. Copy column information into a CSV file with header columns 'NAME', 'TYPE', 'Short Description', 'Long Description'
  a. Note: OK to leave Short and/or Long Description blank
2. Set the csv_file_path variable to the path to the CSV file from step 1
3. Set the out_file variable to the path to the file that the output will be printed to
4. Run $ python gen_dbt_docs_yaml.py
5. The output should be YAML formatted text, ready to be copied-and-pasted into the proper .yml files for generating DBT docs
6. Fix the indentation in the .yml files after pasting if necessary

Output Example (indentation level 3):
      - name: ID
        type: VARCHAR(10)
        description: ID Short Description: ID Long Description
      - name: CITY
        type: VARCHAR(50)
        description: Customer city name
"""

csv_file_path = 'temp.csv'                                        # CSV file to read from
out_file = 'out.txt'                                              # file to print to
cols = ['NAME', 'TYPE', 'Short Description', 'Long Description']  # columns to extract
indentation_level = 0                                             # desired YAML indentation level of output

file = open(out_file, 'w')

df = pd.read_csv(csv_file_path, usecols=cols)
df.fillna("", inplace=True)   # replace NaN with empty string

for index, row in df.iterrows():
    print('  ' * indentation_level + '- name: ' + str(row['NAME']), file=file)
    print('  ' * indentation_level + '  type: ' + row['TYPE'], file=file)

    short_str = str(row['Short Description'])
    long_str = str(row['Long Description'])

    if short_str.strip() == '' or long_str.strip() == '':
        desc = short_str + long_str
    else:
        desc = short_str + ': ' + long_str

    if '\n' in desc:
        desc = desc.replace('\n', '  \n               ' + '  ' * indentation_level) # add mkdn newlines 
        desc = '|\n               ' + '  ' * indentation_level + desc               # add pipe to signify multiline desc
    else:
        desc = desc.replace('"', r'\"')  # escape quotes in single-line descriptions
        desc = '\"' + desc + '\"'

    print('  ' * indentation_level + '  description: ' + desc, file=file)

file.close()
