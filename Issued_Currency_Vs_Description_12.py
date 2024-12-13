import pandas as pd
import re
from tabulate import tabulate
import numpy
import json
import utils


df = pd.read_excel(r'C:\Users\gsravane\Downloads\DCS_Data.xlsx', sheet_name='Sheet1')

# associatestrings
associate_strings_list = [
    {'search_col': 'capitalStructureCleanedDescription', 'search_string': '.*sit.*', 'param_name': 'param1'},
    {'search_col': 'issuedCurrencyName', 'search_string': '.*Slovenian Tolar.*', 'param_name': 'param2'}
]

# parameters list
parameters_list = [
    {'name': 'param1', 'value': 'sit'},
    {'name': 'param2', 'value': 'Slovenian Tolar'}
]


# function to read parameters
def read_parameters(parameters_list):
    parameters_dict = {}
    for param in parameters_list:
        parameters_dict[param['name']] = param['value']
    return parameters_dict

# to read Search strings in associatestrings and parameters
def search_strings(associate_strings_list, parameters_dict, df):
    results = []
    for assoc_string in associate_strings_list:
        search_col = assoc_string['search_col']
        search_string = assoc_string['search_string']
        param_name = assoc_string['param_name']

        if param_name in parameters_dict:
            param_value = parameters_dict[param_name]
            filtered_df = df[df[search_col].str.contains(search_string) & (df[search_col] == param_value)]
            if not filtered_df.empty:
                results.append(filtered_df)

    return pd.concat(results, ignore_index=True)

def display_results(result_df, associate_strings_list):
    
    if len(result_df) > 0:
        
        # Add column names you want to display
        columns_to_display = ['tag', 'documentid', 'companyId', 'filingId', 'filingDateTime', 'peo', 'country', 'capitalStructureCleanedDescription', 'issuedCurrencyName', 'value', 'activeFlag', 'issuedasAmendmentFlag']  
        
        #result_table = result_df[columns_to_display].copy()

        # to Add the corresponding search_string to the result_table
        search_strings_dict = {assoc_string['search_col']: assoc_string['search_string'] for assoc_string in associate_strings_list}
        
        #result_table['Search String'] = result_table.apply(lambda row: search_strings_dict.get(row.name), axis=1)
        
        result_df['Search String'] = result_df.apply(lambda row: search_strings_dict.get(row['tag']), axis=1)

        result_table = result_df[columns_to_display]

        print(tabulate(result_table, headers='keys', tablefmt='pretty'))

# Implementing the above functions
if __name__ == "__main__":

    results=[]

    parameters_dict = read_parameters(parameters_list)

    result_df = search_strings(associate_strings_list, parameters_dict, df)

    display_results(result_df, associate_strings_list)