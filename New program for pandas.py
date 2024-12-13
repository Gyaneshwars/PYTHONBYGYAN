import pandas as pd
import re
from tabulate import tabulate

# Step 3: Read Excel data using pandas
def read_excel_data(file_path, sheet_name):
    df = pd.read_excel(r'C:\Users\gsravane\Downloads\DCS_Data.xlsx', sheet_name='Sheet1')
    return df

# Step 4: Write a function to read parameters
def read_parameters(parameters_list):
    parameters_dict = {}
    for param in parameters_list:
        parameters_dict[param['name']] = param['value']
    return parameters_dict

# Step 5: Search strings in associatestrings and parameters
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
        columns_to_display = ['tag', 'filingId', 'filingDateTime', 'capitalStructureCleanedDescription', 'issuedCurrencyName', 'documentid', 'companyId', 'value']  # Add 'Search String' column
        result_table = result_df[columns_to_display]

        # Add the corresponding search_string to the result_table
        search_strings_dict = {assoc_string['search_col']: assoc_string['search_string'] for assoc_string in associate_strings_list}
        result_table['Search String'] = result_table.apply(lambda row: search_strings_dict.get(row.name), axis=1)

        #print("Results:")
        print(tabulate(result_table, headers='keys', tablefmt='pretty'))  #pretty

# Implement above function & change the fields:
if __name__ == "__main__":
    # Step 1 and 2: Sample associatestrings and parameters list
    associate_strings_list = [
        {'search_col': 'capitalStructureCleanedDescription', 'search_string': '.*sit.*', 'param_name': 'param1'},
        {'search_col': 'issuedCurrencyName', 'search_string': '.*Slovenian Tolar.*', 'param_name': 'param2'}
    ]

    parameters_list = [
        {'name': 'param1', 'value': 'sit'},
        {'name': 'param2', 'value': 'Slovenian Tolar'}
    ]

    results=[]
    # Step 3: Read Excel data
    excel_file_path = r'C:\Users\gsravane\Downloads\DCS_Data.xlsx'
    sheet_name = 'Sheet1'
    data_frame = read_excel_data(excel_file_path, sheet_name)

    # Step 4: Read parameters
    parameters_dict = read_parameters(parameters_list)

    # Step 5: Search strings in the data frame
    result_df = search_strings(associate_strings_list, parameters_dict, data_frame)

    display_results(result_df, associate_strings_list)
    
