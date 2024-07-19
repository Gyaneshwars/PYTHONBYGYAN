import hashlib
import pandas as pd
import numpy as np
import time
import functools
import hashlib
import time
import functools
import datetime as dt
from functools import wraps
import json
from decimal import Decimal
import ast
import re
from datetime import datetime
import math
from sys import argv
import os
import warnings
from functools import reduce
warnings.simplefilter(action='ignore', category=FutureWarning)


PEO_FORMAT = '%Y/%m/%d'
extractedData_parsed = None
historicalData_parsed = None
isDataParsed = False

def get_dataItemIds_list(dataItemId_key, parameters):
    """
    Function to read LHS and RHS dataItemIds and return list of dataItemIds.
    """
    parameter_dataItemIds = []
    if dataItemId_key in parameters.keys():
        for element in parameters[dataItemId_key]:
            dataItemIds = element.get('value', "").split(",")
            for dataItemId in dataItemIds:
                dataItemId = str(dataItemId).strip()
                if (dataItemId not in parameter_dataItemIds) and (dataItemId != ''):
                    parameter_dataItemIds.extend([dataItemId])
                        
    return parameter_dataItemIds


def get_dataItemIds_list2(dataItemId_key, parameters):
    """
    Function to read LHS and RHS dataItemIds and return list of dataItemIds.
    """
    parameter_dataItemIds = []
    if dataItemId_key in parameters.keys():
        for element in parameters[dataItemId_key]:
            dataItemIds = element.get('value', "").split(",")
            for dataItemId in dataItemIds:
                dataItemId = str(dataItemId).strip()
                if (dataItemId != ''):
                    parameter_dataItemIds.extend([dataItemId])
                        
    return parameter_dataItemIds

def get_country_list(country_key, parameters):
    """
    Function to read LHS and RHS dataItemIds and return list of dataItemIds.
    """
    parameter_country = []
    if country_key in parameters.keys():
        for element in parameters[country_key]:
            countries = element.get('value', "").split(",")
            for country in countries:
                country = str(country).strip()
                if (country not in parameter_country) and (country != ''):
                    parameter_country.extend([country])
                        
    return parameter_country

def get_scale_list(scale_key, parameters):
    """
    Function to read scalelist and return list of scale.
    """
    parameter_scale = []
    if scale_key in parameters.keys():
        for element in parameters[scale_key]:
            scales = element.get('value', "").split(",")
            for scale in scales:
                scale = str(scale).strip()
                if (scale not in parameter_scale) and (scale != ''):
                    parameter_scale.extend([scale])
                        
    return parameter_scale

def get_parameter_value(parameters, key):
    """
    Function to return list of parameter values
    """
    try:
        param_value=[]
        for pam in parameters[key]:
            param_value.append(pam.get('value'))
    except:
        param_value=''

    return param_value

def get_parameter_id(parameters, key):
    """
    Function to return list of parameter ids.
    """
    try:
        param_id=[]
        for pam in parameters[key]:
            param_id.append(pam.get('id'))
    except:
        param_id=''

    return param_id 

def execute_operator(lhsval, inputvalue, operator):
    """
    Function to execute operator on two values
    """
    if (operator == "!="):
        return lhsval != inputvalue
    elif (operator == ">"):
        return lhsval > inputvalue
    elif (operator == "<"):
        return lhsval < inputvalue
    elif (operator == "=="):
        return lhsval == inputvalue
    elif (operator == ">="):
        return lhsval >= inputvalue
    elif (operator == "<="):
        return lhsval <= inputvalue
    else:
        return True

def str_to_float(x):
    """
    Function to convert string to float
    """
    try:
        x = float(x)
    except:
        x = 0.0
    return x

# def get_scaled_value(value, scale):
#     """
#     Function to scale value based on the scales_dict
#     """
#     scales_dict = {'actual': 1, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000, 'trillion': 1000000000000, 'tenth': 0.1, \
#                     'hundredth': 0.01, 'thousandth': 0.001, 'tenthousandth': 0.0001, 'dozen': 12, 'hundred': 100, 'lakh': 100000, \
#                     'crore': 10000000, 'bit': 12.5, 'score': 20, 'half': 0.5, 'pair': 2, 'gross': 144, 'ten': 10, 'myraid': 10000, \
#                     'millionth': 0.000001, 'billionth': 0.000000001, 'percendataItemIde': 100, 'fiveHundred': 500, 'hundred Million': 100000000,'none':1}
#     try:
#         if str(scale).lower() in scales_dict.keys():
#             return float(value) * scales_dict[str(scale).lower()]
#         else:
#             return 0
#     except:
#         return 0

def get_scaled_value(fullyAdjValue, scaleId):
    """
    Function to scale value based on the scales_dict
    """
    scales_dict = {5: 0.001,6: 0.0001,7: 1000000000000,8: 0.1,9: 100,10: 100000,11: 10000000,12: 10,13: 10000,14: 0.00001,16: 100,17: 0.001,18: 1,19: 1000,20: 173.913,21: 0.1739,22: 0.0001739,23: 7.33333,24: 7333.3333,25: 7333333.3,26: 0.00002381,27: 0.02381,28: 23.81,29: 0.0001667,30: 0.1667,31: 166.6667,32: 0.005885778,33: 5.886,34: 5886,35: 0.001,36: 1,37: 0.0001667,4: 0.01,3: 1,2: 1000,1: 1000000000,0: 1000000,38: 0.1667,39: 1000,40: 0.000000167,41: 166.6667,42: 157.68333,43: 0.15768,44: 0.00015768,45: 7.0922,46: 7092.1983,47: 7092198.3333,48: 0.001,49: 1,50: 1000,52: 0.00000016667,53: 0.0001667,54: 0.1666666667,55: 166.6666667,56: 166666.666667,57: 166666.6667,58: 166666.6667,59: 7.3333333,60: 7333.3333,61: 7333333.3,62: 100000000,63: 0.0073333333,64: 0.00588577778333333,65: 5.88577778333333,66: 5885.77778333333,67: 0.001,68: 1,69: 1000,70: 1000000,71: 0.0010978,72: 1.0978,73: 1097.8,74: 1097800,75: 35.29523,76: 35295.23,77: 35295230,78: 35295230000,79: 32.01963,80: 32019.63,81: 32019630,82: 32019630000,83: 3.586198,84: 3586.198,85: 3586198,86: 3586198000,87: 0.03529523,88: 0.01601,89: 16.01,90: 16010,91: 16010000,92: 0.0000353,93: 0.0353,94: 35.3,95: 35300,96: 35.29523,97: 35295.23,98: 35295230,99: 0.001,100: 0.01,101: 0.1,102: 1,103: 10,104: 100,105: 1000,106: 10000,107: 1000000,108: 0.00160934,109: 0.0160934,110: 0.160934,111: 1.60934,112: 16.0934,113: 160.934,114: 1609.34,115: 16093.4,116: 1609340,118: 0.00006,119: 0.00001,120: 0.001,121: 1,122: 0.006,123: 6,124: 1000,125: 0.000006,126: 6000,127: 6000000,128: 0.00006,129: 0.00001,-1:1}
    try:
        if scaleId in scales_dict.keys():
            return float(fullyAdjValue) * scales_dict[(scaleId)]
        else:
            return 0
    except:
        return 0

def correct_parameter_bools(df):
    """
    Function to correct the 'ALL' parameter booleans.
    
    This function corrects the 'ALL' parameter boolean in output dataframe in case the child row values are empty 
    for a particular PEO and FPO.

    Parameters
    ----------
    1. df: Pandas DataFrame
        df contains the parsed json output.
    
    Returns
    --------
    1. df: Pandas DataFrame
        Correct pandas dataframe with corrections made for 'ALL' parameter boolean.
    """
    # Reset Dataframe indices.
    df = df.reset_index(drop=True)

    # Get all child rows with null values.
    null_child = df[(df['is_child_row']==1) & (pd.to_numeric(df['value'], errors='coerce').isin([0.0, np.nan]))]

    if len(null_child) > 0:
        # Groupby child rows based on peo, fpo and parent instance and  
        # consider only those instances where count is equal to number of children of that parent.
        null_child = null_child[null_child.groupby(['peo', 'fpo', 'parent_instance'])['peo'].transform('size') == null_child['num_children']]

        if len(null_child) > 0:
            # Get the indexes for which all_bool needs to be changed to 0.
            remove_indexes = list(null_child.index)

            # Get indexes of parent rows corresponding to children with null values.
            null_child = null_child.drop_duplicates(subset=['peo', 'fpo', 'parent_instance'])
            null_child['instance'] = null_child['parent_instance']
            new_indexes = list(df.merge(null_child, on=['peo', 'fpo', 'instance'], how='left', indicator=True).query('_merge == "both"').index)

            # Change all_bool accoridngly.
            df.loc[remove_indexes, 'all_bool'] = 0
            df.loc[new_indexes, 'all_bool'] = 1
    
    return df


def currency_converter(currency_from, currency_to , value):

    if (currency_from==currency_to):
        return value*1
    if (currency_from in list(currencyConversion_parsed['from'])):
        temp = currencyConversion_parsed[currencyConversion_parsed['from']==currency_from]
        if currency_to in list(temp['to']):
            factor=temp[(temp['from']==currency_from) & (temp['to']==currency_to)]['factor'].iloc[0]
            return value*factor
    elif (currency_to in list(currencyConversion_parsed['from'])):
        temp = currencyConversion_parsed[currencyConversion_parsed['from']==currency_to]
        if currency_from in list(temp['to']):
            factor=1/(temp[(temp['from']==currency_to) & (temp['to']==currency_from)]['factor'].iloc[0])
            return value*factor

def get_metadata_items(metadata_rows, metadata, num_rows):
    """
    Function to get metadata values from metadata dictionary.

    Takes input the metadata dictionary, and returns dictionary of metadata values to be extracted.

    Parameters
    ---------
    1. metadata_rows: dict
        dict with keys as metadata keys to extract and values as list of extracted values.
    2. metadata: dict
        metadata dictionary of a single history in json.
    3. num_rows: int
        Count of values extracted from the json.
    
    Returns
    -------
    1. metadata_rows: dict
        metadata dict with appended values for the particular metadata
    """
    for key in metadata_rows.keys():
        # Append metadata value to metadata key list num_rows times to maintain output df length.
        metadata_rows[key].extend([metadata.get(key)]*num_rows)
    return metadata_rows




def parse_extracted_data(extracted_data, convert_to_df = False):
    """
    Function to parse extracted data jsons.
    Boolean convert_to_df is True if only extracted_data is to be parsed.
    By default convert_to_df is False, so that complete output of history can be converted to DF in a single command. 
    Parameters
    ----------
    1. extracted_data: dict
        Dict of extracted data from the json to be parsed.
    2. convert_to_df: bool
        Boolean to check if the output is to be converted to a dataframe or returned as list of rows.
    
    Returns
    -------
    1. output: list or DataFrame
        Contains rows of parsed extracted data.
    """
    #print(extracted_data)
    output = []
    # Define output df column names in case empty DF is to be initialized.
    column_names = ["primaryParentFlag", "tradingItemName", "tradingItemId","periodTypeId","fiscalQuarter","fiscalYear","actualizedDate","periodEndDate","estimatePeriodId","parentFlag","accountingStandardDesc","auditTypeId","auditTypeName","fiscalChainSeriesId","splitFactor","team","userName","securityName","TickerSymbol","exchangeSymbol","tidCurrency","lastTradedDate","tradingItemStatus","tidPrimaryFlag","peo","value","currency","scale","consValue","consNotes","consScale","consCurrency"]
    # Loop through dataItemIds, parent instance and childrows in-case cildwors are present.
    for dataItemId in extracted_data:
        
        for parent_id, parent_inst in extracted_data[dataItemId].items():
            # Ignore parent instances for with isChildRow is True. This is an error in JSON.
            #if ('values' in parent_inst.keys()) and (not parent_inst['isChildRow']):
            if ('values' in parent_inst.keys()):
                values = parent_inst['values']
                # temp_dict has datapoints common for all values in values list. Append temp_dict to each value in values. 
                temp_dict = {'dataItemId': parent_inst['dataItemId'], 'description': parent_inst.get('description'),'dataItemFlag': parent_inst.get('dataItemFlag')}
                [value.update(temp_dict) for value in values]
                output.extend(values)
    # conver_to_df if parsing is not being done for historicalData.
    if convert_to_df:
        output = pd.DataFrame(output)
        if len(output)==0:
            output = pd.DataFrame([], columns=column_names)
        if len(output) > 0:
            output['value_scaled'] = output.apply(lambda row: get_scaled_value(row['fullyAdjValue'], row['scaleId']), axis=1)
        # Correct all_bool for null chil value cases.
        #output = correct_parameter_bools(output)
    return output

def parse_historical_data(historicalData):
    """
    Function to parse complete historicalData.
    
    Loops over each history and calls the function to parse extracted data.
    Parameters
    ----------
    1. historicalData: list
        List of dictionaries of complete historical data.
    
    Returns
    -------
    1. output: Pandas DataFrame
        Dataframe of parsed historical data rows along with metadata and some custom columns.
    """
    # Define metadata_column_names. Can add to the list of more keys need to be extracted.
    # Define all column names and initialize metadata_columns.
    metadata_column_names = ["versionId","companyId","researchContributorId","companyName","industry","industryId","country","latestActualizedPeo","latestPeriodType","latestPeriodYear","fiscalYearEnd","filingDate","language","heading","versionFormat","documentId","sourceId","companyrank","priorityid","tier","primaryEpsFlag","feedFileId"]
    metadata_columns = {name:[] for name in metadata_column_names}
    extracted_data_columns =["primaryParentFlag", "tradingItemName", "tradingItemId","periodTypeId","fiscalQuarter","fiscalYear","actualizedDate","periodEndDate","estimatePeriodId","parentFlag","accountingStandardDesc","auditTypeId","auditTypeName","fiscalChainSeriesId","splitFactor","team","userName","securityName","TickerSymbol","exchangeSymbol","tidCurrency","lastTradedDate","tradingItemStatus","tidPrimaryFlag","peo","value","currency","scale","consValue","consNotes","consScale","consCurrency"]
    column_names = extracted_data_columns + metadata_column_names + ['periodEndDate_parsed']
    output = []
    # Loop over all histories and call parse_extracted_data.
    # For each history, parse the metadata and update metadata columns.
    for hist in historicalData:
        parsed_hist = parse_extracted_data(hist['extractedData'])
        output.extend(parsed_hist)
        metadata_columns = get_metadata_items(metadata_columns,  hist['metadata'], len(parsed_hist))
    output = pd.DataFrame(output)
    if len(output) > 0:
        output['value_scaled'] = output.apply(lambda row: get_scaled_value(row['fullyAdjValue'], row['scaleId']), axis=1)
    # Merge metadata_columns with output dataframe.
    output = pd.concat([output, pd.DataFrame(metadata_columns)], axis=1)
    if len(output)==0:
        return pd.DataFrame([], columns=column_names)
    # Parse periodEndDate and peo.
    output['periodEndDate_parsed'] = pd.to_datetime(output['periodEndDate'])
    #output['peo_parsed'] = pd.to_datetime(output['peo'])
    # Correct all_bool for null chil value cases.
    #output = correct_parameter_bools(output)
    return output

def parse_conversion_data(conversionData, convert_to_df = False):
    """
    Function to parse extracted data jsons.
    Boolean convert_to_df is True if only extracted_data is to be parsed.
    By default convert_to_df is False, so that complete output of history can be converted to DF in a single command. 
    Parameters
    ----------
    1. extracted_data: dict
        Dict of extracted data from the json to be parsed.
    2. convert_to_df: bool
        Boolean to check if the output is to be converted to a dataframe or returned as list of rows.
    Returns
    -------
    1. output: list or DataFrame
        Contains rows of parsed extracted data.
    """
    output = []
    # Define output df column names in case empty DF is to be initialized.
    column_names = ["from","to","factor"]
    # Loop through dataItemIds, parent instance and childrows in-case cildwors are present.
    for dataItemId in conversionData:
        for parent_id, parent_inst in conversionData[dataItemId].items():
            if parent_id=='filingDate':
                filingDate=parent_inst
            if parent_id=='values':
                values=parent_inst
                temp_dict={'filingDate': filingDate}
                [value.update(temp_dict) for value in values]
                output.extend(values)
    # conver_to_df if parsing is not being done for historicalData.
    if convert_to_df:
        output = pd.DataFrame(output)
        if len(output)==0:
            output = pd.DataFrame([], columns=column_names)
    return output

def measure_time(f):

    @functools.wraps(f)
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        print( "{0} {1:2.2f} sec".format(f.__name__, te-ts))
        return result

    return timed


def fetch_comp(a, historicalData, filingMetadata):
    import datetime
    def months(d1, d2):
        return d1.month - d2.month + 12*(d1.year - d2.year)
    filings_list=[]
    ref_filings={}
    comp_type=a

    if comp_type==1:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]=='Reg' \
                    and filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and filingMetadata['metadata']['fiscalQuarter']==hist['metadata']['fiscalQuarter']:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==2:
        pass

    elif comp_type==3:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:2]=='PR' \
                    and filingMetadata['metadata']['periodType']=='ANNUAL' \
                    and filingMetadata['metadata']['docType'][:3]== hist['metadata']['docType'][:3] \
                    and filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and filingMetadata['metadata']['periodType']!=hist['metadata']['periodType'] \
                    and hist['metadata']['fiscalQuarter']=='4' \
                    and filingMetadata['metadata']['fiscalQuarter']=='4':
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==4:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:2]=='PR' \
                    and filingMetadata['metadata']['docType']!=hist['metadata']['docType'] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],PEO_FORMAT))) < 15):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==5:
        for hist in historicalData:
            if ('amd' in filingMetadata['metadata']['docType'].lower() or 'amend' in filingMetadata['metadata']['docType'].lower()) \
                    and filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and ('amd' not in hist['metadata']['docType'].lower() and 'amend' not in filingMetadata['metadata']['docType'].lower()):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==6:
        for hist in historicalData:
            if ('amd' in filingMetadata['metadata']['docType'].lower() or 'amend' in filingMetadata['metadata']['docType'].lower()) \
                    and filingMetadata['metadata']['docType'][:3]== hist['metadata']['docType'][:3] \
                    and filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType']:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==7:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and (((int(filingMetadata['metadata']['fiscalQuarter']) - int(hist['metadata']['fiscalQuarter']) == 1 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                    PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                            PEO_FORMAT))) < 5) \
                          or (filingMetadata['metadata']['fiscalQuarter'] == '1' and hist['metadata']['fiscalQuarter'] == '3' \
                              and (abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                         PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                 PEO_FORMAT))) >= 12 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                               PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                       PEO_FORMAT))) <= 15))) \
                         or (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                        PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                PEO_FORMAT))) >= 11 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                              PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                                                                      PEO_FORMAT))) <= 15)) \
                    and ((7 in filingMetadata['metadata']['filingtypeid'] and filingMetadata['metadata']['filingtypeid'] == hist['metadata']['filingtypeid'] or (7 not in filingMetadata['metadata']['filingtypeid']))):
                filings_list.append(hist['metadata']['filingId'])



    elif comp_type==8:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]== hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                    PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                            PEO_FORMAT))) < 15):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==9:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT))) < 14:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==10:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>=datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                        PEO_FORMAT):
                filings_list.append(hist['metadata']['filingId'])



    elif comp_type==11:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>=datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                        PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType'] != 'ANNUAL' \
                    and filingMetadata['metadata']['periodType']!=hist['metadata']['periodType'] \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT))) < 12:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==12:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType'] == 'ANNUAL' \
                    and filingMetadata['metadata']['periodType']!=hist['metadata']['periodType'] \
                    and filingMetadata['metadata']['fiscalQuarter']=='4' \
                    and hist['metadata']['fiscalQuarter']=='3' \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT))) < 5:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==13:

        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and (( filingMetadata['metadata']['periodType']==hist['metadata']['periodType']) \
                         or ( filingMetadata['metadata']['periodType'] == 'ANNUAL' and hist['metadata']['periodType'] != 'ANNUAL')) \
                    and ((int(filingMetadata['metadata']['fiscalQuarter']) - int(hist['metadata']['fiscalQuarter']) == 1 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                   PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                           PEO_FORMAT))) < 5) \
                         or (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] \
                             and filingMetadata['metadata']['periodType'] == hist['metadata']['periodType'] \
                             and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                            PEO_FORMAT))) >= 12 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                                                          PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                                  PEO_FORMAT))) <= 15)) \
                    and ((7 in filingMetadata['metadata']['filingtypeid'] and filingMetadata['metadata']['filingtypeid'] == hist['metadata']['filingtypeid'] or (7 not in filingMetadata['metadata']['filingtypeid']))):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==14:

        for hist in historicalData:
            if datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                              PEO_FORMAT) \
                    and ((filingMetadata['metadata']['periodType']==hist['metadata']['periodType']) \
                         or (filingMetadata['metadata']['periodType'] == 'ANNUAL' and hist['metadata']['periodType'] != 'ANNUAL')) \
                    and ((int(filingMetadata['metadata']['fiscalQuarter']) - int(hist['metadata']['fiscalQuarter']) == 1 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                   PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                           PEO_FORMAT))) < 5) \
                         or (filingMetadata['metadata']['fiscalQuarter'] == '1' and hist['metadata']['fiscalQuarter'] == '3' and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                       PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                               PEO_FORMAT))) >= 5 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                                            PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                                                                                    PEO_FORMAT))) <= 7) \
                         or (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                        PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                PEO_FORMAT))) >= 12 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                              PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                                                                      PEO_FORMAT))) <= 15)) \
                    and (( 7 in filingMetadata['metadata']['filingtypeid'] and filingMetadata['metadata']['filingtypeid'] == hist['metadata']['filingtypeid'] or (7 not in filingMetadata['metadata']['filingtypeid']))):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==15:

        for hist in historicalData:
            if filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==16:

        for hist in historicalData:
            if abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                          PEO_FORMAT)))<25:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==17:

        for hist in historicalData:
            if (hist['metadata']['amendmentFlag'] == 0 or 7 in hist['metadata']['filingtypeid']) \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and ((filingMetadata['metadata']['periodType'] == hist['metadata']['periodType']) \
                         or (filingMetadata['metadata']['periodType'] == 'ANNUAL' and hist['metadata']['periodType'] != 'ANNUAL') \
                         and ((int(filingMetadata['metadata']['fiscalQuarter']) - int(hist['metadata']['fiscalQuarter']) == 1 \
                               and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                         PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                 PEO_FORMAT))) < 5) \
                              or (filingMetadata['metadata']['fiscalQuarter'] == '1' and hist['metadata']['fiscalQuarter'] == '3' \
                                  and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                            PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                    PEO_FORMAT))) >= 5 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                 PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                         PEO_FORMAT))) <= 7) \
                              or (b.fiscalQuarter == c.fiscalQuarter and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                               PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                       PEO_FORMAT))) >= 11 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                                                                     PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                                                             PEO_FORMAT))) <= 13)) \
                         and ((7 in filingMetadata['metadata']['filingtypeid'] \
                               and filingMetadata['metadata']['filingtypeid'] == hist['metadata']['filingtypeid']) or (7 not in filingMetadata['metadata']['filingtypeid']))):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==18:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]=='Reg' \
                    and filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and filingMetadata['metadata']['fiscalQuarter']==hist['metadata']['fiscalQuarter']:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==19:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT))) < 7 \
                    and filingMetadata['metadata']['fiscalQuarter'] in ['2','3'] \
                    and int(hist['metadata']['fiscalQuarter'])== int(filingMetadata['metadata']['fiscalQuarter']) - 1:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==20:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType'] == 'ANNUAL' \
                    and filingMetadata['metadata']['periodType']!=hist['metadata']['periodType'] \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT))) < 12:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==21:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]=='Reg' \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and (filingMetadata['metadata']['fiscalQuarter'] == hist['metadata']['fiscalQuarter'] and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                    PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                            PEO_FORMAT))) < 15):
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==22:

        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and filingMetadata['metadata']['amendmentFlag'] == hist['metadata']['amendmentFlag'] \
                    and ((filingMetadata['metadata']['periodType'] == 'ANNUAL' and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                         PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                 PEO_FORMAT))) < 26) \
                         or (abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                        PEO_FORMAT))) < 8)):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==23:
        for hist in historicalData:
            if filingMetadata['metadata']['docType'][:3]== hist['metadata']['docType'][:3] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and ((int(filingMetadata['metadata']['fiscalQuarter']) - int(hist['metadata']['fiscalQuarter']) == 1 \
                          and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                         PEO_FORMAT))) < 5) \
                         or (filingMetadata['metadata']['fiscalQuarter']==hist['metadata']['fiscalQuarter'] \
                             and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                             and (abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                        PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                PEO_FORMAT))) >= 12 and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],
                                                                                                                                                                              PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                                                                                      PEO_FORMAT))) <= 15))):
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==24:
        for hist in historicalData:
            if abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                          PEO_FORMAT)))<=13:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==25:
        for hist in historicalData:
            if filingMetadata['metadata']['periodEndDate']==hist['metadata']['periodEndDate'] \
                    and abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                                   PEO_FORMAT)))<=12:
                filings_list.append(hist['metadata']['filingId'])


    elif comp_type==26:
        for hist in historicalData:
            if 7 in filingMetadata['metadata']['filingtypeid'] \
                    and datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT)>datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                       PEO_FORMAT) \
                    and filingMetadata['metadata']['periodType'] == 'ANNUAL' \
                    and months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                               PEO_FORMAT))<=12:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==27:
        for hist in historicalData:
            if abs(months(datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'], PEO_FORMAT), datetime.datetime.strptime(hist['metadata']['periodEndDate'],
                                                                                                                                          PEO_FORMAT)))<=13 \
                    and filingMetadata['metadata']['periodType']==hist['metadata']['periodType'] \
                    and filingMetadata['metadata']['docType'][:3]!= hist['metadata']['docType'][:3]:
                filings_list.append(hist['metadata']['filingId'])

    elif comp_type==28:
        for hist in historicalData:
            if filingMetadata['metadata']['periodType']=='QUARTERLY' \
                    and hist['metadata']['periodType']=='ANNUAL' \
                    and filingMetadata['metadata']['periodEndDate']>hist['metadata']['periodEndDate']:
                filings_list.append(hist['metadata']['filingId'])


    return filings_list

def add_method(cls):
    def decorator(func):
        from functools import wraps
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(*args, **kwargs)
        setattr(cls, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func # returning func means func can still be used normally
    return decorator

class Validation(object):
    def generateHash(self, ruleName, result):
        return hashlib.sha256((ruleName + str(result)).encode()).hexdigest()

    def appendError(self, errorMap, errorMessage, ruleName, associationId):
        if errorMessage:
            self.appendMessages(errorMap, {"error": errorMessage}, ruleName, associationId)

    def appendMessage(self, errorMap, result, ruleName, associationId):
        if result:
            errorobject = {"ruleName": ruleName, "isError": True, "result": result, "hash": self.generateHash(ruleName, result), "associationId": associationId}
            errorMap.append(errorobject)
        else:
            errorobject = {"ruleName": ruleName, "isError": False, "result": result, "hash": self.generateHash(ruleName, result), "associationId": associationId}
            errorMap.append(errorobject)

    def appendMessages(self, errorMap, result, ruleName, associationId):
        if isinstance(result, list):
            # append an object is empty list convert to object
            if not result:
                self.appendMessage(errorMap, {}, ruleName, associationId)
            for r in result:
                self.appendMessage(errorMap, r, ruleName, associationId)
        else:
            self.appendMessage(errorMap, result, ruleName, associationId)

    def filterdataItemIdLevel(self, executionData):
        if executionData["ruleType"] == "dataItemId" and not executionData["isAggregateRule"]:
            return True
        else:
            return False

    def filterAggregatedataItemId(self, executionData):
        if executionData["ruleType"] == "dataItemId" and executionData["isAggregateRule"]:
            return True
        else:
            return False

    def filterDocumentLevelRules(self, executionData):
        if executionData["ruleType"] == "document":
            return True
        else:
            return False

    def filterCalculationLevelRules(self, executionData):
        if executionData["ruleType"] == "calculation":
            return True
        else:
            return False

    def getFilingMetadata(self, extractedData):
        import json
        if type(extractedData['filingMetadata']) is not dict:
            filingMetadata = json.loads(extractedData['filingMetadata'])
        else:
            filingMetadata = extractedData['filingMetadata']
        return filingMetadata

    def validate(self, extractedData, executionData):
        errorMap = []
        self.rundataItemIdLevelRules(errorMap, executionData, extractedData)
        self.runAggregatedataItemIdRules(errorMap, executionData, extractedData)
        self.runDocumentLevelRules(errorMap, executionData, extractedData)
        self.runCalculationLevelRules(errorMap, executionData, extractedData)

        return errorMap

    def runCalculationLevelRules(self, resultMap, executionData, extractedData):
        for rule in filter(self.filterCalculationLevelRules, executionData):
            ruleName = rule['ruleName']
            associationId = rule['associationId']
            try:
                # print("Calculation Rule -> " + rule['ruleName'] + " Primary dataItemId -> " + rule['primarydataItemId'])
                functionargs = {'dataItemId1': extractedData['extractedData'][rule['primarydataItemId']], 'filingMetadata': self.getFilingMetadata(extractedData), 'historicalData': extractedData['historicalData']}

                if len(rule['associateddataItemIds']) > 0:
                    dataItemIdnumber =2
                    for supportingdataItemId in rule['associateddataItemIds']:
                        if supportingdataItemId in extractedData['extractedData']:
                            functionargs['dataItemId' + str(dataItemIdnumber)] = extractedData['extractedData'][supportingdataItemId]
                        else:
                            functionargs['dataItemId' + str(dataItemIdnumber)] = {}
                        dataItemIdnumber = dataItemIdnumber + 1

                self.addParameterValuesToArgs(functionargs, rule)

                func = getattr(self, rule['ruleName'])
                if func:
                    #print('runCalculationLevelRules:', ruleName)
                    calculationresult = self.executeFunctionWithArgs(func, functionargs)
                    if 'error' in calculationresult:
                        self.appendError(resultMap, calculationresult['error'], ruleName, associationId)

                    must = {"value", "description", "type"}
                    if len(calculationresult) >= len(must) and all(key in calculationresult for key in must):
                        self.appendMessages(resultMap, calculationresult, ruleName, associationId)
                    else:
                        self.appendError(resultMap, "value, description and type are required attributes in calculation results", ruleName,
                                         associationId)
            except KeyError as k:
                self.appendError(resultMap, "Key Error -> " + str(k), ruleName, associationId)
            except Exception as e:
                self.appendError(resultMap, str(e), ruleName, associationId)

    def validateHighlight(self, highlight):
        highlightmust = {"versionId", "filingDate"}
        if not (len(highlight) > 0 and all(key in highlight for key in highlightmust)):

            raise ValueError('versionId and filingDate are required attributes for highlight')

        allowed = {"versionid", "filingDate", "dataItemId"}
        if not (len(highlight) > 0 and any(key in highlight for key in allowed)):
            raise ValueError('At least one of "versionid", "filingDate", "dataItemId" attribute is required for highlight')
        else:
            headermust = {"peo",  "scale", "currency"}
            if 'header' in highlight.keys() and not (len(highlight['header']) >= len(headermust) and all(key in highlight['header'] for key in headermust)):
                raise ValueError('[peo,  scale, currency] are required attributes for each header highlights')
            rowmust = {"companyid"}
            if 'row' in highlight.keys() and not (len(highlight['row']) >= len(rowmust) and all(key in highlight['row'] for key in rowmust)):
                raise ValueError('[companyid] is required attributes for each row highlights')
            cellmust = {"peo", "scale", "value", "currency"}
            if 'cell' in highlight.keys() and not (len(highlight['cell']) >= len(cellmust) and all(key in highlight['cell'] for key in cellmust)):
                raise ValueError('peo, scale, value, currency  are required attributes for each cell highlights')
            dataItemIdmust = {"dataItemId"}
            if 'dataItemId' in highlight.keys() and not (len(highlight['dataItemId']) >= len(dataItemIdmust) and all(key in highlight['dataItemId'] for key in dataItemIdmust)):
                raise ValueError('[dataItemId] is required attribute for each row')


    def validateErrors(self, errors):
        for error in errors:
            must = {"error", "highlights"}
            if len(error) == 0:
                return
            elif not (len(error) >= len(must) and all(key in error for key in must)):
                raise ValueError('[error and highlights] are required attributes for each error')
            else:
                for highlight in error['highlights']:
                    self.validateHighlight(highlight)

    def runDocumentLevelRules(self, errorMap, executionData, extractedData):
        for rule in filter(self.filterDocumentLevelRules, executionData):
            ruleName = rule['ruleName']
            associationId = rule['associationId']
            #print("Document -> " + ruleName)
            try:
                functionargs = {'filingMetadata': self.getFilingMetadata(extractedData), 'extractedData': extractedData['extractedData'], 'historicalData': extractedData['historicalData']}
                func = getattr(self, rule['ruleName'])
                #print("Check1")
                if func:
                    self.addParameterValuesToArgs(functionargs, rule)
                    #print('runDocumentLevelRules:', ruleName)
                    errors = self.executeFunctionWithArgs(func, functionargs)
                    self.validateErrors(errors)
                    self.appendMessages(errorMap, errors, ruleName, associationId)
                
            except ValueError as v:
                #print("Check Value Error")
                self.appendError(errorMap, "Invalid Error Check -> " + str(v), ruleName, associationId)
            except KeyError as k:
                #print("Check Key Error")
                self.appendError(errorMap, "Key Error -> " + str(k), ruleName, associationId)
            except Exception as e:
                #print("Check Error")
                self.appendError(errorMap, str(e), ruleName, associationId)

    def rundataItemIdLevelRules(self, errorMap, executionData, extractedData):
        for rule in filter(self.filterdataItemIdLevel, executionData):
            ruleName = rule['ruleName']
            associationId = rule['associationId']
            #print("dataItemId Level Rule --> " + rule['ruleName'] + " Primary dataItemId -> " + rule['primarydataItemId'])

            for fieldId, fieldValue in extractedData['extractedData'][rule['primarydataItemId']].items():
                try:
                    # print("Rule -> " + rule['ruleName'] + " dataItemId -> " + str(fieldValue['dataItemId']) + " fieldId -> " + fieldId)
                    functionargs = {'dataItemId1': fieldValue, 'filingMetadata': self.getFilingMetadata(extractedData), 'historicalData': extractedData['historicalData']}
                    if len(rule['associateddataItemIds']) > 0:
                        dataItemIdnumber = 2
                        for supportingdataItemId in rule['associateddataItemIds']:
                            if supportingdataItemId in extractedData['extractedData'] and len(extractedData['extractedData'][supportingdataItemId]) > 0:
                                functionargs['dataItemId' + str(dataItemIdnumber)] = list(extractedData['extractedData'][supportingdataItemId].values())[0]
                            else:
                                functionargs['dataItemId' + str(dataItemIdnumber)] = {}
                            dataItemIdnumber = dataItemIdnumber + 1

                    func = getattr(self, rule['ruleName'])
                    if func:
                        self.addParameterValuesToArgs(functionargs, rule)
                        #print('rundataItemIdLevelRules:', ruleName)
                        errors = self.executeFunctionWithArgs(func, functionargs)
                        self.validateErrors(errors)
                        self.appendMessages(errorMap, errors, ruleName, associationId)
                except ValueError as v:
                    self.appendError(errorMap, "Invalid Error Check -> " + str(v), ruleName, associationId)
                except KeyError as k:
                    self.appendError(errorMap, "Key Error -> " + str(k), ruleName, associationId)
                except Exception as e:
                    self.appendError(errorMap, str(e), ruleName, associationId)

    def runAggregatedataItemIdRules(self, errorMap, executionData, extractedData):
        for rule in filter(self.filterAggregatedataItemId, executionData):
            ruleName = rule['ruleName']
            associationId = rule['associationId']
            try:
                # print("Aggregate Rule -> " + rule['ruleName'] + " Primary dataItemId -> " + rule['primarydataItemId'])
                functionargs = {'dataItemId1': extractedData['extractedData'][rule['primarydataItemId']], 'filingMetadata': self.getFilingMetadata(extractedData), 'historicalData': extractedData['historicalData']}

                if len(rule['associateddataItemIds']) > 0:
                    dataItemIdnumber =2
                    for supportingdataItemId in rule['associateddataItemIds']:
                        if supportingdataItemId in extractedData['extractedData']:
                            functionargs['dataItemId' + str(dataItemIdnumber)] = extractedData['extractedData'][supportingdataItemId]
                        else:
                            functionargs['dataItemId' + str(dataItemIdnumber)] = {}
                        dataItemIdnumber = dataItemIdnumber + 1

                func = getattr(self, rule['ruleName'])
                if func:
                    self.addParameterValuesToArgs(functionargs, rule)
                    #print('runAggregatedataItemIdRules:', ruleName)
                    errors = self.executeFunctionWithArgs(func, functionargs)
                    self.validateErrors(errors)
                    self.appendMessages(errorMap, errors, ruleName, associationId)
            except ValueError as v:
                self.appendError(errorMap, "Invalid Error Check -> " + str(v), ruleName, associationId)
            except KeyError as k:
                self.appendError(errorMap, "Key Error -> " + str(k), ruleName, associationId)
            except Exception as e:
                self.appendError(errorMap, str(e), ruleName, associationId)

    def addParameterValuesToArgs(self, functionargs, rule):
        if rule['numberOfAdditionalParameters'] > 0:
            if rule['additionalParameters']:
                functionargs["parameters"] = rule['additionalParameters']
            else:
                functionargs["parameters"] = {}

        if rule["hasAssociatedStrings"]:
            if "associatedStrings" in rule and len(rule['associatedStrings']) > 0:
                functionargs["associatedStrings"] = rule['associatedStrings']
            else:
                functionargs["associatedStrings"] = []

    def executeFunctionWithArgs(self, func, functionargs):
        # print("Executing Rule -> " + func.__name__ + " functionargs -> " + str(functionargs))
        return func(**functionargs)
#Estimates Error Checks 
@add_method(Validation)
def EST_56E(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    ni_gaap=get_dataItemIds_list('TAG1', parameters) #ni_gaap_df
    ni_nor=get_dataItemIds_list('TAG2', parameters) #ni_nor_df
    eps_gaap=get_dataItemIds_list('TAG3', parameters) #eps_gaap_df
    eps_nor=get_dataItemIds_list('TAG4', parameters) #eps_nor_df
    operator=get_dataItemIds_list('Operation', parameters) #['>','<']
    try:
        # companyid=filingMetadata['metadata']['companyId']
        
        ni_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_gaap)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()
        ni_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_nor)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()                          
        eps_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_gaap)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()
        eps_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_nor)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()                          

    
        # print(ni_gaap_df)
        # print(ni_nor_df)
        # print(eps_gaap_df)
        # print(eps_nor_df)
        
        #print(ni_gaap_df['value_scaled'],ni_nor_df['value_scaled'],eps_gaap_df['value_scaled'],eps_nor_df['value_scaled'])

        ni_merged_df=pd.merge(ni_gaap_df,ni_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        eps_merged_df=pd.merge(eps_gaap_df,eps_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df=pd.merge(ni_merged_df,eps_merged_df,on=['peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

        # print(ni_merged_df)
        # print(eps_merged_df)

        # print(merged_df)
        # print(merged_df[['value_scaled_x_x','value_scaled_y_x','value_scaled_x_y','value_scaled_y_y']])
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        # if merged_df is not None:
        for ind,row in merged_df.iterrows():
             if (execute_operator(row['value_scaled_x_x'],row['value_scaled_y_x'],operator[0]) & execute_operator(row['value_scaled_x_y'],row['value_scaled_y_y'],operator[1])):
                 peos.append(row['peo'])
                 diff='NA'
                 perc='NA'
                 tid.append(row['tradingItemId_x'])
                 parentflag.append(row['parentFlag'])
                 accounting.append(row['accountingStandardDesc'])
                 fyc.append(row['fiscalChainSeriesId'])
        for ind,row in merged_df.iterrows():
             if (execute_operator(row['value_scaled_y_x'],row['value_scaled_x_x'],operator[0]) & execute_operator(row['value_scaled_y_y'],row['value_scaled_x_y'],operator[1])):
                 peos.append(row['peo'])
                 diff='NA'
                 perc='NA'
                 tid.append(row['tradingItemId_y'])
                 parentflag.append(row['parentFlag'])
                 accounting.append(row['accountingStandardDesc'])
                 fyc.append(row['fiscalChainSeriesId'])
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(ni_gaap)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                           &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                                          |((extractedData_parsed['dataItemId'].isin(ni_nor)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                                          |((extractedData_parsed['dataItemId'].isin(eps_gaap)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                                          |((extractedData_parsed['dataItemId'].isin(eps_nor)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))].drop_duplicates()
            
        temp1_revised=temp1.dropna()                                                                                                                                                                                                      

        for ind, row in temp1_revised.iterrows():
            result = {"highlights": [], "error": "NI GAAP & NI Normalized Increased whereas EPS GAAP & EPS Normalized decreases and vice versa"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)      
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors
    
    
#Estimates Error Checks 
@add_method(Validation)
def EST_57(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    recommendation = get_dataItemIds_list('LHSdataItemIds', parameters)
    targetprice = get_dataItemIds_list('RHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']
        companyid = filingMetadata['metadata']['companyId']
               
        current_rating = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(recommendation)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId','value','tradingItemId','accountingStandardDesc']]
        
        # current_rating['numaric_value']=current_rating['value'].str[-2].astype(int)
        # current_rating['numeric_value'] = current_rating['value'].str[-2:].astype(int)
        current_rating['numeric_value'] = current_rating['value'].str.rstrip('.0').astype(int)
        
        # print(current_rating)
        # current_rating['numeric_value'] = current_rating['value'].apply(lambda x: float(re.search(r'\((\d+\.\d+)\)', x).group(1)))

        previous_rating = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(recommendation)) & (historicalData_parsed['researchContributorId'] == contributor)& (historicalData_parsed['companyId'] == companyid) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][
            ['dataItemId','value','tradingItemId','accountingStandardDesc', 'filingDate']]
        
        # previous_rating['numaric_value']=int(previous_rating['value'].str[-2])
        #previous_rating['numeric_value'] = previous_rating['value'].str[-2:].astype(int)
        
        previous_rating['numeric_value'] = previous_rating['value'].str.rstrip('.0').astype(int)
        
        print(previous_rating)
        
        # previous_rating['numeric_value'] = previous_rating['value'].apply(lambda x: float(re.search(r'\((\d+\.\d+)\)', x).group(1)))
        
        current_tp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(targetprice)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId','value_scaled','currency','tradingItemId','accountingStandardDesc']]
        previous_tp = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(targetprice)) & (historicalData_parsed['researchContributorId'] == contributor) & (historicalData_parsed['companyId'] == companyid)& (
                                                      historicalData_parsed['filingDate'] < filingdate) & (historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId','value_scaled','currency','tradingItemId','accountingStandardDesc', 'filingDate']]
                                        
        
        # print(current_tp)
        # print(previous_tp)
        
        maxprevious_rating_1 = previous_rating.groupby(['dataItemId', 'accountingStandardDesc', 'tradingItemId'])['filingDate'].max().reset_index()

        maxprevious_rating = previous_rating[(previous_rating['filingDate'].isin(maxprevious_rating_1['filingDate']))]

        maxprevious_tp_1 = previous_tp.groupby(['dataItemId', 'accountingStandardDesc', 'tradingItemId'])['filingDate'].max().reset_index()

        maxprevious_tp = previous_tp[(previous_tp['filingDate'].isin(maxprevious_tp_1['filingDate']))]
        
        merged_rating=pd.merge(current_rating, maxprevious_rating,on=['dataItemId','tradingItemId','accountingStandardDesc'],how='inner')

        merged_tp=pd.merge(current_tp, maxprevious_tp,on=['dataItemId','tradingItemId','accountingStandardDesc'],how='inner')
        
        # print(merged_rating)
        # print(merged_tp)
        
        base_currency=merged_tp.currency_x.mode()[0]
        merged_tp['value_scaled_y']=merged_tp.apply(lambda x: currency_converter(currency_from=x['currency_x'], currency_to=base_currency, value=x['value_scaled_y']),axis=1)
        
        # print(merged_tp)
        
        merged_df = pd.merge(merged_rating, merged_tp, on=['accountingStandardDesc', 'tradingItemId'], how='inner')
        
        # print(merged_df)

        
        dataItemId_rating = []
        dataItemId_tp = []
        previousdate = []
        tid = []
        accounting = []
        diff = []
        perc = []
        
        for ind, row in merged_df.iterrows():
            if ((execute_operator(row['filingDate_x'], row['filingDate_y'], operator[0])) & (execute_operator(row['numaric_value_x'], row['numaric_value_y'], operator[1]))&(execute_operator(row['value_scaled_x'], row['value_scaled_y'], operator[1]))):
                dataItemId_rating.append(row['dataItemId_x'])
                dataItemId_tp.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_x'])
                accounting.append(row['accountingStandardDesc'])
                tid.append(row['tradingItemId'])
                diff='NA'
                perc='NA'
                
            if ((execute_operator(row['filingDate_x'], row['filingDate_y'], operator[0]))&(execute_operator(row['numaric_value_x'], row['numaric_value_y'], operator[2]))&(execute_operator(row['value_scaled_x'], row['value_scaled_y'], operator[2]))):
                dataItemId_rating.append(row['dataItemId_x'])
                dataItemId_tp.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_x'])
                accounting.append(row['accountingStandardDesc'])
                tid.append(row['tradingItemId'])
                diff='NA'
                perc='NA'

        diff_df = pd.DataFrame({"dataItemId_x": dataItemId_rating,"dataItemId_y": dataItemId_tp,"filingDate": previousdate, "diff": diff, "perc": perc})
        
        
        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId_x'])) & (
                                          extractedData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                          extractedData_parsed['tradingItemId'].isin(tid)) &  (
                                                  extractedData_parsed['value'] != "") & (
                                          extractedData_parsed['value'].notnull()))][
            ['dataItemId', 'value', 'parentFlag', 'accountingStandardDesc',
             'tradingItemId', 'fiscalChainSeriesId', 'team', 'description', 'tradingItemName']]

                                              
        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId_y'])) & (
                                          extractedData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                          extractedData_parsed['tradingItemId'].isin(tid)) &  (
                                                  extractedData_parsed['value'] != "") & (
                                          extractedData_parsed['value'].notnull()))][
            ['dataItemId', 'value','scale','currency', 'parentFlag', 'accountingStandardDesc',
             'tradingItemId', 'fiscalChainSeriesId', 'team', 'description', 'tradingItemName']]

        temp3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId_x'])) &  (
                                           historicalData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                           historicalData_parsed['tradingItemId'].isin(tid)) &
                                               (historicalData_parsed['value'] != "") & (
                                           historicalData_parsed['value'].notnull()) & (
                                           historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (
                                                   historicalData_parsed['researchContributorId'] == contributor))][
            ['dataItemId', 'value', 'parentFlag', 'accountingStandardDesc',
             'tradingItemId', 'fiscalChainSeriesId',  'team', 'description', 'tradingItemName', 'versionId', 'companyId', 'feedFileId', 'filingDate']]

        temp4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId_y'])) &  (
                                           historicalData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                           historicalData_parsed['tradingItemId'].isin(tid)) &
                                               (historicalData_parsed['value'] != "") & (
                                           historicalData_parsed['value'].notnull()) & (
                                           historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (
                                                   historicalData_parsed['researchContributorId'] == contributor))][
            ['dataItemId', 'value', 'scale','currency','parentFlag', 'accountingStandardDesc',
             'tradingItemId', 'fiscalChainSeriesId',  'team', 'description', 'tradingItemName', 'versionId', 'companyId', 'feedFileId', 'filingDate']]
     
        temp1_revised = temp1.dropna()

        for ind, row in temp1_revised.iterrows():
            result = {"highlights": [],
                      "error": "Non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                         "cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],
                                                  "currency": 'NA'}, "section": row['team'],
                                         "filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                           "description": row["description"],
                                           "versionId": filingMetadata['metadata']['versionId'],
                                           "companyid": filingMetadata['metadata']['companyId'],
                                           "feedFileId": filingMetadata['metadata']['feedFileId'],
                                           "peo": 'NA',
                                           "diff": 'NA',
                                           "percent": 'NA'}
            result["checkGeneratedForList"] = [
                {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                 "fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',
                 "value": row["value"], "units": 'NA', "currency": 'NA',
                 "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                 "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                 "diff": 'NA',
                 "percent": 'NA'}]
            errors.append(result)

        temp2_revised = temp2.dropna()

        for ind, row in temp2_revised.iterrows():
            result = {"highlights": [],
                      "error": "non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                         "cell": {"peo": 'NA', "scale": row['scale'], "value": row['value'],
                                                  "currency": row['currency']}, "section": row['team'],
                                         "filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                           "description": row["description"],
                                           "versionId": filingMetadata['metadata']['versionId'],
                                           "companyid": filingMetadata['metadata']['companyId'],
                                           "feedFileId": filingMetadata['metadata']['feedFileId'],
                                           "peo": 'NA',
                                           "diff": 'NA',
                                           "percent": 'NA'}
            result["checkGeneratedForList"] = [
                {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                 "fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',
                 "value": row["value"], "units": row['scale'], "currency": row['currency'],
                 "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                 "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                 "diff": 'NA',
                 "percent": 'NA'}]
            errors.append(result)
            
            temp3_revised = temp3.dropna()

            for ind, row in temp3_revised.iterrows():
                result = {"highlights": [],
                          "error": "non periodic data items captured values' combination is not meeting up the standard definition"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo":'NA', "scale": 'NA', "value": row['value'],
                                                      "currency": 'NA'}, "section": row['team'],
                                             "filingId": row['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"], "versionId": row['versionId'],
                                               "companyid": row['companyId'], "feedFileId": row['feedFileId'],
                                               "peo": 'NA',
                                               "diff": 'NA',
                                               "percent": 'NA'}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear":'NA', "fiscalQuarter": 'NA', "peo": 'NA',
                     "value": row["value"], "units": 'NA', "currency": 'NA',
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "refFilingId": row["versionId"], "refFilingDate": row["filingDate"],
                     "estimatePeriodId": 'NA',
                     "diff": 'NA',
                     "percent": 'NA'}]
            errors.append(result)

            temp4_revised = temp4.dropna()

            for ind, row in temp4_revised.iterrows():
                result = {"highlights": [],
                          "error": "non periodic data items captured values' combination is not meeting up the standard definition"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo":'NA', "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": row['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"], "versionId": row['versionId'],
                                               "companyid": row['companyId'], "feedFileId": row['feedFileId'],
                                               "peo": 'NA',
                                               "diff": 'NA',
                                               "percent": 'NA'}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear":'NA', "fiscalQuarter": 'NA', "peo": 'NA',
                     "value": row["value"], "units": row['scale'], "currency": row['currency'],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "refFilingId": row["versionId"], "refFilingDate": row["filingDate"],
                     "estimatePeriodId": 'NA',
                     "diff": 'NA',
                     "percent": 'NA'}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation)
def EST_102(historicalData,filingMetadata,extractedData,parameters):
    errors = []   
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #[!=]
    
    # print(left_dataItemIds_list)
    
    try:
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']
        
        # print(extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))
        # print(historicalData_parsed['companyId'])
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','scale','currency','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()
        # print(current)

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['peo'].isin(current['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['companyId']==companyid))][['dataItemId','peo','estimatePeriodId','value','scale','currency','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']].drop_duplicates()
        
        # print(current)
        # print(previous)
        
        # maxprevious1=previous.groupby(['researchContributorId'])['filingDate'].max().reset_index()
        
        maxprevious1=previous.groupby(['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        # print(maxprevious)

        # if ((len(current)>0) & (len(maxprevious)>0)):
        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner').drop_duplicates()
        
        # print(merged_df)
        # print(merged_df[['dataItemId','peo','value_x','value_y','currency_x','currency_y','currencyId_x','currencyId_y']])

        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        peos=[]
        curx=[]
        cury=[]
        AS=[]
        fyc=[]
        diff=[]
        perc=[]
        
        for ind, row in merged_df.iterrows():
            if execute_operator(row['currencyId_x'],row['currencyId_y'],operator[0]):
                peos.append(row['peo'])
                curx.append(row['currencyId_x'])
                cury.append(row['currencyId_y'])
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])
                parentflag.append(row['parentFlag'])
                AS.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'  
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"filingDate":previousdate,"diff":diff,"perc":perc})
        print(diff_df)
        

        if len(diff_df)>0:
            # diff_df['curcomb']=diff_df.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['currencyId']),axis=1)
            # historicalData_parsed['curcomb']=historicalData_parsed.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['currencyId']),axis=1)
    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(AS))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','fiscalYear','fiscalQuarter','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalChainSeriesId']].drop_duplicates()
            # print(temp1)

            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['researchContributorId']==contributor))][['dataItemId','peo','value','scale','currency','fiscalYear','fiscalQuarter','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate','fiscalChainSeriesId']].drop_duplicates()
            # print(temp2)

            temp1_revised=temp1.dropna()    
            temp2_revised=temp2.dropna() 
            
            # print(temp1_revised)

            for ind, row in temp1_revised.iterrows():

                result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "units": row['scale'], "value": row['value'], "currency": row['currency'],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)

            for ind, row in temp2_revised.iterrows():
                    result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'], "versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "units": row['scale'], "value": row['value'], "currency": row['currency'],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
            
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors
    
#Estimates Error Checks 
@add_method(Validation)        
def EST_56G(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA

    try:

        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        DA_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag_list))&(historicalData_parsed['periodTypeId']!=1)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['fiscalYear'].isin(lhs_df['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA


        if ((len(lhs_df)>0) & (len(rhs_df)>0)& (len(DA_df)>0)):

            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','value_scaled'],how='inner')

            merged_DA_df = pd.merge(DA_df,merged_df,on=['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear'],how='inner')
            

            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            dataitem=[]

            for ind, row in merged_DA_df.iterrows():
                if (row['value_scaled_x']!=0):
                    peos.append(row['peo_y'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    dataitem.append(row['dataItemId_x'])

            diff_df=pd.DataFrame({"peo":peos,"dataitem":dataitem}).drop_duplicates()
            

            if len(diff_df)>0:
                diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataitem'],x['peo']),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)

                temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId']]
                temp1_revised=temp1.dropna()
                
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "EBIT estimate = EBITDA estimate in FY but D&A Actual available in related actualized Qs"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": 'NA', "percent": 'NA'}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": 'NA', "percent": 'NA'}]
                    errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_35(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # tags
    operator = get_dataItemIds_list('Operation', parameters)
    try:
        documentdate=filingMetadata['metadata']['filingDate']
        
        
        temp = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(dataItemId_list)))&
                                     (extractedData_parsed['value'] != "") &
                                     (extractedData_parsed['value'].notnull())&
                                     (extractedData_parsed['peo']!= "")&
                                     (extractedData_parsed['peo'].notnull()))][['dataItemId', 'peo', 'fiscalChainSeriesId']]
        temp['companyId'] = filingMetadata['metadata']['companyId']

        # print(temp)
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list)) & (historicalData_parsed['value'] != "")&
                                          (historicalData_parsed['value'].notnull())&
                                          (historicalData_parsed['peo']!="")&
                                          (historicalData_parsed['filingDate'] < documentdate)&
                                          (historicalData_parsed['peo'].notnull()))][['dataItemId', 'peo','fiscalChainSeriesId', 'filingDate','companyId']]
        
        
        
        maxprevious = previous.groupby(['companyId'])['filingDate'].max().reset_index()

        previous = previous[previous['filingDate'].isin(maxprevious['filingDate'])]

        # print(previous)

        merged_df = pd.merge(temp, previous, on=['companyId'], how='inner')

        # print(merged_df)

        filingdate = []
        diff = []
        perc = []
        series1 = []
        series2 = []
        
        for ind, row in merged_df.iterrows():
            if execute_operator(row['fiscalChainSeriesId_x'], row['fiscalChainSeriesId_y'], operator[0]):
                filingdate.append(row['filingDate'])
                difference = 'NA'
                series1.append(row['fiscalChainSeriesId_x'])
                series2.append(row['fiscalChainSeriesId_y'])
                diff.append(difference)
                perc = 'NA'

        diff_df = pd.DataFrame(
            {"diff": diff, "perc": perc, "filingDate": filingdate, "curseries": series1, "preseries": series2}).drop_duplicates()
        #print(diff_df)
        temp1 = extractedData_parsed[(extractedData_parsed['fiscalChainSeriesId'].isin(series1))][
            ['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team']].drop_duplicates()
        #print(temp1)
        temp2 = historicalData_parsed[((historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (
            historicalData_parsed['fiscalChainSeriesId'].isin(series2)) )][
            ['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team', 'versionId', 'feedFileId',
             'filingDate', 'companyId']].drop_duplicates()


        if len(temp1) > 0 and len(temp2) > 0:
            temp1_revised = temp1.dropna()
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},
                                             "cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},
                                             "section": row['team'],
                                             "filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA",
                                               "versionId": filingMetadata['metadata']['versionId'],
                                               "companyid": filingMetadata['metadata']['companyId'],
                                               "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [
                    {"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA",
                     "peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA",
                     "accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],
                     "fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)

            temp2_revised = temp2.dropna()
            
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},
                                             "cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},
                                             "section": row['team'], "filingId": row['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA",
                                               "versionId": row['versionId'], "companyid": row['companyId'],
                                               "feedFileId": row['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [
                    {"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA",
                     "peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA",
                     "accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],
                     "fiscalChainSeries": row["fiscalChainSeriesId"], "refFilingId": row["versionId"],
                     "refFilingDate": row["filingDate"], "estimatePeriodId": "NA", "diff": "NA", "percent": "NA"}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_22D(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)  # ["!="]
    Threshold = get_parameter_value(parameters, 'Min_Threshold') #['0']
    try:
        quarters_semis = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['value'] == '0.0')&(extractedData_parsed['value'] != '') &(extractedData_parsed['value'].notnull())&((extractedData_parsed["periodTypeId"] == 2) | (extractedData_parsed["periodTypeId"] == 10)))][['dataItemId', 'peo', 'value_scaled', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'periodTypeId', 'fiscalYear']].drop_duplicates()

        fiscal_year = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value'] != '0.0')&(extractedData_parsed['value'] != '')&(extractedData_parsed['value'].notnull())& (extractedData_parsed["periodTypeId"] == 1))][['dataItemId', 'peo', 'value_scaled', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'periodTypeId', 'fiscalYear']]

        print(quarters_semis)
        print(fiscal_year)

        if ((len(quarters_semis) > 0) & (len(fiscal_year) > 0 )):

            quarters_semis = quarters_semis.groupby(['dataItemId', 'fiscalYear', 'periodTypeId', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo', 'count'), PEO_Sum=('value_scaled','sum')).reset_index()
        
           
            merged_df = pd.merge(quarters_semis, fiscal_year,on=['dataItemId', 'fiscalYear', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', ], how='inner')
            
            print(merged_df)
        
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]
            dataItemIds=[]
            
            if merged_df is not None:
                for ind, row in merged_df.iterrows():
                    if row['periodTypeId_x'] == 2:
                        if row['PEO_Count'] == 4:
                            if row['value_scaled']!=0:
                                if execute_operator(float(row['PEO_Sum']), float(Threshold[0]), operator[0]):
                                        peos.append(row['fiscalYear'])
                                        dataItemIds.append(row['dataItemId'])
                                        tid.append(row['tradingItemId'])
                                        parentflag.append(row['parentFlag'])
                                        accounting.append(row['accountingStandardDesc'])
                                        fyc.append(row['fiscalChainSeriesId'])
                                        diff='NA'
                                        perc='NA'
                for ind, row in merged_df.iterrows():
                    if row['periodTypeId_x'] == 10:
                        if row['PEO_Count'] == 2:
                            if row['value_scaled']!=0:
                                if execute_operator(float(row['PEO_Sum']), float(Threshold[0]), operator[0]):
                                        peos.append(row['fiscalYear'])
                                        dataItemIds.append(row['dataItemId'])
                                        tid.append(row['tradingItemId'])
                                        parentflag.append(row['parentFlag'])
                                        accounting.append(row['accountingStandardDesc'])
                                        fyc.append(row['fiscalChainSeriesId'])
                                        diff='NA'
                                        perc='NA'
                diff_df = pd.DataFrame({"fiscalYear": peos, "diff": diff, "perc": perc, 'dataItemId': dataItemIds})
        

                if len(diff_df)>0:
                    diff_df['peocomb']=diff_df.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1 )
        
                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['tradingItemId'].isin(tid)) &(extractedData_parsed['periodTypeId'] == 1)&(extractedData_parsed['parentFlag'].isin(parentflag))&
                                                  (extractedData_parsed['accountingStandardDesc'].isin(accounting)) &(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) &(extractedData_parsed['value'] != "") &
                                                  (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName','peocomb']]
        
                    temp1_revised = temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():
                    #if row['value'] != 0:
                        result = {"highlights": [], "error": "The all Quarters and Semi annuals are captured zero but Fiscal Years are having value(PDF Only)."}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],"currency": row['currency']}, "section": row['team'], "filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],"diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0], "percent":diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],"value": row["value"], "units": row["scale"], "currency": row["currency"],"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],"percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}]
                    errors.append(result)    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
    
#Estimates Error Checks 
@add_method(Validation) 
def EST_16AA(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters) #['<=']
    Threshold=get_parameter_value(parameters,'Max_Threshold') #90
    
    try:
        
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        
        # print(filingdate)
        # print(contributor)
        # print(historicalData_parsed['researchContributorId'])
        
          
        
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']].drop_duplicates()
        
        # print(current)
        # print(previous)
        
        previous['daysdiff']=abs((pd.to_datetime(filingdate)-pd.to_datetime(previous['filingDate'])).dt.days)

        # print(current)
        # print(previous)        
        
        maxprevious1=previous.groupby(['dataItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()
        
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))].drop_duplicates()
        
        #maxprevious['days']=abs((pd.to_datetime(filingdate)-pd.to_datetime(maxprevious['filingDate'])).dt.days)
        
        maxprevious = maxprevious.copy()
        current['peocomb']=current.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        maxprevious['peocomb']=maxprevious.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        
        # print(current)
        # print(maxprevious)
        
        temp=maxprevious[~((maxprevious['peocomb'].isin(current['peocomb']))&(maxprevious['parentFlag'].isin(current['parentFlag']))
                         &(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]
        
        # print(temp)
        
        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        peo=[]
        AS=[]
        fyc=[]
        diff=[]
        perc=[]        
        for ind, row in temp.iterrows():
            if execute_operator(row['daysdiff'],float(Threshold[0]),operator[0]):
                peo.append(row['peo'])
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])
                parentflag.append(row['parentFlag'])
                AS.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'  
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"filingDate":previousdate,"parentFlag":parentflag,"diff":diff,"perc":perc})
        
        # print(diff_df)
        if len(diff_df)>0:
            diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            # print(diff_df)
            historicalData_parsed['peocomb']=historicalData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            # previous['peocomb']=previous.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            # print(historicalData_parsed)
       
            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(diff_df['parentFlag']))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()
            # temp1 = previous[((previous['peocomb'].isin(diff_df['peocomb'])) &(previous['parentFlag'].isin(parentflag))&(previous['accountingStandardDesc'].isin(AS))&(previous['fiscalChainSeriesId'].isin(fyc))&(previous['value']!="")&(previous['value'].notnull())&(previous['filingDate'].isin(diff_df['filingDate'])))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','fiscalChainSeriesId','fiscalYear','filingDate']].drop_duplicates()
            # print(temp1)
            temp1_revised=temp1.dropna() 

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "PEO not in current document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":row["estimatePeriodId"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_16A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold = get_parameter_value(parameters, 'Max Threshold')

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']
        latestactual = pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo']).date()

        current = extractedData_parsed[((extractedData_parsed['dataItemFlag'] == 'E') & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'fiscalChainSeriesId', 'periodTypeId',
             'fiscalYear','tradingItemId','periodEndDate']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemFlag'] == 'E') & (historicalData_parsed['researchContributorId'] == contributor) & (historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'fiscalChainSeriesId', 'periodTypeId',
             'fiscalYear', 'filingDate','tradingItemId','periodEndDate','researchContributorId']]

        previous['peocomb']=previous.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        previous['periodEndDate'] = pd.to_datetime(previous['periodEndDate']).dt.date
        previous['daysdiff'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(previous['filingDate'])).dt.days)

        maxprevious1 = previous.groupby(['dataItemId', 'parentFlag', 'accountingStandardDesc', 'fiscalChainSeriesId','tradingItemId'])[
            'filingDate'].max().reset_index()
        
        maxprevious1['datecomb'] = maxprevious1[['dataItemId','filingDate']].astype(str).apply(lambda x: ''.join(x),axis=1)
        previous['datecomb'] = previous[['dataItemId','filingDate']].astype(str).apply(lambda x: ''.join(x),axis=1)

        maxprevious = previous[(previous['datecomb'].isin(maxprevious1['datecomb']))]

        current['peocomb'] = current[['dataItemId','peo']].astype(str).apply(lambda x: ''.join(x),axis=1)

        temp = maxprevious[ ((maxprevious['parentFlag'].isin(current['parentFlag']))&(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc'])) 
                           & (maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId']))&(maxprevious['tradingItemId'].isin(current['tradingItemId']))
                           &(maxprevious['dataItemId'].isin(current['dataItemId']))&~(maxprevious['peocomb'].isin(current['peocomb'])))]

        dataItemIds = []
        previousdate = []
        parentflag = []
        peo = []
        tid=[]
        AS = []
        fyc = []
        diff = []
        perc = []
        contributor=[]
        for ind, row in temp.iterrows():
            if ((execute_operator(row['daysdiff'], float(Threshold[0]), operator[0])) &  (execute_operator(latestactual, row['periodEndDate'], operator[1]))):
                peo.append(row['peo'])
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])
                parentflag.append(row['parentFlag'])
                AS.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                tid.append(row['tradingItemId'])
                contributor.append(row['researchContributorId'])
                diff = 'NA'
                perc = 'NA'

        diff_df = pd.DataFrame({"dataItemId": dataItemIds, "peo": peo, "filingDate": previousdate,"researchContributorId": contributor, "diff": diff, "perc": perc})

        if len(diff_df) > 0:
            diff_df['peocomb'] = diff_df[['dataItemId','peo','filingDate','researchContributorId']].astype(str).apply(lambda x: ''.join(x),axis=1)
            historicalData_parsed['peocomb'] = historicalData_parsed[['dataItemId','peo','filingDate','researchContributorId']].astype(str).apply(lambda x: ''.join(x),axis=1)

            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['tradingItemId'].isin(tid))
                                           &(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))] [['dataItemId','peo','scale','value','currency','fiscalYear','fiscalQuarter','fiscalChainSeriesId','estimatePeriodId','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()
            

            temp1_revised=temp1.dropna()  

            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "PEO not in current document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": "NA", "percent": "NA"}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":row["estimatePeriodId"], "diff":"NA", "percent": "NA"}]
                errors.append(result)         
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_1(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    
    try:
        eps = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'value', 'team', 'description']]

        if len(eps)==0:

            result = {"highlights": [], "error": "Compulsory Tag - EPS"}
            result["highlights"].append({"row": {"name": "21634", "id": "21634","companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": "Periodic Estimates","filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": "21634", "description": "EPS Normalized Estimate", "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA", "diff": "NA", "percent": "NA"}
            result["checkGeneratedForList"]=[{"tag": "21634", "description": "EPS Normalized Estimate", "tradingItemId": "NA","fiscalYear": "NA", "fiscalQuarter":"NA", "peo": "NA","value": "NA","units": "NA","currency": "NA","tradingItemName": "NA","accountingStdDesc": "NA","parentConsolidatedFlag": "NA","fiscalChainSeries": "NA", "diff": "NA", "percent": "NA"}]
            errors.append(result)                    


        print(errors)
        return errors                                                                                    
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_13A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold = get_parameter_value(parameters, 'Max Threshold')

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][
            ['dataItemId', 'peo', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
             'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter']].drop_duplicates()

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (
                                              historicalData_parsed['value'].notnull())&
                                              (historicalData_parsed['peo'].isin(current['peo'])))][
            ['dataItemId', 'peo', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
             'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter', 'filingDate','researchContributorId']].drop_duplicates()

        maxprevious1 = previous.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        previous['peocomb']=previous[['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId','filingDate']].astype(str).apply(lambda x: ''.join(x),axis=1)
        maxprevious1['peocomb']=maxprevious1[['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId','filingDate']].astype(str).apply(lambda x: ''.join(x),axis=1)

        maxprevious = previous[(previous['peocomb'].isin(maxprevious1['peocomb']))]

        merged_df = pd.merge(current, maxprevious,
                             on=['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')

        base_currency = merged_df.currency_x.mode()[0]
        merged_df['value_scaled_y'] = merged_df.apply(lambda x: currency_converter(currency_from=x['currency_y'], currency_to=base_currency,value=x['value_scaled_y']), axis=1)
        
        merged_df['variation'] = (((merged_df[['value_scaled_x', 'value_scaled_y']].max(axis=1)) - (merged_df[
            ['value_scaled_x', 'value_scaled_y']].min(axis=1))) / merged_df[['value_scaled_x', 'value_scaled_y']].min(
            axis=1)) * 100

        dataItemIds = []
        previousdate = []
        parentflag = []
        tid = []
        accounting = []
        fyc = []
        peos = []
        diff = []
        perc = []
        for ind, row in merged_df.iterrows():
            if execute_operator(float(row['variation']), float(Threshold[0]), operator[0]):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(float(round(row['variation'])))
                perc.append(float(round(row['variation'])))
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])

        diff_df = pd.DataFrame(
            {"dataItemId": dataItemIds, "peo": peos, "filingDate": previousdate,"researchContributorId": contributor, "diff": diff, "perc": perc})

        if len(diff_df) > 0:
            diff_df['peocomb1'] = diff_df.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)
            extractedData_parsed['peocomb1'] = extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']),
                                                                         axis=1)

            temp1 = extractedData_parsed[((extractedData_parsed['peocomb1'].isin(diff_df['peocomb1'])) & (
                extractedData_parsed['parentFlag'].isin(parentflag)) & (
                                              extractedData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                              extractedData_parsed['tradingItemId'].isin(tid)) & (
                                              extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (
                                                      extractedData_parsed['value'] != "") & (
                                              extractedData_parsed['value'].notnull()))][
                ['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc',
                 'tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear',
                 'fiscalQuarter', 'tradingItemName', 'peocomb1']].drop_duplicates()

            temp1_revised = temp1.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Percentage variation (Comparable)"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"],
                                               "versionId": filingMetadata['metadata']['versionId'],
                                               "companyid": filingMetadata['metadata']['companyId'],
                                               "feedFileId": filingMetadata['metadata']['feedFileId'],
                                               "peo": row["peo"],
                                               "diff": diff_df[diff_df['peocomb1'] == row["peocomb1"]]['diff'].iloc[0],
                                               "percent": diff_df[diff_df['peocomb1'] == row["peocomb1"]]['perc'].iloc[0]}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],
                     "value": row["value"], "units": row["scale"], "currency": row["currency"],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "diff": diff_df[diff_df['peocomb1'] == row["peocomb1"]]['diff'].iloc[0],
                     "percent": diff_df[diff_df['peocomb1'] == row["peocomb1"]]['perc'].iloc[0]}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_18B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if merged_df is not None:
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(float(round(difference)))
                perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
       
        if len(diff_df)>0:
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId','dataItemId', 'peo', 'value', 'scale','currency','accountingStandardDesc','parentFlag','team','description','tradingItemId', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()
    
            temp1_revised=temp1.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Capex = Free cash flow"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_18E(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Cash EPS
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #CFPS
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:

    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Cash EPS
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFPS


        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

      
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
            
        #if merged_df is not None:
        for ind,row in merged_df.iterrows():                
            if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(float(round(difference)))
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        if len(diff_df)>0:    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId','dataItemId', 'peo', 'value', 'scale','currency','accountingStandardDesc','parentFlag','team','description','tradingItemId', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()
    
            temp1_revised=temp1.dropna()  

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Cash EPS = CFPS"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Estimates Error Checks 
@add_method(Validation) 
def EST_18G(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_list=get_dataItemIds_list2('LHSdataItemIds', parameters)
    right_list=get_dataItemIds_list2('RHSdataItemIds', parameters)
    tag_list=get_dataItemIds_list2('TAG1', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
            
    try:

        datacomb=list(zip(left_list,right_list))

        comparable=pd.DataFrame(datacomb,columns=['dataitem1','dataitem2']).drop_duplicates(inplace=False)

        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]                          
        capex_df=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']] 

        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','tradingItemId'],how='inner')
        merged_df_capex=pd.merge(merged_df,capex_df,on=['peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId'],how='inner')

        comparable['compressed']=comparable.apply(lambda x:'%s%s' % (x['dataitem1'],x['dataitem2']),axis=1)
        merged_df_capex['compressed']=merged_df.apply(lambda x:'%s%s' % (x['dataItemId_x'],x['dataItemId_y']),axis=1)

        key_cols = ['compressed']

        dataitem_combination=merged_df_capex.merge(comparable.loc[:, comparable.columns.isin(key_cols)])

        dataItemIds_x=[]
        dataItemIds_y=[]
        peos=[]
        parentflag=[]
        accounting=[]
        tid=[]
        fyc=[]        
        diff=[]
        perc=[]
        
        for ind,row in dataitem_combination.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                dataItemIds_x.append(row['dataItemId_x'])
                dataItemIds_y.append(row['dataItemId_y'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId_x'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
        diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"peo":peos,"diff":diff,"perc":perc})

        diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId_y'],x['peo']),axis=1)
        extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        
        
        temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))
                                       &(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']].drop_duplicates()
        
        temp1_revised=temp1.dropna()
                                                                                                                                                                                                                              
        for ind, row in temp1_revised.iterrows():
                
            result = {"highlights": [], "error": "FFO = AFFO - one combination and FFOPS = AFFOPS - other combination"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
            errors.append(result)
        
        print(errors) 
        return errors                                                                   
    except Exception as e:
        print(e)
        return errors 

#Estimates Error Checks 
@add_method(Validation) 
def EST_56A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT,INTEREST EXP
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
       
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt3

        # print(lhs_df)
        # print(rhs_df)

        if ((len(lhs_df)>0) & (len(rhs_df)>0)):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT + Int exp
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBT
        
            # print(lhs_df)
            # print(rhs_df)
        
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
            # print(merged_df)
    
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            
            #if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if (row['value_scaled_x']!=0 and row['value_scaled_y']!=0):
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])               
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
                        perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
            if len(diff_df)>0:    
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId','dataItemId', 'peo', 'value', 'scale','currency','accountingStandardDesc','parentFlag','team','description','tradingItemId', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()
        
                temp1_revised=temp1.dropna() 
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "EBIT + Interest = EBT Normalized"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_56D(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    tag_list=get_dataItemIds_list('TAG1', parameters) #CAPEX
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #CFO
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #FCF
        CAPEX_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #CAPEX


        if (len(lhs_df)>0 & len(rhs_df)>0 & len(CAPEX_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            CAPEX_df["value_scaled"] = CAPEX_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_CAPEX_df = pd.merge(CAPEX_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if merged_CAPEX_df is not None:
        for ind,row in merged_CAPEX_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(float(round(difference)))
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        if len(diff_df)>0:
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))
                    &(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()

            temp1_revised=temp1.dropna()
                    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "CFO =FCF and Capex available"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_56I(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO, CAPEX, M.CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:

        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX, M.CAPEX

        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
                
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
                perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        if len(diff_df)>0:
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'description','team', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()
    
            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Free Cash Flow = CFO + Capex + M.capex"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Estimates Error Checks 
@add_method(Validation) 
def EST_18F(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Revenue
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Revenue
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
            
        #if merged_df is not None:
        for ind,row in merged_df.iterrows():
            if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(float(round(difference)))
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

        if len(diff_df)>0:    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']].drop_duplicates()
    
            temp1_revised=temp1.dropna()  
        
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Revenue = EBITDA"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_345(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CashEPS
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EPSNormalized
    tag1_list=get_dataItemIds_list('TAG1', parameters) #EPSGAAP
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:

    
        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CashEPS
        rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPS Normalised
        tag1_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag1_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPS GAAP
        
        
        if (len(lhs_df)>0 & len(rhs_df)>0 | len(lhs_df)>0 & len(tag1_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            tag1_df["value_scaled"] = tag1_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        
        if (((len(lhs_df)>0) & (len(rhs_df)>0)) | ((len(lhs_df)>0) & (len(tag1_df)>0))):
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
            merged_df1=pd.merge(lhs_df,tag1_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
            
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            
            if merged_df is not None:
                for ind,row in merged_df.iterrows():
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])               
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(float(round(difference)))
                        perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))        
            
            if merged_df1 is not None:
                for ind,row in merged_df1.iterrows():
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])               
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(float(round(difference)))
                        perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
                
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
                if len(diff_df)>0:    
                    temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']].drop_duplicates()
            
                    temp1_revised=temp1.dropna()  
                    
                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Cash EPS collected = GAAP EPS OR Normalized EPS"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result)      
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def EST_3L(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)#INAVPS
    right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)#CAPRATE

    try:

        inavps = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','tradingItemId']]

        caprate = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','tradingItemId']]
 
        if ((len(inavps)>0) & (len(caprate)==0)):

            dataItemIds=[]

            for ind, row in inavps.iterrows():

                dataItemIds.append(row['dataItemId'])
    
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','tradingItemId','team','description','tradingItemName','parentFlag','scale','currency']].drop_duplicates()
    
                temp1_revised=temp1.dropna() 
 
                for ind, row in temp1_revised.iterrows():
        
                    result = {"highlights": [], "error": "INAVPS is collected but cap rate not"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": "NA", "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA", "diff": "NA", "percent": "NA"}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": "NA", "fiscalQuarter":"NA", "peo": "NA","value": row["value"],"units": row['scale'],"currency": row['currency'],"tradingItemName": row["tradingItemName"],"accountingStdDesc": 'NA',"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": "NA", "diff": "NA", "percent": "NA"}]
                    errors.append(result)
         
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_15G(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)        
    operator = get_dataItemIds_list('Operation', parameters) #[!=]
    max_threshold=get_parameter_value(parameters,'Max Threshold')
    
    try:
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        if len(current)>0:
            previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())
                                              &(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
            

            maxprevious1=previous.groupby(['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])['filingDate'].max().reset_index()
  
            maxprevious = previous[((previous['filingDate'].isin(maxprevious1['filingDate']))
                                    &(previous['dataItemId'].isin(maxprevious1['dataItemId']))
                                    &(previous['peo'].isin(maxprevious1['peo'])))].drop_duplicates()

            merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
            merged_df['variation'] = ((merged_df[['value_scaled_x', 'value_scaled_y']].max(axis=1) - merged_df[['value_scaled_x', 'value_scaled_y']].min(axis=1)) / merged_df[['value_scaled_x', 'value_scaled_y']].min(axis=1)) * 100
            merged_df['valuelen_x']=(round(merged_df['value_scaled_x'])).apply(lambda x: len(str(x)))
            merged_df['valuelen_y']=(round(merged_df['value_scaled_y'])).apply(lambda x: len(str(x)))
            merged_df['units_diff']=abs(merged_df['valuelen_x']-merged_df['valuelen_y'])

            filingdate=[]
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]
    
            for ind,row in merged_df.iterrows():
                if (execute_operator(float(row['units_diff']),float(max_threshold[0]),operator[0])):
                    filingdate.append(row['filingDate'])
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])  
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(float(round(row['variation'])))
                    perc.append(float(round(row['variation'])))
                                           
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,"filingDate":filingdate})

            if len(diff_df)>0: 
                
                diff_df['peocomb']=diff_df[['dataItemId','peo']].astype(str).apply(lambda x: ''.join(x),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed[['dataItemId','peo']].astype(str).apply(lambda x: ''.join(x),axis=1)
                historicalData_parsed['peocomb']=historicalData_parsed[['dataItemId','peo']].astype(str).apply(lambda x: ''.join(x),axis=1)
                
                temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']].drop_duplicates() 

              
                temp1_revised=temp1.dropna()  
 
        
                for ind, row in temp1_revised.iterrows():
        
                    result = {"highlights": [], "error": "Units length difference is more than 3 digits compare to previous documents"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                    errors.append(result)
               

        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors  

#Estimates Error Checks 
@add_method(Validation) 
def EST_22C(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters) #['!=]

    try:
        FQ = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())  & ((extractedData_parsed["periodTypeId"] == 2)|(extractedData_parsed["periodTypeId"] == 10)))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())& (extractedData_parsed["periodTypeId"] == 1))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        # print(FQ)
        # print(FY)

        FQ["valuesign"]=np.sign((FQ['value']).astype(float))
        FY["valuesign"]=np.sign((FY['value']).astype(float))

        # print(FQ)
        # print(FY)

        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId',],how='inner')


        merged_df_PEO_count= merged_df.groupby(['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId_x']).agg(quarter_sum=('valuesign_x','sum'),fy_sum=('valuesign_y','sum')).reset_index() 


        peos=[]
        diff=[]
        perc=[]
        dataItemIds=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff='NA'
        perc='NA'
        
        for ind,row in merged_df_PEO_count.iterrows():
            if (row['periodTypeId_x']==2 and abs(row['fy_sum'])==4 and abs(row['quarter_sum'])==4):
                if execute_operator(row['quarter_sum'],row['fy_sum'],operator[0]):
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])

        for ind,row in merged_df_PEO_count.iterrows():
            if (row['periodTypeId_x']==10 and abs(row['fy_sum'])==2 and abs(row['quarter_sum'])==2):
                if execute_operator(row['quarter_sum'],row['fy_sum'],operator[0]):
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"fiscalYear":peos,"diff":diff,"perc":perc})
        
        diff_df['peocomb']=diff_df.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1)
        extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1 )
        
        temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))
                                                  &(extractedData_parsed['value'] != "") &(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName','peocomb']] #.drop_duplicates()    

        temp1_revised=temp1.dropna()
        
        for ind, row in temp1_revised.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "All Quarter OR Semis values are collected in one sign and Fiscal Year value collected in another sign"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                    

        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_22D(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)  # ["!="]
    Threshold = get_parameter_value(parameters, 'Min_Threshold') #['0']
    try:
        quarters_semis = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['value'] == '0.0')&(extractedData_parsed['value'] != '') &(extractedData_parsed['value'].notnull())&((extractedData_parsed["periodTypeId"] == 2) | (extractedData_parsed["periodTypeId"] == 10)))][['dataItemId', 'peo', 'value_scaled', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'periodTypeId', 'fiscalYear']].drop_duplicates()

        fiscal_year = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value'] != '0.0')&(extractedData_parsed['value'] != '')&(extractedData_parsed['value'].notnull())& (extractedData_parsed["periodTypeId"] == 1))][['dataItemId', 'peo', 'value_scaled', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'periodTypeId', 'fiscalYear']]

        if ((len(quarters_semis) > 0) & (len(fiscal_year) > 0 )):

            quarters_semis = quarters_semis.groupby(['dataItemId', 'fiscalYear', 'periodTypeId', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo', 'count'), PEO_Sum=('value_scaled','sum')).reset_index()
        
           
            merged_df = pd.merge(quarters_semis, fiscal_year,on=['dataItemId', 'fiscalYear', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', ], how='inner')
            
        
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]
            dataItemIds=[]
            
            if merged_df is not None:
                for ind, row in merged_df.iterrows():
                    if row['periodTypeId_x'] == 2:
                        if row['PEO_Count'] == 4:
                            if row['value_scaled']!=0:
                                if execute_operator(float(row['PEO_Sum']), float(Threshold[0]), operator[0]):
                                        peos.append(row['fiscalYear'])
                                        dataItemIds.append(row['dataItemId'])
                                        tid.append(row['tradingItemId'])
                                        parentflag.append(row['parentFlag'])
                                        accounting.append(row['accountingStandardDesc'])
                                        fyc.append(row['fiscalChainSeriesId'])
                                        diff='NA'
                                        perc='NA'
                for ind, row in merged_df.iterrows():
                    if row['periodTypeId_x'] == 10:
                        if row['PEO_Count'] == 2:
                            if row['value_scaled']!=0:
                                if execute_operator(float(row['PEO_Sum']), float(Threshold[0]), operator[0]):
                                        peos.append(row['fiscalYear'])
                                        dataItemIds.append(row['dataItemId'])
                                        tid.append(row['tradingItemId'])
                                        parentflag.append(row['parentFlag'])
                                        accounting.append(row['accountingStandardDesc'])
                                        fyc.append(row['fiscalChainSeriesId'])
                                        diff='NA'
                                        perc='NA'
                diff_df = pd.DataFrame({"fiscalYear": peos, "diff": diff, "perc": perc, 'dataItemId': dataItemIds})
        

                if len(diff_df)>0:
                    diff_df['peocomb']=diff_df.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1 )
        
                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['tradingItemId'].isin(tid)) &(extractedData_parsed['periodTypeId'] == 1)&(extractedData_parsed['parentFlag'].isin(parentflag))&
                                                  (extractedData_parsed['accountingStandardDesc'].isin(accounting)) &(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) &(extractedData_parsed['value'] != "") &
                                                  (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName','peocomb']]
        
                    temp1_revised = temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():
                    #if row['value'] != 0:
                        result = {"highlights": [], "error": "The all Quarters and Semi annuals are captured zero but Fiscal Years are having value(PDF Only)."}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],"currency": row['currency']}, "section": row['team'], "filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],"diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0], "percent":diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],"value": row["value"], "units": row["scale"], "currency": row["currency"],"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],"percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}]
                    errors.append(result)    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_15E(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']
        

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate','researchContributorId']]
        
        maxprevious1 = previous.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        maxprevious = previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        merged_df = pd.merge(current, maxprevious,
                             on=['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')

        dataItemIds = []
        previousdate = []
        parentflag = []
        tid = []
        accounting = []
        fyc = []
        peos = []
        diff = []
        perc = []
        for ind, row in merged_df.iterrows():
            if ((execute_operator(float(row['value_x']), float(row['value_y']), operator[0])) & (execute_operator(row['currency_x'], row['currency_y'], operator[1]))):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])

        diff_df = pd.DataFrame(
            {"dataItemId": dataItemIds, "peo": peos, "filingDate": previousdate, "diff": diff, "perc": perc})
        
        if len(diff_df) > 0:
            diff_df['peocomb'] = diff_df.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)
            extractedData_parsed['peocomb'] = extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)
            historicalData_parsed['peocomb'] = historicalData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)

            temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (
                extractedData_parsed['parentFlag'].isin(parentflag)) & (
                                              extractedData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                              extractedData_parsed['tradingItemId'].isin(tid)) & (
                                              extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (
                                                      extractedData_parsed['value'] != "") & (
                                              extractedData_parsed['value'].notnull()))][
                ['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc',
                 'tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear',
                 'fiscalQuarter', 'tradingItemName', 'peocomb']].drop_duplicates()

            temp1_revised = temp1.dropna()
            
            temp2 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) & (
                historicalData_parsed['parentFlag'].isin(parentflag)) & (
                                               historicalData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                               historicalData_parsed['tradingItemId'].isin(tid)) & (
                                               historicalData_parsed['fiscalChainSeriesId'].isin(fyc)) & (
                                                       historicalData_parsed['value'] != "") & (
                                               historicalData_parsed['value'].notnull()) & (
                                               historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (
                                                       historicalData_parsed['researchContributorId'] == contributor))][
                ['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc',
                 'tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear',
                 'fiscalQuarter', 'tradingItemName', 'peocomb', 'versionId', 'companyId', 'feedFileId', 'filingDate']].drop_duplicates()

                                               
            temp2_revised = temp2.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Same value different currency_Non per share data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"],
                                               "versionId": filingMetadata['metadata']['versionId'],
                                               "companyid": filingMetadata['metadata']['companyId'],
                                               "feedFileId": filingMetadata['metadata']['feedFileId'],
                                               "peo": row["peo"],
                                               "diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],
                                               "percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],
                     "value": row["value"], "units": row["scale"], "currency": row["currency"],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],
                     "percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Same value different currency_Non per share data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": row['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"], "versionId": row['versionId'],
                                               "companyid": row['companyId'], "feedFileId": row['feedFileId'],
                                               "peo": row["peo"],
                                               "diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],
                                               "percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],
                     "value": row["value"], "units": row["scale"], "currency": row["currency"],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "refFilingId": row["versionId"], "refFilingDate": row["filingDate"],
                     "estimatePeriodId": row["estimatePeriodId"],
                     "diff": diff_df[diff_df['peocomb'] == row["peocomb"]]['diff'].iloc[0],
                     "percent": diff_df[diff_df['peocomb'] == row["peocomb"]]['perc'].iloc[0]}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_195(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #['>]
    variation=get_parameter_value(parameters,'Max Threshold') #100%
    
    try:
        yesscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']!=-1)&(extractedData_parsed['currencyId']!=-1)&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        volume = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']!=-1)&((extractedData_parsed['currencyId']==-1)|(extractedData_parsed['currencyId']==0))&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]

        noscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']==-1)&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        
        if len(yesscale)>0:
            yesscale['consValue_scaled'] = yesscale.apply(lambda row: get_scaled_value(row['consValue'], row['consScaleId']), axis=1)
            
            base_currency=yesscale.consCurrency.mode()[0]

            yesscale["value_scaled"] = yesscale.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            yesscale["consValue_scaled"] = yesscale.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
        
        if len(volume)>0:
            volume['consValue_scaled'] = volume.apply(lambda row: get_scaled_value(row['consValue'], row['consScaleId']), axis=1)
    
        if len(noscale)>0: 
            noscale['value_scaled'] = pd.to_numeric(noscale['value_scaled'],errors = 'coerce')
            noscale['consValue_scaled'] = pd.to_numeric(noscale['consValue'],errors = 'coerce')
            
            
        if (len(yesscale)>0 or len(noscale)>0 or len(volume)>0):

            frames = [yesscale, volume,noscale]
            temp = pd.concat(frames)[['dataItemId','peo','value_scaled','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId','consValue_scaled']]
            
            temp['consensusvariation']=((abs(((temp['value_scaled']).astype(float))-((temp['consValue_scaled']).astype(float))))/(abs(temp[['value_scaled','consValue_scaled']])).min(axis=1))*100
            
            temp.replace([np.inf, -np.inf], np.nan, inplace=True)
            temp.dropna(inplace=True)

            dataItemIds=[]
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            
            for ind,row in temp.iterrows():
                    
                if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
    
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])  
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId']) 
                    diff.append(float(round(row['consensusvariation'])))
                    perc.append(float(round(row['consensusvariation'])))
                    
    
            diff_df=pd.DataFrame({"peo":peos,"dataItemId":dataItemIds,"diff":diff,"perc":perc})
                        

            diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                
            temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']] 
            
            temp1=temp1.drop_duplicates()
            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "%Variation - Net Income Normalized: Extracted vs Calculated"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid":filingMetadata['metadata']['companyId']},"cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],"diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                          
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors 

#Estimates Error Checks 
@add_method(Validation)
def EST_30A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    list1 = get_dataItemIds_list('TAG1', parameters)  # scale applicable data items(periodic)
    list2 = get_dataItemIds_list('TAG2', parameters)  # scale applicable data items(Non-periodic)
    list3 = get_dataItemIds_list('TAG3', parameters)  # currency applicable data items(periodic)
    list4 = get_dataItemIds_list('TAG4', parameters)  # currency applicable data items(Non-periodic)
    list5 = get_dataItemIds_list('LHSdataItemIds', parameters)  # trading item applicable data items(periodic)
    list6 = get_dataItemIds_list('RHSdataItemIds', parameters)  # trading item applicable data items(Non-periodic)
    list7 = get_dataItemIds_list('LHSTags', parameters)  # Parent flag applicable data items(periodic)
    list8 = get_dataItemIds_list('RHSTags', parameters)  # Parent flag applicable data items(Non-periodic)
    try:

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list1)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['scaleId'] == -1)|(extractedData_parsed['scaleId'] == "")|(~(extractedData_parsed['scaleId'].notnull()))))][['dataItemId','peo',  'value',  'team', 'description']].drop_duplicates()

        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list2)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['scaleId'] == -1)|(extractedData_parsed['scaleId'] == "")|(~(extractedData_parsed['scaleId'].notnull()))))][['dataItemId',  'value',  'team', 'description']].drop_duplicates()

        temp3 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list3)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['currencyId'] == -1)|(extractedData_parsed['currencyId'] == "")|(~(extractedData_parsed['currencyId'].notnull()))))][['dataItemId', 'peo', 'value',  'team', 'description']].drop_duplicates()

        temp4 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list4)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['currencyId'] == -1)|(extractedData_parsed['currencyId'] == "")|(~(extractedData_parsed['currencyId'].notnull()))))][['dataItemId',  'value', 'team', 'description']].drop_duplicates()


        temp5 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list5)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['tradingItemId'] == -1)|(extractedData_parsed['tradingItemId'] == "")|(~(extractedData_parsed['tradingItemId'].notnull()))))][['dataItemId','peo', 'value', 'team', 'description']].drop_duplicates()

        temp6 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list6)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['tradingItemId'] == -1)|(extractedData_parsed['tradingItemId'] == "")|(~(extractedData_parsed['tradingItemId'].notnull()))))][['dataItemId', 'value', 'team', 'description']].drop_duplicates()

        temp7 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list7)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['parentFlag'] == "")|(~(extractedData_parsed['parentFlag'].notnull()))))][['dataItemId','peo',  'value',   'team', 'description']].drop_duplicates()

        temp8 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list8)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['parentFlag'] == "")|(~(extractedData_parsed['parentFlag'].notnull()))))][['dataItemId',  'value',   'team', 'description']].drop_duplicates()

        if len(temp1)>0:
            temp1_revised=temp1.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Units not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp2)>0:
            temp2_revised=temp2.dropna()

            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Units not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp3)>0:

            temp3_revised=temp3.dropna()

            for ind, row in temp3_revised.iterrows():
                result = {"highlights": [],
                          "error": "Currency not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp4)>0:

            temp4_revised=temp4.dropna()

            for ind, row in temp4_revised.iterrows():
                result = {"highlights": [],
                          "error": "Currency not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp5)>0:
           
            temp5_revised=temp5.dropna()

            for ind, row in temp5_revised.iterrows():
                result = {"highlights": [],
                          "error": "Trading item not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp6)>0:
           
            temp6_revised=temp6.dropna()

            for ind, row in temp6_revised.iterrows():
                result = {"highlights": [],
                          "error": "Trading item not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp7)>0:

            temp7_revised=temp7.dropna()

            for ind, row in temp7_revised.iterrows():
                result = {"highlights": [],
                          "error": "Parent flag not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp8)>0:

            temp8_revised=temp8.dropna()

            for ind, row in temp8_revised.iterrows():
                result = {"highlights": [],
                          "error": "Parent flag not specified for the Data Item,"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_30B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    list1 = get_dataItemIds_list('TAG1', parameters)  # scale not applicable data items(periodic)
    list2 = get_dataItemIds_list('TAG2', parameters)  # scale not applicable data items(Non-periodic)
    list3 = get_dataItemIds_list('TAG3', parameters)  # currency not applicable data items(periodic)
    list4 = get_dataItemIds_list('TAG4', parameters)  # currency not applicable data items(Non-periodic)
    list5 = get_dataItemIds_list('LHSdataItemIds', parameters)  # trading item not applicable data items(periodic)
    list6 = get_dataItemIds_list('RHSdataItemIds', parameters)  # trading item not applicable data items(Non-periodic)

    try:

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list1)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['scaleId'] != -1))][['dataItemId','peo',  'value',  'team', 'description']].drop_duplicates()

        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list2)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['scaleId'] != -1))][['dataItemId',  'value',  'team', 'description']].drop_duplicates()

        temp3 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list3)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['currencyId'] != -1))][['dataItemId', 'peo', 'value',  'team', 'description']].drop_duplicates()

        temp4 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list4)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['currencyId'] != -1))][['dataItemId',  'value', 'team', 'description']].drop_duplicates()


        temp5 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list5)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['tradingItemId'] != -1))][['dataItemId','peo', 'value', 'team', 'description']].drop_duplicates()

        temp6 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list6)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & (extractedData_parsed['tradingItemId'] != -1))][['dataItemId', 'value', 'team', 'description']].drop_duplicates()


        if len(temp1)>0:
            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Units collected for not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp2)>0:
            temp2_revised=temp2.dropna()
            
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Units collected for not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp3)>0:

            temp3_revised=temp3.dropna()
            
            for ind, row in temp3_revised.iterrows():
                result = {"highlights": [],
                          "error": "Currency collected not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp4)>0:

            temp4_revised=temp4.dropna()
            
            for ind, row in temp4_revised.iterrows():
                result = {"highlights": [],
                          "error": "Currency collected not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp5)>0:
           
            temp5_revised=temp5.dropna()
            
            for ind, row in temp5_revised.iterrows():
                result = {"highlights": [],
                          "error": "Trading collected for not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": row['peo'], "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'],"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": row['peo'],"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        if len(temp6)>0:
           
            temp6_revised=temp6.dropna()

            for ind, row in temp6_revised.iterrows():
                result = {"highlights": [],
                          "error": "Trading collected for not applicable data items"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_30C(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    list1 = get_dataItemIds_list('TAG1', parameters)  # AS Applicable data items

    try:

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(list1)) & (extractedData_parsed['value'] != "") &
                                      (extractedData_parsed['value'].notnull()) & ((extractedData_parsed['accountingStandardDesc'] == "")|(~(extractedData_parsed['accountingStandardDesc'].notnull()))))][['dataItemId',  'value',   'team', 'description']].drop_duplicates()

        if len(temp1)>0:

            temp1_revised=temp1.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "GAAP not specified for the Data Item"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA',"diff": 'NA',"percent": 'NA'}
                result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": "NA","fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": 'NA', "accountingStdDesc": 'NA',"parentConsolidatedFlag": "NA", "fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_67(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #[Tags]
    operator = get_dataItemIds_list('Operation', parameters) #["<"]    
    try:
        latestactual = pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo']).date()
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']        

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate','periodEndDate']].drop_duplicates()

        # print(current)
        # print(previous)

        maxprevious1=previous.groupby(['dataItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()

        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        maxprevious['periodEndDate']=pd.to_datetime(maxprevious['periodEndDate']).dt.date
        
        current['didpeocomb']=current.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        maxprevious['didpeocomb']=maxprevious.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        
      
        temp_df=maxprevious[~((maxprevious['didpeocomb'].isin(current['didpeocomb']))&(maxprevious['parentFlag'].isin(current['parentFlag']))&(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]
        

        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        peo=[]
        AS=[]
        fyc=[]
        diff=[]
        perc=[]      

   
        for ind, row in temp_df.iterrows():
            if execute_operator(latestactual, row['periodEndDate'], operator[0]):
                peo.append(row['peo'])
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])
                parentflag.append(row['parentFlag'])
                AS.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'  
                    
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"filingDate":previousdate,"diff":diff,"perc":perc})

        if len(diff_df)>0:
            diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            historicalData_parsed['peocomb']=historicalData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
       
            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])))] [['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()
    
    
            temp1_new=temp1.dropna() 
    
        
            for ind, row in temp1_new.iterrows():
    
                result = {"highlights": [], "error": "Same Sign/Missing Tag"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":row["estimatePeriodId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors    


#Estimates Error Checks 
@add_method(Validation) 
def EST_60(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    #dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:

        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(~(extractedData_parsed['parentFlag'].isin(historicalData_parsed['parentFlag']))
                                        |~(extractedData_parsed['tradingItemId'].isin(historicalData_parsed['tradingItemId']))
                                        |~(extractedData_parsed['accountingStandardDesc'].isin(historicalData_parsed['accountingStandardDesc']))
                                        |~(extractedData_parsed['fiscalChainSeriesId'].isin(historicalData_parsed['fiscalChainSeriesId']))
                                        ))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        

        if len(current)>0:
            dataItemIds=[]
            parentflag=[]
            accounting=[]
            peos=[]
            tid=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in current.iterrows():
                dataItemIds.append(row['dataItemId'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                peos.append(row['peo'])
                diff='NA'
                perc='NA'
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo':peos})
            
            print(diff_df)

            temp= extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) &(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']].drop_duplicates()

            temp_revised=temp.dropna()

            for ind, row in temp_revised.iterrows():
                result = {"highlights": [], "error": "New flavor captured for the company"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)                    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_193(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    
    try:
        
        # if filingMetadata['metadata']['latestPeriodType']=="FY":
        latestfy=int(filingMetadata['metadata']['latestPeriodYear'])
        # else:
        #     latestfy=int(filingMetadata['metadata']['latestPeriodYear'])-1        
                
        # if filingMetadata['metadata']['latestPeriodType']=="FY":
        #     latestactualPEO=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo']).date()
        #     MinEstimatePEO = pd.to_datetime(latestactualPEO+pd.DateOffset(years=1)).date()
        #     # print(MinEstimatePEO)

        # print(latestfy)    
        # temp_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed["periodTypeId"] == 1) &(pd.to_datetime(extractedData_parsed["periodEndDate"]).dt.date == MinEstimatePEO) &((extractedData_parsed['value'] == "") | (extractedData_parsed['value'].isnull()))][['dataItemId', 'peo', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter', 'periodEndDate']].drop_duplicates()

        temp_df1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['fiscalYear']==latestfy) &(extractedData_parsed["periodTypeId"] == 1))][['dataItemId', 'peo', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter', 'periodEndDate']].drop_duplicates()

        # print(temp_df1)
        temp_df2=temp_df1.groupby(['dataItemId','parentFlag','accountingStandardDesc'])['periodEndDate'].min().reset_index()
        # print(temp_df)
        # print(temp_df2)
        
        filter_df= temp_df1[(temp_df1['dataItemId'].isin(temp_df2['dataItemId']))&(temp_df1['parentFlag'].isin(temp_df2['parentFlag']))&(temp_df1['accountingStandardDesc'].isin(temp_df2['accountingStandardDesc']))&(temp_df1['periodEndDate'].isin(temp_df2['periodEndDate']))&((temp_df1['value']=="") | (temp_df1['value'].isnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','periodEndDate']]
        
        # print(filter_df)
        
        min_peos=[]
        diff=[]
        perc=[]

        
        if filter_df is not None:
            for ind,row in filter_df.iterrows():           
                min_peos.append(row['peo'])  
                difference='NA'
                diff.append(difference)
                perc='NA'
            
        diff_df=pd.DataFrame({"peo":min_peos,"diff":diff,"perc":perc})
        
        if len(diff_df)>0:
            temp1=extractedData_parsed[(extractedData_parsed['peo'].isin(min_peos))]

            temp1_revised=temp1.dropna()
        
            for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Min Period is Greater than FY1"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)                       
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_333(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    country1 = get_dataItemIds_list('COUNTRY_INCLUDE', parameters)  # Japan
    try:
        
        if (filingMetadata['metadata']['country'] not in country1):
            temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['parentFlag']=='Parent')&(extractedData_parsed['value'] != "")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId']]
            # print(temp)

            if len(temp) > 0:
                # dataItemIds = []
                # peos = []
                # tid = []
                parentflags = []
                # accounting = []
                # fyc = []
                diff = []
                perc = []

                for ind, row in temp.iterrows():
                    # dataItemIds.append(row['dataItemId'])
                    # peos.append(row['peo'])
                    # tid.append(row['tradingItemId'])
                    parentflags.append(row['parentFlag'])
                    # accounting.append(row['accountingStandardDesc'])
                    # fyc.append(row['fiscalChainSeriesId'])
                    diff = 'NA'
                    perc = 'NA'

                diff_df = pd.DataFrame({"parentflag": parentflags})

                temp1 = extractedData_parsed[((extractedData_parsed['parentFlag'].isin(parentflags)))][['parentFlag','team', 'description','accountingStandardDesc', 'scale', 'currency']]  #[['dataItemId', 'value', 'peo', 'scale', 'currency', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'team', 'description', 'tradingItemName', 'fiscalYear', 'fiscalQuarter','fiscalChainSeriesId']].drop_duplicates()

                temp1_revised = temp1.dropna()
                
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Parent data collected for other Asian comp"}
                    result["highlights"].append({"parentConsolidatedFlag": row['parentFlag'], "row": {"accountingStdDesc": row['accountingStandardDesc']}, "section": row['team'], "scale": row['scale'],  "currency": row['currency'],
                                                 "filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"] = {"statement": "", "tag": row['parentFlag'],"description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],
                                                   "feedFileId": filingMetadata['metadata']['feedFileId'],
                                                   "diff": 'NA', "percent": 'NA'}
                    result["checkGeneratedForList"] = [
                        {"tag": row['parentFlag'], "description": row["description"], "accountingStdDesc": row["accountingStandardDesc"],
                         "parentConsolidatedFlag": row["parentFlag"],
                         "diff": 'NA',
                         "percent": 'NA'}]
                    errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_332(historicalData,filingMetadata,extractedData,parameters):
        errors = []
        left_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        countries=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
        try:
            
            if filingMetadata['metadata']['country'] in countries:
                consolidated = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_list))&(extractedData_parsed['parentFlag']=='Consolidated')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currencyId','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']].drop_duplicates()
                parent = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_list))&(extractedData_parsed['parentFlag']=='Parent')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currencyId','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']].drop_duplicates()                          
                
                # print(consolidated)
                # print(parent)
                
                merged_df=pd.merge(consolidated,parent,on=['dataItemId','peo','accountingStandardDesc','fiscalChainSeriesId','tradingItemId'],how='inner')
                
                base_currency=merged_df.currency_x.mode()[0]
                merged_df['value_scaled_y']=merged_df.apply(lambda x: currency_converter(currency_from=x['currency_y'], currency_to=base_currency, value=x['value_scaled_y']),axis=1)
    
                # print(merged_df)
    
                dataItemIds=[]
                peos=[]
                accounting=[]
                tid=[]
                fyc=[]        
                diff=[]
                perc=[]
                for ind,row in merged_df.iterrows():                    
                    if execute_operator(row['value_scaled_x'], row['value_scaled_y'], operator[0]):    
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId'])
                        accounting.append(row['accountingStandardDesc'])
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'
                diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
                
                # print(diff_df)
                
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']].drop_duplicates()

                temp1_revised=temp1.dropna()
                
                                                                                                                                                                                                                                              
                for ind, row in temp1_revised.iterrows():                        
                    result = {"highlights": [], "error": "consolidate data lessthan Parent data for Korean and Japan Companies"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
         
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors  

#Estimates Error Checks 
@add_method(Validation) 
def EST_63(historicalData,filingMetadata,extractedData,parameters):
    errors = []    
    try:
        companyid=filingMetadata['metadata']['companyId']
        contributor=filingMetadata['metadata']['researchContributorId']
        
        temp0 = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','currencyId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].drop_duplicates()
 
        print(temp0)
        
        if len(temp0)>0:
            
            temp1=historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(historicalData_parsed['dataItemFlag']=="E"))][['dataItemId','peo','value','currencyId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].drop_duplicates()

            print(temp1)

            final=temp0[~(temp0['dataItemId'].isin(temp1['dataItemId'])|temp0['currencyId'].isin(temp1['currencyId'])|temp0['fiscalChainSeriesId'].isin(temp1['fiscalChainSeriesId'])|temp0['accountingStandardDesc'].isin(temp1['accountingStandardDesc'])|temp0['parentFlag'].isin(temp1['parentFlag']))]
            
            print(final)
              
            dataItemIds=[]                                                                                                                                                                                         
            peos=[]
            diff=[]
            perc=[]
            currencyid=[]
            parentflags=[]
            accountingStandard=[]
    
            #if final is not None:
            for ind,row in final.iterrows():
                dataItemIds.append(row['dataItemId'])
                currencyid.append(row['currencyId'])
                parentflags.append(row['parentFlag'])
                accountingStandard.append(row['accountingStandardDesc'])
                peos.append(row['peo'])               
                difference='NA'
                diff.append(difference)
                perc='NA'
            
            diff_df=pd.DataFrame({"peo":peos,"currencyId":currencyid,"diff":diff,"perc":perc,"parentFlag": parentflags})      
            
            print(diff_df)
            
            if len(diff_df)>0:
                final1=extractedData_parsed[((extractedData_parsed['parentFlag'].isin(diff_df[parentflags])&extractedData_parsed['currencyId'].isin(diff_df[currencyid])&extractedData_parsed['accountingStandardDesc'].isin(accountingStandard)))][['parentFlag','team', 'description','accountingStandardDesc', 'scale', 'currencyId', 'currency']].drop_duplicates()    #[['dataItemId', 'peo', 'value', 'scale', 'currency','currencyId', 'parentFlag', 'accountingStandardDesc',
                                      #'tradingItemId', 'team', 'description', 'tradingItemName', 'fiscalYear', 'fiscalQuarter','fiscalChainSeriesId']]
                
                final1_revised = final1.dropna()
                
                print(final1)
                
                for ind, row in final1_revised.iterrows():
                    # result = {"highlights": [], "error": "Initiation at Company Level for different flavors"}
                    # result["highlights"].append({"parentConsolidatedFlag": row['parentFlag'], "row": {"accountingStdDesc": row['accountingStandardDesc'],"currencyId": row['currencyId']}, "section": row['team'], "scale": row['scale'],  "currency": row['currency'],"currencyId": row['currencyId'],
                    #                              "filingId": filingMetadata['metadata']['versionId']})
                    # result["checkGeneratedFor"] = {"statement": "", "tag": row['parentFlag'],"description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],
                    #                                "feedFileId": filingMetadata['metadata']['feedFileId'],
                    #                                "diff": 'NA', "percent": 'NA'}
                    # result["checkGeneratedForList"] = [
                    #     {"tag": row['parentFlag'], "description": row["description"], "accountingStdDesc": row["accountingStandardDesc"],
                    #      "parentConsolidatedFlag": row["parentFlag"],
                    #      "diff": 'NA',
                    #      "percent": 'NA'}]
                    errors.append(result)                    
                    result = {"highlights": [], "error": "Initiation at Company Level for different flavors"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_11A(historicalData,filingMetadata,extractedData,parameters):
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
               
        try:
            quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear','team','description','tradingItemName']]
            fiscalyear = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear']]                          
            
            quarter['peocomb']=quarter[['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
            fiscalyear['peocomb']=fiscalyear[['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)

            quarter_revised=quarter[~(quarter['peocomb'].isin(fiscalyear['peocomb']))]
            
            if len(quarter_revised)>0:
                
                quarter_revised=quarter_revised.drop_duplicates()
                quarter_revised_new=quarter_revised.dropna()

                for ind, row in quarter_revised_new.iterrows():
    
                    result = {"highlights": [], "error": "Value exist for quarters but not for fiscal year"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": 'NA', "scale": 'NA', "value": 'NA', "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":'NA', "peo": 'NA',"value": 'NA',"units": 'NA',"currency": 'NA',"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": 'NA', "percent": 'NA'}]
                    errors.append(result)
                                  
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors    


#Estimates Error Checks 
@add_method(Validation) 
def EST_65(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)  # dataitemslist
    right_dataItemId_list = get_dataItemIds_list('RHSdataItemIds', parameters)


    try:
        if filingMetadata['metadata']['latestPeriodType']=='Q4':
            latestfy=filingMetadata['metadata']['latestPeriodYear']
            
        else:
            latestfy=int(filingMetadata['metadata']['latestPeriodYear'])-1

        # print(latestfy)
        
        epsactual = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['fiscalYear']==latestfy)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'value_scaled', 'tradingItemId', 'periodEndDate']].drop_duplicates()
        
        # print(epsactual)
        if len(epsactual)>0:
            if (epsactual['value_scaled']<0).all():
                temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','team','tradingItemId','accountingStandardDesc','description','tradingItemName']].drop_duplicates()
                # print(temp)
                if len(temp) > 0:

                    temp_revised = temp.dropna()

                    for ind, row in temp_revised.iterrows():
                        result = {"highlights": [], "error": "Base Year Negative_LTGR"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']},"cell": {"peo": 'NA', "scale": 'NA',"value": row['value'], "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],"description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'],"peo": 'NA', "diff": "NA", "percent": "NA"}
                        result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"],"tradingItemId": row["tradingItemId"],"fiscalYear": 'NA',"fiscalQuarter": 'NA', "peo": 'NA', "value": row["value"], "units": 'NA',"currency": 'NA', "tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": 'NA', "fiscalChainSeries": 'NA', "diff": "NA", "percent": "NA"}]
                        errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)     
def EST_57(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    recommendation = get_dataItemIds_list('LHSdataItemIds', parameters)
    targetprice = get_dataItemIds_list('RHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #[!=,>,<]

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']
        companyid = filingMetadata['metadata']['companyId']
               
        current_rating = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(recommendation)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId','value','tradingItemId','accountingStandardDesc']].drop_duplicates()
        
        # print(current_rating)
        
        # current_rating['numaric_value']=current_rating['value'].str[-2].astype(int)
        # current_rating['numeric_value'] = current_rating['value'].str[-2:].astype(int)
        # current_rating['numeric_value'] = current_rating['value'].str.rstrip('.0').astype(int)
        
        
        current_rating['numeric_value'] = current_rating['value'].apply(lambda x: float(re.search(r'\((\d+\.\d+)\)', x).group(1)))
        # print(current_rating)

        previous_rating = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(recommendation)) & (historicalData_parsed['researchContributorId'] == contributor)& (historicalData_parsed['companyId'] == companyid) & (historicalData_parsed['filingDate'] < filingdate) & (historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId','value','tradingItemId','accountingStandardDesc', 'filingDate']].drop_duplicates()
        
        # previous_rating['numaric_value']=int(previous_rating['value'].str[-2])
        #previous_rating['numeric_value'] = previous_rating['value'].str[-2:].astype(int)
        
        # previous_rating['numeric_value'] = previous_rating['value'].str.rstrip('.0').astype(int)
        
        
        
        previous_rating['numeric_value'] = previous_rating['value'].apply(lambda x: float(re.search(r'\((\d+\.\d+)\)', x).group(1)))
        # print(previous_rating)
        
        current_tp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(targetprice)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId','value_scaled','currency','tradingItemId','accountingStandardDesc']].drop_duplicates()
        
        previous_tp = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(targetprice)) & (historicalData_parsed['researchContributorId'] == contributor) & (historicalData_parsed['companyId'] == companyid)& (
                                                      historicalData_parsed['filingDate'] < filingdate) & (historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId','value_scaled','currency','tradingItemId','accountingStandardDesc', 'filingDate']].drop_duplicates()
                                        
        
        # print(current_tp)
        # print(previous_tp)
        
        maxprevious_rating_1 = previous_rating.groupby(['dataItemId','tradingItemId'])['filingDate'].max().reset_index() # 'accountingStandardDesc', 

        maxprevious_rating = previous_rating[(previous_rating['filingDate'].isin(maxprevious_rating_1['filingDate']))]
        
        # print(maxprevious_rating)

        maxprevious_tp_1 = previous_tp.groupby(['dataItemId', 'tradingItemId'])['filingDate'].max().reset_index() # 'accountingStandardDesc',

        maxprevious_tp = previous_tp[(previous_tp['filingDate'].isin(maxprevious_tp_1['filingDate']))]
        
        # print(maxprevious_tp)
        
        merged_rating=pd.merge(current_rating, maxprevious_rating,on=['dataItemId','tradingItemId'],how='inner') #,'accountingStandardDesc'

        merged_tp=pd.merge(current_tp, maxprevious_tp,on=['dataItemId','tradingItemId'],how='inner') #,'accountingStandardDesc'
        
        # print(merged_rating)
        # print(merged_tp)
        
        base_currency=merged_tp.currency_x.mode()[0]
        merged_tp['value_scaled_y']=merged_tp.apply(lambda x: currency_converter(currency_from=x['currency_x'], currency_to=base_currency, value=x['value_scaled_y']),axis=1)
        
        # print(merged_tp)
        
        merged_df = pd.merge(merged_rating, merged_tp, on=['tradingItemId'], how='inner') #'accountingStandardDesc', 
        
        # print(merged_df)
        # print(merged_df[['filingDate_x','filingDate_y','numeric_value_x','numeric_value_y','value_scaled_x','value_scaled_y']])
        

        
        dataItemId_rating = []
        dataItemId_tp = []
        previousdate = []
        tid = []
        diff = []
        perc = []
        
        for ind, row in merged_df.iterrows():
            if ((execute_operator(row['filingDate_x'], row['filingDate_y'], operator[0])) & (execute_operator(row['numeric_value_x'], row['numeric_value_y'], operator[1]))&(execute_operator(row['value_scaled_x'], row['value_scaled_y'], operator[1]))):
                dataItemId_rating.append(row['dataItemId_x'])
                dataItemId_tp.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_x'])
                tid.append(row['tradingItemId'])
                diff='NA'
                perc='NA'
                
            if ((execute_operator(row['filingDate_x'], row['filingDate_y'], operator[0]))&(execute_operator(row['numeric_value_x'], row['numeric_value_y'], operator[2]))&(execute_operator(row['value_scaled_x'], row['value_scaled_y'], operator[2]))):
                dataItemId_rating.append(row['dataItemId_x'])
                dataItemId_tp.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_x'])
                tid.append(row['tradingItemId'])
                diff='NA'
                perc='NA'

        diff_df = pd.DataFrame({"dataItemId_x": dataItemId_rating,"dataItemId_y": dataItemId_tp,"filingDate": previousdate, "diff": diff, "perc": perc})
        
        
        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId_x'])) & (extractedData_parsed['tradingItemId'].isin(tid)) &  (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'value', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'fiscalChainSeriesId', 'team', 'description', 'tradingItemName']].drop_duplicates()

                                              
        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId_y'])) &  (extractedData_parsed['tradingItemId'].isin(tid)) &  (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'value','scale','currency', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'fiscalChainSeriesId', 'team', 'description', 'tradingItemName']].drop_duplicates()

        temp3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId_x'])) & (historicalData_parsed['tradingItemId'].isin(tid)) &(historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()) & (historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (historicalData_parsed['researchContributorId'] == contributor))][['dataItemId', 'value', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'fiscalChainSeriesId',  'team', 'description', 'tradingItemName', 'versionId', 'companyId', 'feedFileId', 'filingDate']].drop_duplicates()

        temp4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId_y'])) &  (historicalData_parsed['tradingItemId'].isin(tid)) &(historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()) & (historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (historicalData_parsed['researchContributorId'] == contributor))][['dataItemId', 'value', 'scale','currency','parentFlag', 'accountingStandardDesc','tradingItemId', 'fiscalChainSeriesId',  'team', 'description', 'tradingItemName', 'versionId', 'companyId', 'feedFileId', 'filingDate']].drop_duplicates()
     
        temp1_revised = temp1.dropna()

        for ind, row in temp1_revised.iterrows():
            result = {"highlights": [],"error": "Non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'],"peo": 'NA',"diff": 'NA',"percent": 'NA'}
            result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": 'NA',"percent": 'NA'}]
            errors.append(result)

        temp2_revised = temp2.dropna()

        for ind, row in temp2_revised.iterrows():
            result = {"highlights": [],"error": "Non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'],"peo": 'NA',"diff": 'NA',"percent": 'NA'}
            result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": 'NA',"percent": 'NA'}]
            errors.append(result)
            
        temp3_revised = temp3.dropna()

        for ind, row in temp3_revised.iterrows():
            result = {"highlights": [],"error": "Non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'],"peo": 'NA',"diff": 'NA',"percent": 'NA'}
            result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": 'NA',"percent": 'NA'}]
            errors.append(result)

        temp4_revised = temp4.dropna()

        for ind, row in temp4_revised.iterrows():
            result = {"highlights": [],"error": "Non periodic data items captured values' combination is not meeting up the standard definition"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"cell": {"peo": 'NA', "scale": 'NA', "value": row['value'],"currency": 'NA'}, "section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'], "description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'],"peo": 'NA',"diff": 'NA',"percent": 'NA'}
            result["checkGeneratedForList"] = [{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter": 'NA', "peo": 'NA',"value": row["value"], "units": 'NA', "currency": 'NA',"tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],"diff": 'NA',"percent": 'NA'}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors    

#Estimates Error Checks 
@add_method(Validation)  
def EST_35(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # tags
    operator = get_dataItemIds_list('Operation', parameters)
    try:
        documentdate=filingMetadata['metadata']['filingDate']

        
        
        temp = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(dataItemId_list)))&(extractedData_parsed['value'] != "") &(extractedData_parsed['value'].notnull())&(extractedData_parsed['peo']!= "")&(extractedData_parsed['peo'].notnull()))][['dataItemId', 'peo', 'fiscalChainSeriesId']].drop_duplicates()
        # print(temp)
        
        temp['companyId'] = filingMetadata['metadata']['companyId']

        # print(temp)
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['value'] != "")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['companyId'].isin(temp['companyId']))&(historicalData_parsed['peo']!="")&(historicalData_parsed['filingDate'] < documentdate)&(historicalData_parsed['peo'].notnull()))][['dataItemId', 'peo','fiscalChainSeriesId', 'filingDate','companyId']].drop_duplicates()
        
        # print(previous)
        
        maxprevious = previous.groupby(['companyId'])['filingDate'].max().reset_index()
        # print(maxprevious)

        previous = previous[previous['filingDate'].isin(maxprevious['filingDate'])]

        # print(previous)

        merged_df = pd.merge(temp, previous, on=['companyId'], how='inner')

        # print(merged_df)

        filingdate = []
        diff = []
        perc = []
        series1 = []
        series2 = []
        
        for ind, row in merged_df.iterrows():
            if execute_operator(row['fiscalChainSeriesId_x'], row['fiscalChainSeriesId_y'], operator[0]):
                filingdate.append(row['filingDate'])
                difference = 'NA'
                series1.append(row['fiscalChainSeriesId_x'])
                series2.append(row['fiscalChainSeriesId_y'])
                diff.append(difference)
                perc = 'NA'

        diff_df = pd.DataFrame({"diff": diff, "perc": perc, "filingDate": filingdate, "curseries": series1, "preseries": series2}).drop_duplicates()
        #print(diff_df)
        temp1 = extractedData_parsed[(extractedData_parsed['fiscalChainSeriesId'].isin(series1))][['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team']].drop_duplicates()
        #print(temp1)
        temp2 = historicalData_parsed[((historicalData_parsed['filingDate'].isin(diff_df['filingDate'])) & (historicalData_parsed['fiscalChainSeriesId'].isin(series2)) )][['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team', 'versionId', 'feedFileId','filingDate', 'companyId']].drop_duplicates()


        if len(temp1) > 0 and len(temp2) > 0:
            temp1_revised = temp1.dropna()
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],"error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},"cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA","versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [{"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA","peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA","accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)

            temp2_revised = temp2.dropna()
            
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},"cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA","versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [{"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA","peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA","accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)


        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
    
#Estimates Error Checks 
@add_method(Validation) 
def EST_56D(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    tag_list=get_dataItemIds_list('TAG1', parameters) #CAPEX
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #CFO
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #FCF
        CAPEX_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #CAPEX

        # print(lhs_df)
        # print(rhs_df)
        # print(CAPEX_df)

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(CAPEX_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            CAPEX_df["value_scaled"] = CAPEX_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_CAPEX_df = pd.merge(CAPEX_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        # print(merged_CAPEX_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if merged_CAPEX_df is not None:
        for ind,row in merged_CAPEX_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(float(round(difference)))
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        if len(diff_df)>0:
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName']].drop_duplicates()

            temp1_revised=temp1.dropna()
                    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "CFO =FCF and Capex available"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_47A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #['>]
    variation=get_parameter_value(parameters,'Max_Threshold') #100%
    
    try:
        yesscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']!=-1)&(extractedData_parsed['currencyId']!=-1)&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        volume = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']!=-1)&((extractedData_parsed['currencyId']==-1)|(extractedData_parsed['currencyId']==0))&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]

        noscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']==-1)&(extractedData_parsed['value'].notnull())&(extractedData_parsed['consValue']!="")&(extractedData_parsed['consValue'].notnull()))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        
        # print(yesscale)
        print(volume)
        print(noscale)
        #&(extractedData_parsed['dataItemFlag']=="E")
        
        if len(yesscale)>0:
            yesscale['consValue_scaled'] = yesscale.apply(lambda row: get_scaled_value(row['consValue'], row['consScaleId']), axis=1)
            
            base_currency=yesscale.consCurrency.mode()[0]

            yesscale["value_scaled"] = yesscale.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            yesscale["consValue_scaled"] = yesscale.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
        
        if len(volume)>0:
            volume['consValue_scaled'] = volume.apply(lambda row: get_scaled_value(row['consValue'], row['consScaleId']), axis=1)
    
        if len(noscale)>0: 
            noscale['value_scaled'] = pd.to_numeric(noscale['value_scaled'],errors = 'coerce')
            noscale['consValue_scaled'] = pd.to_numeric(noscale['consValue'],errors = 'coerce')
            
            
        if (len(yesscale)>0 or len(noscale)>0 or len(volume)>0):

            frames = [yesscale, volume,noscale]
            temp = pd.concat(frames)[['dataItemId','peo','value_scaled','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId','consValue_scaled']]
            
            temp['consensusvariation']=((abs(((temp['value_scaled']).astype(float))-((temp['consValue_scaled']).astype(float))))/(abs(temp[['value_scaled','consValue_scaled']])).min(axis=1))*100
            
            temp.replace([np.inf, -np.inf], np.nan, inplace=True)
            temp.dropna(inplace=True)

            dataItemIds=[]
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            
            for ind,row in temp.iterrows():
                if execute_operator(float(row['consensusvariation']),float(variation[0]),operator[0]):
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])  
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId']) 
                    diff.append(float(round(row['consensusvariation'])))
                    perc.append(float(round(row['consensusvariation'])))
                    
    
            diff_df=pd.DataFrame({"peo":peos,"dataItemId":dataItemIds,"diff":diff,"perc":perc})
                        

            diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                
            temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']].drop_duplicates() 
            

            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "%Variance with consensus"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid":filingMetadata['metadata']['companyId']},"cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],"diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                # result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                # result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                # result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":row["estimatePeriodId"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)                          
                # errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors   
            
#Estimates Error Checks 
@add_method(Validation) 
def EST_22C(historicalData,filingMetadata,extractedData,parameters):
    #Json10
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters) #['!=]

    try:
        FQ = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())  & ((extractedData_parsed["periodTypeId"] == 2)|(extractedData_parsed["periodTypeId"] == 10)))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())& (extractedData_parsed["periodTypeId"] == 1))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        # print(FQ)
        # print(FY)

        FQ["valuesign"]=np.sign((FQ['value']).astype(float))
        FY["valuesign"]=np.sign((FY['value']).astype(float))

        # print(FQ)
        # print(FY)

        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId',],how='inner')

        # print(merged_df)
        # print(merged_df[['periodTypeId_x','periodTypeId_y']])

        merged_df_PEO_count= merged_df.groupby(['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId_x']).agg(quarter_sum=('valuesign_x','sum'),fy_sum=('valuesign_y','sum')).reset_index() 



        # print(merged_df_PEO_count)

        peos=[]
        diff=[]
        perc=[]
        dataItemIds=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff='NA'
        perc='NA'
        
        for ind,row in merged_df_PEO_count.iterrows():
            if (row['periodTypeId_x']==2 and abs(row['fy_sum'])==4 and abs(row['quarter_sum'])==4):
                if execute_operator(row['quarter_sum'],row['fy_sum'],operator[0]):
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])

        for ind,row in merged_df_PEO_count.iterrows():
            if (row['periodTypeId_x']==10 and abs(row['fy_sum'])==2 and abs(row['quarter_sum'])==2):
                if execute_operator(row['quarter_sum'],row['fy_sum'],operator[0]):
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])


        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"fiscalYear":peos,"diff":diff,"perc":perc})
        
        diff_df['peocomb']=diff_df.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1)
        extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'],x['fiscalYear']),axis=1 )
        
        temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))
                                                  &(extractedData_parsed['value'] != "") &(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName','peocomb']] #.drop_duplicates()    

        temp1_revised=temp1.dropna()
        
        for ind, row in temp1_revised.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "All Quarter OR Semis values are collected in one sign and Fiscal Year value collected in another sign."}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                    

        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def EST_101(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    operator=get_dataItemIds_list('Operation', parameters)

    try:
        latestactual=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo']).date()
        
        temp = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())
                                     &(extractedData_parsed['peo']!="")&(extractedData_parsed['peo'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','value','tradingItemId','periodEndDate']]
        
        if len(temp)>0:
            temp['periodEndDate']=pd.to_datetime(temp['periodEndDate']).dt.date
            dataItemIds=[]
            parentflag=[]
            peo=[]
            AS=[]
            tradingitem=[]
            period=[]
            for ind, row in temp.iterrows():
               if execute_operator(latestactual,row['periodEndDate'], operator[0]):

                    dataItemIds.append(row['dataItemId'])
                    parentflag.append(row['parentFlag'])
                    AS.append(row['accountingStandardDesc'])
                    tradingitem.append(row['tradingItemId'])
                    peo.append(row['peo'])
                    period.append(row['periodEndDate'])
                
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"period":period})
            
            if len(diff_df)>0:
                diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['tradingItemId'].isin(tradingitem))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(AS))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId']]
    
                temp1_revised=temp1.dropna() 
        
                for ind, row in temp1_revised.iterrows():
        
                    result = {"highlights": [], "error": "Actual already collected for the PEO"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": "NA", "percent": "NA"}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                    errors.append(result)
         
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors


#Estimates Error Checks 
@add_method(Validation) 
def EST_10G(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #REVENUE
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT

    try:
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates() #REVENUE
        
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']].drop_duplicates()

        
        # print(lhs_df)
        # print(rhs_df)
        
        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
            
        # print(merged_df)
        
        dataItemIds_x=[]    
        dataItemIds_y=[]        
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        diff='NA'
        perc='NA'

        for ind,row in merged_df.iterrows():
                if row['value_scaled_x']==0.0:
                    if row['value_scaled_y']>0:
                        dataItemIds_x.append(row['dataItemId_x'])
                        dataItemIds_y.append(row['dataItemId_y'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
        diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"peo":peos,"diff":diff,"perc":perc}) 
                           
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']].drop_duplicates()

        temp1_revised=temp1.dropna()

        for ind, row in temp1_revised.iterrows():
            result = {"highlights": [], "error": "Revenue Estimate is Zero and EBIT in Positive"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_18B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if merged_df is not None:
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(float(round(difference)))
                perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
       
        if len(diff_df)>0:
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())))][['fiscalChainSeriesId','dataItemId', 'peo', 'value', 'scale','currency','accountingStandardDesc','parentFlag','team','description','tradingItemId', 'fiscalYear', 'fiscalQuarter','tradingItemName']]
    
            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Same values collected for Capex and Free cash flow"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_11B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag1_list=get_dataItemIds_list('TAG1', parameters) # NI GAAP
    tag2_list=get_dataItemIds_list('TAG2', parameters) # NI Normalized
    tag3_list=get_dataItemIds_list('TAG3', parameters) # EPS GAAP
    tag4_list=get_dataItemIds_list('TAG4', parameters) # EPS Normalized
    tag5_list=get_dataItemIds_list('LHSdataItemIds', parameters) # EBT GAAP
    tag6_list=get_dataItemIds_list('RHSdataItemIds', parameters) # EBT Normalized
    operator = get_dataItemIds_list('Operation', parameters) #["=="],["!="] 
    max_threshold=get_parameter_value(parameters,'Max Threshold')  #[4]
    min_threshold=get_parameter_value(parameters,'Min_Threshold') #[1]
    try:

                
        eps_gaap_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag3_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        eps_nor_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag4_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        eps_fy = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag3_list))|(extractedData_parsed['dataItemId'].isin(tag4_list)))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        ni_gaap_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag1_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        
        ni_nor_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag2_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        ni_fy = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag1_list))|(extractedData_parsed['dataItemId'].isin(tag2_list)))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        
        ebt_gaap_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag5_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        
        ebt_nor_quarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag6_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        ebt_fy = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag5_list))|(extractedData_parsed['dataItemId'].isin(tag6_list)))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        merged_eps_quarter=pd.merge(eps_gaap_quarter,eps_nor_quarter,on=['peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        eps_fy_group=eps_fy.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        eps_quarter_group=merged_eps_quarter.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        eps_merged_df=pd.merge(eps_fy_group, eps_quarter_group,on=['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear'],how='inner')


        merged_ni_quarter=pd.merge(ni_gaap_quarter,ni_nor_quarter,on=['peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        ni_fy_group=ni_fy.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        ni_quarter_group=merged_ni_quarter.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        ni_merged_df=pd.merge(ni_fy_group, ni_quarter_group,on=['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear'],how='inner')
        
                            
        fiscalyear=[]
        parentflag=[]
        accounting=[]
        tid=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind,row in eps_merged_df.iterrows():

            if ((execute_operator(row['peo_count_x'],float(min_threshold[0]),operator[0]))&(execute_operator(row['peo_count_y'],float(max_threshold[0]),operator[0]))):

                fiscalyear.append(row['fiscalYear'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                tid.append(row['tradingItemId'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'

        diff_df1=pd.DataFrame({"fiscalYear":fiscalyear,"diff":diff,"perc":perc})

        eps = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag3_list))|(extractedData_parsed['dataItemId'].isin(tag4_list)))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (extractedData_parsed['fiscalYear'].isin(diff_df1['fiscalYear']))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','tradingItemName','fiscalYear','fiscalQuarter']]

        fiscalyear=[]
        parentflag=[]
        accounting=[]
        tid=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind,row in ni_merged_df.iterrows():
 
            if ((execute_operator(row['peo_count_x'],float(min_threshold[0]),operator[0]))&(execute_operator(row['peo_count_y'],float(max_threshold[0]),operator[0]))):

                fiscalyear.append(row['fiscalYear'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'

        diff_df2=pd.DataFrame({"fiscalYear":fiscalyear,"diff":diff,"perc":perc})


        ni = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag1_list))|(extractedData_parsed['dataItemId'].isin(tag2_list)))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (extractedData_parsed['fiscalYear'].isin(diff_df2['fiscalYear']))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','tradingItemName','fiscalYear','fiscalQuarter']]

        merged_ebt_quarter=pd.merge(ebt_gaap_quarter,ebt_nor_quarter,on=['peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        ebt_fy_group=ebt_fy.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        ebt_quarter_group=merged_ebt_quarter.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']).agg(peo_count=('peo','count')).reset_index()

        ebt_merged_df=pd.merge(ebt_fy_group, ebt_quarter_group,on=['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear'],how='inner')

        fiscalyear=[]
        parentflag=[]
        accounting=[]
        tid=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind,row in ebt_merged_df.iterrows():

            if ((execute_operator(row['peo_count_x'],float(min_threshold[0]),operator[0]))&(execute_operator(row['peo_count_y'],float(max_threshold[0]),operator[0]))):

                fiscalyear.append(row['fiscalYear'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'

        diff_df3=pd.DataFrame({"fiscalYear":fiscalyear,"diff":diff,"perc":perc})

        ebt = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag5_list))|(extractedData_parsed['dataItemId'].isin(tag6_list)))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (extractedData_parsed['fiscalYear'].isin(diff_df3['fiscalYear']))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','tradingItemName','fiscalYear','fiscalQuarter']]
        
        eps_revised=eps.dropna()

        for ind, row in eps_revised.iterrows():

            result = {"highlights": [], "error": "All Q's duplicated but we have single flavor for FY"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df1[diff_df1['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df1[diff_df1['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
            errors.append(result)
        
        ni_revised=ni.dropna()

        for ind, row in ni_revised.iterrows():

            result = {"highlights": [], "error": "All Q's duplicated but we have single flavor for FY"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df2[diff_df2['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df2[diff_df2['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df2[diff_df2['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df2[diff_df2['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
            errors.append(result)
            
        ebt_revised=ebt.dropna()
        for ind, row in ebt_revised.iterrows():

            result = {"highlights": [], "error": "All Q's duplicated but we have single flavor for FY"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df3[diff_df3['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df3[diff_df3['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df3[diff_df3['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df3[diff_df3['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
            errors.append(result)  
            
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors    
    
#Estimates Error Checks 
@add_method(Validation) 
def EST_15A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:
        if len(extractedData_parsed)>0:
            temp0 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','scale','scaleId','value','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']].drop_duplicates()

            temp1=temp0.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])['scaleId'].nunique().reset_index(name='countunits')

            if len(temp0)>0:
                merged_df=pd.merge(temp0,temp1[temp1['countunits']>1],on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner') 
                

                dataItemIds=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[]
                peos=[]
                
                for ind, row in merged_df.iterrows():
                    if row['countunits'] > 1:
                        dataItemIds.append(row['dataItemId'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        peos.append(row['peo'])
                        diff='NA'
                        perc='NA'

                    
                diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,"tradingItemId":tid,"parentFlag":parentflag,"accountingStandardDesc":accounting,"fiscalChainSeriesId":fyc})
                
                if len(diff_df)>0:
                    diff_df['peocomb']=diff_df[['dataItemId','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed[['dataItemId','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)

                    temp2=extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId','dataItemId', 'accountingStandardDesc','parentFlag','team','description','tradingItemId', 'tradingItemName','peocomb']].drop_duplicates()
            
                    temp2_revised=temp2.dropna()

                    for ind, row in temp2_revised.iterrows():
                        result = {"highlights": [], "error": "Unit Variation"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid":filingMetadata['metadata']['companyId']},"cell": {"peo": 'NA', "scale": 'NA', "value": 'NA', "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": 'NA',"units": 'NA',"currency": 'NA',"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation) 
def EST_14(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    # operator = get_dataItemIds_list('Operation', parameters) #["==","!="]
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:
        temp0 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        
        # print(temp0)
        
        temp1=temp0.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])['currency'].nunique().reset_index(name='currencycount')

        # print(temp1)          
        if len(temp0)>0:
            merged_df=pd.merge(temp0,temp1[temp1['currencycount']>1],on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner') 

        # print(merged_df)

            dataItemIds=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
        
        # if merged_df is not None:
            for ind, row in merged_df.iterrows():
                if row['currencycount'] > 1:
                    # if execute_operator(row['peo_x'],row['peo_y'],operator[1]):
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'

            
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc})
            
            if len(diff_df)>0:
                temp2=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))
                                                          &(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear', 'fiscalQuarter','tradingItemName']]
        
                temp2_revised=temp2.dropna()
                
                for ind, row in temp2_revised.iterrows():
                                
                    result = {"highlights": [], "error": "Tags which have currency variation"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                    errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
    
#Estimates Error Checks 
@add_method(Validation) 
def EST_60(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    # dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:

        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(~(extractedData_parsed['parentFlag'].isin(historicalData_parsed['parentFlag']))
                                        |~(extractedData_parsed['tradingItemId'].isin(historicalData_parsed['tradingItemId']))
                                        |~(extractedData_parsed['accountingStandardDesc'].isin(historicalData_parsed['accountingStandardDesc']))
                                        |~(extractedData_parsed['fiscalChainSeriesId'].isin(historicalData_parsed['fiscalChainSeriesId']))
                                        ))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]  #(extractedData_parsed['dataItemFlag']=="E")
        

        # print(current)
        if len(current)>0:
            dataItemIds=[]
            parentflag=[]
            accounting=[]
            # peos=[]
            tid=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in current.iterrows():
                dataItemIds.append(row['dataItemId'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                # peos.append(row['peo'])
                diff='NA'
                perc='NA'
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc}) #'peo':peos
            
            # print(diff_df)

            temp= extractedData_parsed[(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&((extractedData_parsed['parentFlag'].isin(parentflag))|(extractedData_parsed['tradingItemId'].isin(tid))|(extractedData_parsed['accountingStandardDesc'].isin(accounting))|(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))][['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','team','description','tradingItemName']].drop_duplicates()

            temp_revised=temp.dropna()

            for ind, row in temp_revised.iterrows():
                result = {"highlights": [], "error": "New flavor captured for the company"}
                # result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": 'NA', "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                # result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                # result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA', "diff": 'NA', "percent": 'NA'}]
                # errors.append(result) 
                result["highlights"].append({"row": {"name": 'NA', "id": 'NA',"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": 'NA', "scale": 'NA', "value": 'NA', "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": 'NA', "description": 'NA', "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                result["checkGeneratedForList"]=[{"tag": 'NA', "description": 'NA', "tradingItemId": row["tradingItemId"],"tradingItemName": row['tradingItemName'],"accountingStdDesc": row['accountingStandardDesc'],"parentConsolidatedFlag": row['parentFlag'],"fiscalChainSeries": row['fiscalChainSeriesId'],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": 'NA',"units": 'NA',"currency": 'NA', "diff": 'NA', "percent": 'NA'}]
                errors.append(result)                    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_10F(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBT_GAAP #EBT_Norm
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #ETR

    try:
        
        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 
        rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 


        if ((len(lhs_df)>0) & (len(rhs_df)>0)):
            lhs_df['value_sign']=np.sign(lhs_df['value_scaled'])
            rhs_df['value_sign']=np.sign(rhs_df['value_scaled'])
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_sign'].sum().reset_index() #EBITDA
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_sign'].sum().reset_index() #EBIT  

            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

            if len (merged_df)>0:
                
                peos=[]
                diff=[]
                perc=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[] 

                for ind,row in merged_df.iterrows():
                    if ((row['value_sign_x']<0 )& (row['value_sign_y']!=0)):
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
                
                if len(diff_df)>0:

                    temp1 = extractedData_parsed[((extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId']]
        
                    temp1_revised=temp1.dropna() 


                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Effective Tax Rate  (%) collected when EBT  is negative"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_56B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA
    operator = get_dataItemIds_list('Operation', parameters) #["<"]
    try:
        if len(extractedData_parsed)>0:
            lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA 
            rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
            DA_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            

            if ((len(lhs_df)>0) & (len(rhs_df)>0) & (len(DA_df)>0)):

                base_currency=lhs_df.currency.mode()[0]
                lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                DA_df["value_scaled"] = DA_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
    
            
                merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
                merged_DA_df = pd.merge(DA_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
                merged_DA_df['variation'] = (((merged_DA_df[['value_scaled_x', 'value_scaled_y']].max(axis=1)) - (merged_DA_df[['value_scaled_x', 'value_scaled_y']].min(axis=1))) / merged_DA_df[['value_scaled_x', 'value_scaled_y']].min(axis=1)) * 100

                peos=[]
                diff=[]
                perc=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]
            
                for ind,row in merged_DA_df.iterrows():
                    if (row['value_scaled']==0):
                        if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                            peos.append(row['peo'])
                            tid.append(row['tradingItemId'])
                            parentflag.append(row['parentFlag'])
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff.append(float(round(row['variation'])))
                            perc.append(float(round(row['variation'])))

   
                    diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc,"tradingItemId":tid,"parentFlag":parentflag,"accountingStandardDesc":accounting,"fiscalChainSeriesId":fyc})
            
                    if len(diff_df)>0:    
                        diff_df['peocomb']=diff_df[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
                        extractedData_parsed['peocomb']=extractedData_parsed[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)

                        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peocomb'].isin(diff_df['peocomb']))
                                                      &(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']].drop_duplicates()
                
                        temp1_revised=temp1.dropna()
                
                        for ind, row in temp1_revised.iterrows():
                            result = {"highlights": [], "error": "D&A = 0, but EBIT < EBITDA"}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation) 
def EST_56I(historicalData,filingMetadata,extractedData,parameters):
    #JSON 15
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO, CAPEX, M.CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:

        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX, M.CAPEX

        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        
        # print(lhs_df)
        # print(rhs_df)


        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
                
        # print(lhs_df)
        # print(rhs_df)

        #if len(left_dataItemIds_list)==3:
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        # print(merged_df)


        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if merged_df is not None:
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
                perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        if len(diff_df)>0:
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'scale', 'currency', 'description','team', 'fiscalYear', 'fiscalQuarter','tradingItemName']]
    
            temp1_revised=temp1.dropna()
            
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Free Cash Flow is equal to sum of Capex, M.capex and Cash from Operations"}
                # result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                # result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                # result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                # errors.append(result) 
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_56C(historicalData,filingMetadata,extractedData,parameters):
    #JSON 16
    errors = []
    left_dataItemIds_list = get_dataItemIds_list('LHSdataItemIds', parameters)  # Revenue
    right_dataItemIds_list = get_dataItemIds_list('RHSdataItemIds', parameters)  # EBIT
    tag_list = get_dataItemIds_list('TAG1', parameters)  # GM

    try:
        if len(extractedData_parsed)>0:
            gm = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list)) &
                                    (extractedData_parsed['consValue']!="")&
                                    (extractedData_parsed['consValue'].notnull()))][['dataItemId', 'peo', 'parentFlag', 'consValue', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter']]  # GM

            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) &
                                        (extractedData_parsed['value'] != "") &
                                        (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter']]  # Revenue
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) &
                                        (extractedData_parsed['value'] != "")
                                        & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear', 'fiscalQuarter']]  # EBIT


            merged_df = pd.merge(lhs_df, rhs_df, on=['peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                                    'fiscalChainSeriesId', 'periodTypeId', 'fiscalYear',
                                                    'value_scaled'], how='inner')
            merged_DA_df = pd.merge(gm, merged_df,
                                    on=['parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId',
                                        'peo'], how='inner')

            if len(merged_DA_df)>0:
                peos = []
                tid = []
                parentflag = []
                accounting = []
                fyc = []
                dataitem_x = []
                dataitem_y = []
                diff = []
                perc = []

                for ind, row in merged_DA_df.iterrows():

                    peos.append(row['peo'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    dataitem_x.append(row['dataItemId_x'])
                    dataitem_y.append(row['dataItemId_y'])
                    diff = 'NA'
                    perc = 'NA'

                diff_df = pd.DataFrame({"peo": peos, "diff": diff, "perc": perc,"tradingItemId":tid,"parentFlag":parentflag,"accountingStandardDesc":accounting,"fiscalChainSeriesId":fyc}).drop_duplicates()

                if len(diff_df)>0:
                    diff_df['peocomb']=diff_df[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)

                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb'])) &
                                                (extractedData_parsed['dataItemId'].isin(dataitem_y))&
                                                (extractedData_parsed['value'] != "") &
                                                (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value', 'scale', 'currency', 'parentFlag', 'accountingStandardDesc',
                        'tradingItemId', 'team', 'description', 'tradingItemName', 'fiscalYear', 'fiscalQuarter','fiscalChainSeriesId','peocomb']].drop_duplicates()

                    temp1_revised = temp1.dropna()

                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Revenue = EBIT , GM is available"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_56G(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA

    try:
        if len(extractedData_parsed)>0:
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
            DA_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag_list))&(historicalData_parsed['periodTypeId']!=1)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['fiscalYear'].isin(lhs_df['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA


            if ((len(lhs_df)>0) & (len(rhs_df)>0)& (len(DA_df)>0)):

                base_currency=lhs_df.currency.mode()[0]
                lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
                merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','value_scaled'],how='inner')

                merged_DA_df = pd.merge(DA_df,merged_df,on=['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear'],how='inner')
                

                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]
                dataitem=[]
       
                for ind, row in merged_DA_df.iterrows():
                    if (row['value_scaled_x']!=0):
                        peos.append(row['peo_y'])
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc'])
                        fyc.append(row['fiscalChainSeriesId'])
                        dataitem.append(row['dataItemId_x'])
        
                diff_df=pd.DataFrame({"peo":peos,"tradingItemId":tid,"parentFlag":parentflag,"accountingStandardDesc":accounting,"fiscalChainSeriesId":fyc}).drop_duplicates()
        
                if len(diff_df)>0:
                    diff_df['peocomb']=diff_df[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed[['peo','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']].astype(str).apply(lambda x: ''.join(x),axis=1)
        
                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId']].drop_duplicates()
                    temp1_revised=temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "EBIT = EBITDA  in FY , D&A with Actual value in recent Qs"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": 'NA', "percent": 'NA'}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": 'NA', "percent": 'NA'}]
                        errors.append(result)
    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_321(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    countryCode=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
    try:
        if len(extractedData_parsed)>0:
            if filingMetadata['metadata']['country'] in countryCode:
                            
                temp = extractedData_parsed[((extractedData_parsed['periodTypeId']==10)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['team']].drop_duplicates()

                
                if len(temp)>0:
                        temp_revised=temp.dropna()

                        for ind, row in temp_revised.iterrows():                               
                            result = {"highlights": [], "error": "Semi-annual value for US and Canada companies"}
                            result["highlights"].append({"row": {"name": 'NA', "id": 'NA',"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": 'NA', "scale": 'NA', "value": 'NA', "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": 'NA', "description": 'NA', "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                            result["checkGeneratedForList"]=[{"tag": 'NA', "description": 'NA', "tradingItemId": 'NA',"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": 'NA',"units": 'NA',"currency": 'NA',"tradingItemName": 'NA',"accountingStdDesc": 'NA',"parentConsolidatedFlag": 'NA',"fiscalChainSeries": 'NA', "diff": 'NA', "percent": 'NA'}]
                            errors.append(result)                    
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors 


#Estimates Error Checks 
@add_method(Validation) 
def EST_333(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list = get_dataItemIds_list('LHSdataItemIds', parameters)
    country1 = get_dataItemIds_list('COUNTRY_INCLUDE', parameters)  # Japan
    try:
        
        if (filingMetadata['metadata']['country'] not in country1):
            temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['parentFlag']=='Parent')&(extractedData_parsed['value'] != "")&(extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'value', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId']]
            # print(temp)

            if len(temp) > 0:
                # dataItemIds = []
                # peos = []
                # tid = []
                parentflags = []
                # accounting = []
                # fyc = []
                diff = []
                perc = []

                for ind, row in temp.iterrows():
                    # dataItemIds.append(row['dataItemId'])
                    # peos.append(row['peo'])
                    # tid.append(row['tradingItemId'])
                    parentflags.append(row['parentFlag'])
                    # accounting.append(row['accountingStandardDesc'])
                    # fyc.append(row['fiscalChainSeriesId'])
                    diff = 'NA'
                    perc = 'NA'

                diff_df = pd.DataFrame({"parentflag": parentflags})

                temp1 = extractedData_parsed[((extractedData_parsed['parentFlag'].isin(parentflags)))][['parentFlag','team', 'description','accountingStandardDesc', 'scale', 'currency']]  #[['dataItemId', 'value', 'peo', 'scale', 'currency', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'team', 'description', 'tradingItemName', 'fiscalYear', 'fiscalQuarter','fiscalChainSeriesId']].drop_duplicates()

                temp1_revised = temp1.dropna()
                
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Parent data collected for other Asian comp"}
                    result["highlights"].append({"parentConsolidatedFlag": row['parentFlag'], "row": {"accountingStdDesc": row['accountingStandardDesc']}, "section": row['team'], "scale": row['scale'],  "currency": row['currency'],
                                                 "filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"] = {"statement": "", "tag": row['parentFlag'],"description": row["description"],"versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],
                                                   "feedFileId": filingMetadata['metadata']['feedFileId'],
                                                   "diff": 'NA', "percent": 'NA'}
                    result["checkGeneratedForList"] = [
                        {"tag": row['parentFlag'], "description": row["description"], "accountingStdDesc": row["accountingStandardDesc"],
                         "parentConsolidatedFlag": row["parentFlag"],
                         "diff": 'NA',
                         "percent": 'NA'}]
                    errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_102(historicalData,filingMetadata,extractedData,parameters):
    errors = []   
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #[!=]
    
    try:
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']
        
        # print(extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))
        # print(historicalData_parsed['companyId'])
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','scale','currency','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()
        # print(current)

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','scale','currency','currencyId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']].drop_duplicates()
        
        # print(current)
        # print(previous)
        
        # maxprevious1=previous.groupby(['researchContributorId'])['filingDate'].max().reset_index()
        
        maxprevious1=previous.groupby(['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]

        if ((len(current)>0) & (len(maxprevious)>0)):
            merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner').drop_duplicates()
        
        # print(merged_df)
        # print(merged_df[['dataItemId','peo','value_x','value_y','currency_x','currency_y','currencyId_x','currencyId_y']])

            dataItemIds=[]
            previousdate=[]
            parentflag=[]
            peos=[]
            curx=[]
            cury=[]
            AS=[]
            fyc=[]
            diff=[]
            perc=[]
            
            for ind, row in merged_df.iterrows():
                if execute_operator(row['currencyId_x'],row['currencyId_y'],operator[0]):
                    peos.append(row['peo'])
                    curx.append(row['currencyId_x'])
                    cury.append(row['currencyId_y'])
                    dataItemIds.append(row['dataItemId'])
                    previousdate.append(row['filingDate'])
                    parentflag.append(row['parentFlag'])
                    AS.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'  
                    
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"filingDate":previousdate,"diff":diff,"perc":perc})
            # print(diff_df)
            
    
            if len(diff_df)>0:
                # diff_df['curcomb']=diff_df.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['currencyId']),axis=1)
                # historicalData_parsed['curcomb']=historicalData_parsed.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['currencyId']),axis=1)
        
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(~(extractedData_parsed['currencyId'].isin(cury)))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(AS))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','fiscalYear','fiscalQuarter','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName']].drop_duplicates()
                # print(temp1)
    
                # temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(historicalData_parsed['currencyId'].isin(cury)) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['researchContributorId']==contributor))] [['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()
                # print(temp2)
    
                temp1_revised=temp1.dropna()    
                # temp2_revised=temp2.dropna() 
                
                # print(temp1_revised)
    
                for ind, row in temp1_revised.iterrows():
    
                    result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                    # result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"section": row['team'], "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    # result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    # result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    # errors.append(result)
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'], "versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                # for ind, row in temp2_revised.iterrows():
                #         result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                #         result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},"section": row['team'], "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                #         result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                #         result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                #         errors.append(result)
            
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_5B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)    
    scale_list=get_dataItemIds_list('Scale_list', parameters)
    try:

        res = [eval(i) for i in scale_list]
        print(res)
        non_periodic = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['tradingItemId']!=-1)&(extractedData_parsed['scaleId']!=-1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&~(extractedData_parsed['scaleId'].isin(res)))][['dataItemId','value','scaleId','currency','tradingItemId','accountingStandardDesc']]

        periodic = extractedData_parsed[((extractedData_parsed['tradingItemId']!=-1)&(extractedData_parsed['scaleId']!=-1)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&~(extractedData_parsed['scaleId'].isin(res))&~(extractedData_parsed['dataItemId'].isin(dataItemId_list)))][['dataItemId','peo','value','scaleId','currency','tradingItemId','accountingStandardDesc','periodTypeId','fiscalYear','fiscalQuarter','parentFlag','fiscalChainSeriesId']]
        
        #print(non_periodic)
        if len(periodic)>0:
            dataItemIds=[]
            tid=[]
            peo=[]
            parentflag=[]
            fyc=[]
            accounting=[]
            diff=[]
            perc=[]
            for ind, row in periodic.iterrows():
    
                dataItemIds.append(row['dataItemId'])
                peo.append(row['peo'])
                parentflag.append(row['parentFlag'])
                tid.append(row['tradingItemId']) 
                fyc.append(row['fiscalChainSeriesId'])
                accounting.append(row['accountingStandardDesc'])
                diff='NA'                    
                perc='NA'        

            diff_df1=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"diff":diff,"perc":perc,"tradingItemId":tid})   

    
            if len(diff_df1)>0:
                diff_df1['peocomb']=diff_df1.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
    
                temp0 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df1['peocomb']))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','peocomb']]
                
                temp0_revised=temp0.dropna()
                
                for ind, row in temp0_revised.iterrows():
                    result = {"highlights": [], "error": "Per share data item captured other than in absolutes or 1/100"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df1[diff_df1['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['peocomb']==row["peocomb"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df1[diff_df1['peocomb']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['peocomb']==row["peocomb"]]['perc'].iloc[0]}]
                    errors.append(result)
        
        if len(non_periodic)>0:
            dataItemIds=[]
            tid=[]
            accounting=[]
            diff=[]
            perc=[]
            for ind, row in non_periodic.iterrows():
    
                dataItemIds.append(row['dataItemId'])
                tid.append(row['tradingItemId']) 
                accounting.append(row['accountingStandardDesc'])
                diff='NA'                    
                perc='NA'        
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,"tradingItemId":tid})


            if len(diff_df)>0:       
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName']]
    
                temp1_revised=temp1.dropna()
            
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Per share data item captured other than in absolutes or 1/100"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo":'NA', "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries":'NA', "diff": 'NA', "percent": 'NA'}]
                    errors.append(result)
        
  
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Estimates Error Checks 
@add_method(Validation) 
def EST_6A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max Threshold') 
    
    try:
     
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']


        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','parentFlag','accountingStandardDesc','tradingItemId']]


        previous = historicalData_parsed[((historicalData_parsed['dataItemFlag']=='E')&(historicalData_parsed['value']!="")&
                                          (historicalData_parsed['researchContributorId']==contributor)&
                                          (historicalData_parsed['filingDate']<filingdate)&
                                          (historicalData_parsed['value'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate','researchContributorId']]
        
        previous['daysdiff'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(previous['filingDate'])).dt.days)

      
        maxprevious=previous.groupby(['researchContributorId'])['filingDate'].max().reset_index()
        
        
        
        previousdoc=previous[(previous['filingDate'].isin(maxprevious['filingDate'])&(previous['researchContributorId']==contributor))]

        temp=previousdoc[~((previousdoc['dataItemId'].isin(current['dataItemId']))&(previousdoc['parentFlag'].isin(current['parentFlag']))
                         &(previousdoc['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(previousdoc['tradingItemId'].isin(current['tradingItemId'])))]
        

        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        AS=[]
        tid=[]
        diff=[]
        perc=[]        
        for ind, row in temp.iterrows():
            if execute_operator(row['daysdiff'],float(Threshold[0]),operator[0]):
                dataItemIds.append(row['dataItemId'])
                previousdate.append(row['filingDate'])
                parentflag.append(row['parentFlag'])
                AS.append(row['accountingStandardDesc'])
                tid.append(row['tradingItemId'])
                diff='NA'
                perc='NA'  
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"filingDate":previousdate,"diff":diff,"perc":perc})
        
        if len(diff_df)>0:
       

            temp1 = historicalData_parsed[((historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['dataItemId'].isin(diff_df['dataItemId'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['tradingItemId'].isin(tid))
                                           &(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))] [['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']]
            

            temp1_revised=temp1.dropna()         
            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "Tag not in current document (Comparable)"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": 'NA', "scale": 'NA', "value": 'NA', "currency": 'NA'},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": 'NA',"units": 'NA',"currency": 'NA',"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA',"refFilingId":row["versionId"],"refFilingDate":row["filingDate"], "diff": 'NA', "percent": 'NA'}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors
#Estimates Error Checks 
@add_method(Validation) 
def EST_61(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
        errors = []
        left_list=get_dataItemIds_list2('LHSdataItemIds', parameters)
        max_threshold=get_parameter_value(parameters,'Max Threshold') 
        min_threshold=get_parameter_value(parameters,'Min_Threshold') #100%
        operator = get_dataItemIds_list('Operation', parameters)     
        try:
            
            filingdate=filingMetadata['metadata']['filingDate']
            contributor=filingMetadata['metadata']['researchContributorId']
            
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value_scaled','currencyId','parentFlag','tradingItemId']]

            previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','value','value_scaled','currency','tradingItemId','filingDate','currencyId','parentFlag']]
            
            maxprevious1=previous.groupby(['dataItemId'])['filingDate'].max().reset_index()
            
            maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
            merged_df=pd.merge(lhs_df,maxprevious,on=['dataItemId','parentFlag','tradingItemId'],how='inner')
            
            print(lhs_df)
            print(maxprevious)
            
            
            
            merged_df['variation']=((merged_df[['value_scaled_x', 'value_scaled_y']].max(axis=1) - merged_df[['value_scaled_x', 'value_scaled_y']].min(axis=1)) / merged_df[['value_scaled_x', 'value_scaled_y']].min(axis=1)) * 100
            
            print(merged_df)
            
            dataItemIds=[]
            parentflag=[]
            # accounting=[]
            tid=[]
            # fyc=[]        
            diff=[]
            perc=[]
            for ind,row in merged_df.iterrows():
                if ((execute_operator(row['variation'], float(min_threshold[0]), operator[0]))|(execute_operator(row['variation'], float(max_threshold[0]), operator[1]))):
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag']) 
                    # accounting.append(row['accountingStandardDesc']) 
                    # fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'

            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc})
            
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId'])) &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','tradingItemName']]

            temp1_revised=temp1.dropna()
                                                                                                                                                                                                                                          
            for ind, row in temp1_revised.iterrows():
                    
                result = {"highlights": [], "error": " captured price target is not in given range"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": 'NA', "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": 'NA', "percent": 'NA'}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": 'NA', "percent": 'NA'}]
                errors.append(result)
         
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors   
#Estimates Error Checks 
@add_method(Validation) 
def EST_13B(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    
    try:

        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','currency','tradingItemId']]
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','value','currency','tradingItemId','filingDate']]
        maxprevious1=previous.groupby(['dataItemId'])['filingDate'].max().reset_index()
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        merged_df=pd.merge(current,maxprevious,on=['dataItemId'],how='inner')
        
        print(merged_df)

        dataItemIds=[]
        previousdate=[]
        tid_x=[]
        tid_y=[]
        diff=[]
        perc=[]        
        for ind, row in merged_df.iterrows():
            if execute_operator(row['tradingItemId_x'],row['tradingItemId_y'],operator[0]):
                tid_x.append(row['tradingItemId_x']) 
                tid_y.append(row['tradingItemId_y'])
                diff='NA'
                perc='NA'
                dataItemIds.append(row['dataItemId']) 
                previousdate.append(row['filingDate'])
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,'filingDate':previousdate,"diff":diff,"perc":perc})
        if len(diff_df)>0:

            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['tradingItemId'].isin(tid_x))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName']]
            
            temp1_revised=temp1.dropna()
           
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(historicalData_parsed['tradingItemId'].isin(tid_y))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['researchContributorId']==contributor))] [['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']]

            temp2_revised=temp2.dropna() 
            
    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Target price Trading item variation between current and previous document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": 'NA', "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": 'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [], "error": "Target price Trading item variation between current and previous document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": 'NA', "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo":'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA',"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
           
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_15G(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)        
    operator = get_dataItemIds_list('Operation', parameters) #[!=]
    
    try:
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']
        
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())
                                          &(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
        
        
        # print(current)
        # print(previous)

        maxprevious=previous.groupby(['dataItemId','peo','scale','value','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

        # print(current)
        # print(previous)
        # print(maxprevious)

        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','value'],how='inner')
        
        # print(merged_df)
        # print(merged_df[['dataItemId','peo','currency_x','currency_y']])

        filingdate=[]
        dataItemIds=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        diff=[]
        perc=[]

        #if merged_df is not None:
        for ind,row in merged_df.iterrows():
            if (execute_operator(row['currency_x'],row['currency_y'],operator[0]) | execute_operator(row['scale_x'],row['scale_y'],operator[0])):
                filingdate.append(row['filingDate'])
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])  
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
                                       
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,"filingDate":filingdate})

        if len(diff_df)>0:       
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName']] 
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds))&(historicalData_parsed['companyId']==companyid) & (historicalData_parsed['peo'].isin(peos)) 
                                           & (historicalData_parsed['filingDate'].isin(filingdate))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId']]
           
            temp1_revised=temp1.dropna()  
            temp2_revised=temp2.dropna()  
    
            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "Units length difference is more than 3 digits compare to previous documents"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
           
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [], "error": "Units length difference is more than 3 digits compare to previous documents"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors  
#Estimates Error Checks 
@add_method(Validation) 
def EST_33(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Any Dataitemid
    operator = get_dataItemIds_list('Operation', parameters) #[">="]

    try:
        FQ = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed["value"]!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"]==2))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #Quarters data
        # print(FQ)
        HY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed["value"]!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"]==10))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #semi anual data
        # print(HY)
        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed["value"]!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"]==1))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #Annual data
        # print(FY)
    
        
        if (((len(FY)>0) & (len(FQ)>0))):
            base_currency=FQ.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
        # merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        elif ((len(HY)>0) & (len(FQ)>0)):    
            base_currency=FQ.currency.mode()[0]
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            HY["value_scaled"] = HY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)            
        
        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')    
        merged_df1=pd.merge(FQ,HY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        
        # print(merged_df)
        # print(merged_df1)
            
        peos=[]
        diff=[]
        perc=[]
        dataItemIds=[]
        FQx=[]
        FQy=[]

        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if  execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(float(round(difference)))
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    FQx.append(row['fiscalQuarter_x'])
                    FQy.append(row['fiscalQuarter_y'])
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))    
        if merged_df1 is not None:
            for ind,row in merged_df1.iterrows():
                if  execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(float(round(difference)))
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    FQx.append(row['fiscalQuarter_x'])
                    FQy.append(row['fiscalQuarter_y'])
                    perc.append(float(round(difference/(row[['value_scaled_x','value_scaled_y']].min()))*100))  
                    
        diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,'fiscalQuarter_x':FQx,'fiscalQuarter_y':FQy})
            
        # print(diff_df)

        if len(diff_df)>0:
            temp0 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & extractedData_parsed['fiscalYear'].isin(peos)
                                            &(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull())&((extractedData_parsed['fiscalQuarter'].isin(FQx))|(extractedData_parsed['fiscalQuarter'].isin(FQy))))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','tradingItemName']]
            
            temp0_revised=temp0.dropna()

            for ind, row in temp0_revised.iterrows():
                result = {"highlights": [], "error": "Any of the quarter value is  equals or more than  to related fiscal year value"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['description'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":'NA', "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                           
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation) 
def EST_35(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # tags
    operator = get_dataItemIds_list('Operation', parameters)
    try:
        documentdate=filingMetadata['metadata']['filingDate']

        
        temp = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(dataItemId_list)))&(extractedData_parsed['value'] != "") &(extractedData_parsed['value'].notnull())&(extractedData_parsed['peo']!= "")&(extractedData_parsed['peo'].notnull()))][['dataItemId', 'peo', 'fiscalChainSeriesId']].drop_duplicates()
        
        temp['companyId'] = filingMetadata['metadata']['companyId']
        
        print(temp)

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['value'] != "")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['companyId'].isin(temp['companyId']))&(historicalData_parsed['peo']!="")&(historicalData_parsed['filingDate'] < documentdate)&(historicalData_parsed['peo'].notnull()))][['dataItemId', 'peo','fiscalChainSeriesId', 'filingDate','companyId']].drop_duplicates()
        
        print(previous)
        newfyc=temp[~((temp['fiscalChainSeriesId']).isin(previous['fiscalChainSeriesId']))]
        
        # print(newfyc)
        
        maxprevious = previous.groupby(['companyId','fiscalChainSeriesId'])['filingDate'].max().reset_index()
        
        # print(maxprevious)

        previous = previous[previous['filingDate'].isin(maxprevious['filingDate'])]


        merged_df = pd.merge(temp, previous, on=['companyId'], how='inner')
        
        print(merged_df)

        filingdate = []
        diff = []
        perc = []
        series1 = []
        series2 = []
        
        for ind, row in merged_df.iterrows():
            if execute_operator(row['fiscalChainSeriesId_x'], row['fiscalChainSeriesId_y'], operator[0]):
                filingdate.append(row['filingDate'])
                difference = 'NA'
                series1.append(row['fiscalChainSeriesId_x'])
                series2.append(row['fiscalChainSeriesId_y'])
                diff.append(difference)
                perc = 'NA'
                

        diff_df = pd.DataFrame({"diff": diff, "perc": perc, "filingDate": filingdate, "curseries": series1, "preseries": series2}).drop_duplicates()


        temp1 = extractedData_parsed[(extractedData_parsed['fiscalChainSeriesId'].isin(series1))][['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team']].drop_duplicates()

        temp2 = historicalData_parsed[((historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['fiscalChainSeriesId'].isin(series2)) )][['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team', 'versionId', 'feedFileId','filingDate', 'companyId']].drop_duplicates()

        if len(newfyc)>0:
            temp3 = extractedData_parsed[(extractedData_parsed['fiscalChainSeriesId'].isin(newfyc['fiscalChainSeriesId']))][['fiscalChainSeriesId', 'accountingStandardDesc', 'parentFlag', 'team']].drop_duplicates()
            
            # print(temp3)
            
            temp3_revised=temp3.dropna()
            
            for ind, row in temp3_revised.iterrows():
                result = {"highlights": [],"error": "New fiscal Year series collected for this company"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},"cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA","versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [{"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA","peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA","accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)

        if len(temp1) > 0 and len(temp2) > 0:
            temp1_revised = temp1.dropna()
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],"error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},"cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA","versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [{"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA","peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA","accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)

            temp2_revised = temp2.dropna()
            
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": "NA"},"cell": {"peo": "NA", "scale": "NA", "value": "NA", "currency": "NA"},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": "NA", "description": "NA","versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'],"feedFileId": filingMetadata['metadata']['feedFileId'], "peo": "NA"}
                result["checkGeneratedForList"] = [{"tag": "NA", "description": "NA", "tradingItemId": "NA", "fiscalYear": "NA", "fiscalQuarter": "NA","peo": "NA", "value": "NA", "units": "NA", "currency": "NA", "tradingItemName": "NA","accountingStdDesc": row["accountingStandardDesc"], "parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": "NA", "percent": "NA"}]
                errors.append(result)


        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors    

#Estimates Error Checks 
@add_method(Validation) 
def EST_59A(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max Threshold') 
    try:

        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']
        
        # print(filingdate)

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','value_scaled','peo','currency','tradingItemId']].drop_duplicates()
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','value','value_scaled','peo','currency','tradingItemId','filingDate']].drop_duplicates()

        current['peo']=current['peo'].fillna('NP: 1234')
        previous['peo']=previous['peo'].fillna('NP: 1234')        

        maxprevious1=previous.groupby(['dataItemId'])['filingDate'].max().reset_index()
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        maxprevious = maxprevious.copy()
        # maxprevious['hours'] = abs(((pd.to_datetime('filingdate')) - (pd.to_datetime(maxprevious['filingDate']))).dt.total_seconds() / 3600)
        maxprevious.loc[:,'hours'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(maxprevious['filingDate'])).dt.total_seconds() / 3600)

        # maxprevious['days']=abs((pd.to_datetime(filingdate)-pd.to_datetime(maxprevious['filingDate'])).dt.days)
        # maxprevious.loc[:,'hours'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(maxprevious['filingDate'])).dt.total_seconds() / 3600)
        
        # print(current)
        # print(maxprevious)

        

        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo'],how='inner')
        
        # print(merged_df)


        dataItemIds=[]
        previousdate=[]
        tid_x=[]
        tid_y=[]
        diff=[]
        perc=[]
        peos=[]        
        for ind, row in merged_df.iterrows():
            if ((execute_operator(row['hours'],float(Threshold[0]),operator[0]))&(execute_operator(row['value_x'],row['value_y'],operator[1]))):  #&(execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[1]))
                tid_x.append(row['tradingItemId_x']) 
                tid_y.append(row['tradingItemId_y'])
                diff='NA'
                perc='NA'
                dataItemIds.append(row['dataItemId']) 
                previousdate.append(row['filingDate'])
                peos.append(row['peo'])
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,'filingDate':previousdate,"diff":diff,"perc":perc})
        
        # print(diff_df)
        

        if len(diff_df)>0:

            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid_x))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName']].drop_duplicates()
            
            temp1_revised=temp1.dropna()
           
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(historicalData_parsed['peo'].isin(diff_df['peo']))&(historicalData_parsed['tradingItemId'].isin(tid_y))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['researchContributorId']==contributor))] [['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()

            temp2_revised=temp2.dropna() 
            
    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Two revisions on the same day"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": row['peo'],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [], "error": "Two revisions on the same day"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo":'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA',"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
           
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors  
    
#Estimates Error Checks 
@add_method(Validation) 
def EST_59(historicalData,filingMetadata,extractedData,parameters):
    #JSON 19
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max Threshold') 
    try:

        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']
        
        # print(filingdate)

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','value','value_scaled','peo','currency','tradingItemId','parentFlag']].drop_duplicates()
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','value','value_scaled','peo','currency','tradingItemId','parentFlag','filingDate']].drop_duplicates()

        current['peo']=current['peo'].fillna('NP: 1234')
        previous['peo']=previous['peo'].fillna('NP: 1234')        

        maxprevious1=previous.groupby(['dataItemId'])['filingDate'].max().reset_index()
        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        maxprevious = maxprevious.copy()
        # maxprevious['hours'] = abs(((pd.to_datetime('filingdate')) - (pd.to_datetime(maxprevious['filingDate']))).dt.total_seconds() / 3600)
        maxprevious.loc[:,'hours'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(maxprevious['filingDate'])).dt.total_seconds() / 3600)

        # maxprevious['days']=abs((pd.to_datetime(filingdate)-pd.to_datetime(maxprevious['filingDate'])).dt.days)
        # maxprevious.loc[:,'hours'] = abs((pd.to_datetime(filingdate) - pd.to_datetime(maxprevious['filingDate'])).dt.total_seconds() / 3600)
        
        # print(current)
        # print(maxprevious)

        

        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','tradingItemId','parentFlag'],how='inner')
        
        # print(merged_df)


        dataItemIds=[]
        previousdate=[]
        tid=[]
        # tid_x=[]
        # tid_y=[]
        diff=[]
        perc=[]
        peos=[]        
        for ind, row in merged_df.iterrows():
            if ((execute_operator(row['hours'],float(Threshold[0]),operator[0]))&(execute_operator(row['value_x'],row['value_y'],operator[1]))):  #&(execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[1]))
                tid.append(row['tradingItemId']) 
                # tid_y.append(row['tradingItemId_y'])
                diff='NA'
                perc='NA'
                dataItemIds.append(row['dataItemId']) 
                previousdate.append(row['filingDate'])
                peos.append(row['peo'])
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,'filingDate':previousdate,"diff":diff,"perc":perc})
        
        # print(diff_df)
        

        if len(diff_df)>0:

            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['peo'].isin(diff_df['peo']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName']].drop_duplicates()
            
            temp1_revised=temp1.dropna()
           
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(historicalData_parsed['peo'].isin(diff_df['peo']))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate']))&(historicalData_parsed['researchContributorId']==contributor))] [['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','scale','currency','team','description','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()

            temp2_revised=temp2.dropna() 
            
    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "Multiple Target price value collected for the same Trading item from the same contributor on the same date"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row['peo'], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo": row['peo'],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [], "error": "Multiple Target price value collected for the same Trading item from the same contributor on the same date"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": 'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": 'NA', "fiscalQuarter":'NA', "peo":'NA',"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": 'NA',"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":'NA', "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
           
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors  

#Estimates Error Checks 
@add_method(Validation) 
def EST_67A(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #[Tags]
    operator = get_dataItemIds_list('Operation', parameters) #["<"]    
    try:
        latestactual = pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo']).date()
        filingdate=filingMetadata['metadata']['filingDate']
        contributor=filingMetadata['metadata']['researchContributorId']
        companyid=filingMetadata['metadata']['companyId']        

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear']].drop_duplicates()

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate','periodEndDate']].drop_duplicates()


        maxprevious1=previous.groupby(['dataItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()

        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        maxprevious['periodEndDate']=pd.to_datetime(maxprevious['periodEndDate']).dt.date
        
        current['didpeocomb']=current.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        maxprevious['didpeocomb']=maxprevious.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        
      
        temp_df=maxprevious[~((maxprevious['didpeocomb'].isin(current['didpeocomb']))&(maxprevious['parentFlag'].isin(current['parentFlag']))
                        &(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]
        

        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        peo=[]
        AS=[]
        fyc=[]
        diff=[]
        perc=[]      

        if temp_df is not None:    
            for ind, row in temp_df.iterrows():
                if execute_operator(latestactual, row['periodEndDate'], operator[0]):
                    peo.append(row['peo'])
                    dataItemIds.append(row['dataItemId'])
                    previousdate.append(row['filingDate'])
                    parentflag.append(row['parentFlag'])
                    AS.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'  
                    
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"filingDate":previousdate,"diff":diff,"perc":perc})

        if len(diff_df)>0:
            diff_df['peocomb']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
            historicalData_parsed['peocomb']=historicalData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
       
            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])))] [['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId','filingDate']].drop_duplicates()
    
    
            temp1_new=temp1.dropna() 
    
        
            for ind, row in temp1_new.iterrows():
    
                result = {"highlights": [], "error": "Same Sign/Missing Tag"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"refFilingId":row["versionId"],"refFilingDate":row["filingDate"],"estimatePeriodId":row["estimatePeriodId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors    

#Estimates Error Checks 
@add_method(Validation) 
def EST_6B(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag1_list=get_dataItemIds_list('TAG1', parameters) # NI GAAP
    tag2_list=get_dataItemIds_list('TAG2', parameters) # NI Normalized
    tag3_list=get_dataItemIds_list('TAG3', parameters) # EPS GAAP
    tag4_list=get_dataItemIds_list('TAG4', parameters) # EPS Normalized
    tag5_list=get_dataItemIds_list('LHSdataItemIds', parameters) # EBT GAAP
    tag6_list=get_dataItemIds_list('RHSdataItemIds', parameters) # EBT Normalized
    operator = get_dataItemIds_list('Operation', parameters) #["=="],["!="] 

    try:

        filingdate = filingMetadata['metadata']['filingDate']
        contributor = filingMetadata['metadata']['researchContributorId']

        current_ni_gaap = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag1_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value','value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        current_ni_normalized = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag2_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        current_eps_gaap = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag3_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value','value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        current_eps_normalized = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag4_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        current_ebt_gaap = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag5_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value','value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]

        current_ebt_normalized = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag6_list)) & (extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId']]


        previous_ni_gaap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag1_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_ni_gaap_group = previous_ni_gaap.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_ni_gaap = previous_ni_gaap[(previous_ni_gaap['filingDate'].isin(max_previous_ni_gaap_group['filingDate']))]
        


        previous_ni_normalized = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_ni_normalized_group = previous_ni_normalized.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_ni_normalized = previous_ni_normalized[(previous_ni_normalized['filingDate'].isin(max_previous_ni_normalized_group['filingDate']))]



        previous_eps_gaap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_eps_gaap_group = previous_eps_gaap.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_eps_gaap = previous_eps_gaap[(previous_eps_gaap['filingDate'].isin(max_previous_eps_gaap_group['filingDate']))]
        

        previous_eps_normalized = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag4_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_eps_normalized_group = previous_eps_normalized.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_eps_normalized = previous_eps_normalized[(previous_eps_normalized['filingDate'].isin(max_previous_eps_normalized_group['filingDate']))]

    

        previous_ebt_gaap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag5_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_ebt_gaap_group = previous_ebt_gaap.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_ebt_gaap = previous_ebt_gaap[(previous_ebt_gaap['filingDate'].isin(max_previous_ebt_gaap_group['filingDate']))]
        

        previous_ebt_normalized = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(tag6_list)) & (historicalData_parsed['researchContributorId'] == contributor) & (
                                                      historicalData_parsed['filingDate'] < filingdate) & (
                                                      historicalData_parsed['value'] != "") & (historicalData_parsed['value'].notnull()))][['dataItemId', 'peo','value', 'value_scaled', 'currency', 'parentFlag', 'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId', 'filingDate']]
                                 
        max_previous_ebt_normalized_group = previous_ebt_normalized.groupby(
            ['dataItemId', 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId', 'fiscalChainSeriesId'])[
            'filingDate'].max().reset_index()

        max_previous_ebt_normalized = previous_ebt_normalized[(previous_ebt_normalized['filingDate'].isin(max_previous_ebt_normalized_group['filingDate']))]
     

        
        merged_df_ni_current = pd.merge(current_ni_gaap, current_ni_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')

        merged_df_ni_previous = pd.merge(max_previous_ni_gaap, max_previous_ni_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')
        
        merged_df_ni=pd.merge(merged_df_ni_current,merged_df_ni_previous,on=['dataItemId_x','dataItemId_y','peo','parentFlag', 
                                                                       'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId'],how='inner')
       

        merged_df_eps_current = pd.merge(current_eps_gaap, current_eps_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')

        merged_df_eps_previous = pd.merge(max_previous_eps_gaap, max_previous_eps_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')
        
        merged_df_eps=pd.merge(merged_df_eps_current,merged_df_eps_previous,on=['dataItemId_x','dataItemId_y','peo','parentFlag', 
                                                                       'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId'],how='inner')
        
        merged_df_ebt_current = pd.merge(current_ebt_gaap, current_ebt_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')

        merged_df_ebt_previous = pd.merge(max_previous_ebt_gaap, max_previous_ebt_normalized,
                             on=[ 'peo', 'parentFlag', 'accountingStandardDesc', 'tradingItemId',
                                 'fiscalChainSeriesId'], how='inner')
        
        merged_df_ebt=pd.merge(merged_df_ebt_current,merged_df_ebt_previous,on=['dataItemId_x','dataItemId_y','peo','parentFlag', 
                                                                       'accountingStandardDesc', 'tradingItemId','fiscalChainSeriesId'],how='inner')        

        dataItemId1 = []
        dataItemId2 = []
        previousdate = []
        parentflag = []
        tid = []
        accounting = []
        fyc = []
        peos = []
        diff = []
        perc = []
        for ind, row in merged_df_ni.iterrows():
            if ((execute_operator(row['value_scaled_x_x'], row['value_scaled_y_x'], operator[0])) 
                & (execute_operator(row['value_scaled_x_x'], row['value_scaled_y_y'], operator[1]))
                &(execute_operator(row['value_scaled_y_x'], row['value_scaled_x_y'], operator[1]))):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
                dataItemId1.append(row['dataItemId_x'])
                dataItemId2.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_y'])


        for ind, row in merged_df_eps.iterrows():
            if ((execute_operator(row['value_scaled_x_x'], row['value_scaled_y_x'], operator[0])) 
                & (execute_operator(row['value_scaled_x_x'], row['value_scaled_y_y'], operator[1]))
                &(execute_operator(row['value_scaled_y_x'], row['value_scaled_x_y'], operator[1]))):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
                dataItemId1.append(row['dataItemId_x'])
                dataItemId2.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_y'])
                

        
        for ind, row in merged_df_ebt.iterrows():
            if ((execute_operator(row['value_scaled_x_x'], row['value_scaled_y_x'], operator[0])) 
                & (execute_operator(row['value_scaled_x_x'], row['value_scaled_y_y'], operator[1]))
                &(execute_operator(row['value_scaled_y_x'], row['value_scaled_x_y'], operator[1]))):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc'])
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
                dataItemId1.append(row['dataItemId_x'])
                dataItemId2.append(row['dataItemId_y'])
                previousdate.append(row['filingDate_y'])
                
        diff_df = pd.DataFrame({"filingDate_y": previousdate,"dataItemId_x": dataItemId1,"dataItemId_y": dataItemId2, "peo": peos, "diff": diff, "perc": perc}) 

        if len(diff_df) > 0:
            diff_df['peocomb1'] = diff_df.apply(lambda x: '%s%s' % (x['dataItemId_x'], x['peo']), axis=1)
            diff_df['peocomb2'] = diff_df.apply(lambda x: '%s%s' % (x['dataItemId_y'], x['peo']), axis=1)
            extractedData_parsed['peocomb'] = extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)
            historicalData_parsed['peocomb'] = historicalData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'], x['peo']), axis=1)

            temp1 = extractedData_parsed[(((extractedData_parsed['peocomb'].isin(diff_df['peocomb1']))|
                                          (extractedData_parsed['peocomb'].isin(diff_df['peocomb2'])))& (
                extractedData_parsed['parentFlag'].isin(parentflag)) & (
                                              extractedData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                              extractedData_parsed['tradingItemId'].isin(tid)) & (
                                              extractedData_parsed['fiscalChainSeriesId'].isin(fyc)) & (
                                                      extractedData_parsed['value'] != "") & (
                                              extractedData_parsed['value'].notnull()))][
                ['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc',
                 'tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear',
                 'fiscalQuarter', 'tradingItemName']]
            temp1=temp1.drop_duplicates()                                      
            temp1_revised = temp1.dropna()

            temp2 = historicalData_parsed[(((historicalData_parsed['peocomb'].isin(diff_df['peocomb1']))|
                                           (historicalData_parsed['peocomb'].isin(diff_df['peocomb2'])))& (
                historicalData_parsed['parentFlag'].isin(parentflag)) & (
                                               historicalData_parsed['accountingStandardDesc'].isin(accounting)) & (
                                               historicalData_parsed['tradingItemId'].isin(tid)) & (
                                               historicalData_parsed['fiscalChainSeriesId'].isin(fyc)) & (
                                                       historicalData_parsed['value'] != "") & (
                                               historicalData_parsed['value'].notnull()) & (
                                               historicalData_parsed['filingDate'].isin(previousdate)) & (
                                                       historicalData_parsed['researchContributorId'] == contributor))][
                ['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc',
                 'tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear',
                 'fiscalQuarter', 'tradingItemName',  'versionId', 'companyId', 'feedFileId', 'filingDate']]
                                                           
            temp2=temp2.drop_duplicates()                                               
            temp2_revised = temp2.dropna()

            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [],
                          "error": "Current Vs Previous doc where collected flavor is NORM=GAAP and GAAP=NORM"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"],
                                               "versionId": filingMetadata['metadata']['versionId'],
                                               "companyid": filingMetadata['metadata']['companyId'],
                                               "feedFileId": filingMetadata['metadata']['feedFileId'],
                                               "peo": row["peo"],
                                               "diff": 'NA',
                                               "percent": 'NA'}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],
                     "value": row["value"], "units": row["scale"], "currency": row["currency"],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "diff": 'NA',
                     "percent": 'NA'}]
                errors.append(result)
            for ind, row in temp2_revised.iterrows():
                result = {"highlights": [],
                          "error": "Current Vs Previous doc where collected flavor is NORM=GAAP and GAAP=NORM"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]},
                                             "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'],
                                                      "currency": row['currency']}, "section": row['team'],
                                             "filingId": row['versionId']})
                result["checkGeneratedFor"] = {"statement": "", "tag": row['dataItemId'],
                                               "description": row["description"], "versionId": row['versionId'],
                                               "companyid": row['companyId'], "feedFileId": row['feedFileId'],
                                               "peo": row["peo"],
                                               "diff": 'NA',
                                               "percent": 'NA'}
                result["checkGeneratedForList"] = [
                    {"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],
                     "fiscalYear": row["fiscalYear"], "fiscalQuarter": row["fiscalQuarter"], "peo": row["peo"],
                     "value": row["value"], "units": row["scale"], "currency": row["currency"],
                     "tradingItemName": row["tradingItemName"], "accountingStdDesc": row["accountingStandardDesc"],
                     "parentConsolidatedFlag": row["parentFlag"], "fiscalChainSeries": row["fiscalChainSeriesId"],
                     "refFilingId": row["versionId"], "refFilingDate": row["filingDate"],
                     "estimatePeriodId": row["estimatePeriodId"],
                     "diff":'NA',
                     "percent": 'NA'}]
                errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


def runValidations(extractedData, executionData):
    
    global extractedData_parsed, historicalData_parsed, isDataParsed,currencyConversion_parsed
    import json

    extractedData = json.loads(extractedData)
    executionData = json.loads(executionData)

    # Calling json parser functions
    try:
        isDataParsed = False
        historicalData_parsed = parse_historical_data(extractedData['historicalData'])
        extractedData_parsed = parse_extracted_data(extractedData['extractedData'], convert_to_df = True)
        #currencyConversion_parsed = parse_conversion_data(extractedData['currency_conversion'],True)
        isDataParsed = True
    except:
        isDataParsed = False

    return Validation().validate(extractedData, executionData)

if __name__=="__main__":
    from sys import argv
    import os
    import json
    
    executionDataFile = r'C:\Users\gsravane\Downloads\ExecutionDataTesting.json'
    # executionDataFile = r'C:\Users\gsravane\Downloads\ExecutionDataTesting.json'
    
    extractedDataFile = r'C:\Users\gsravane\Downloads\ErrorChecksData (22).json'
    #extractedDataFile = r'C:\Users\gsravane\Downloads\EstimatesErrorChecks.json'

    errors={}

    if os.path.exists(executionDataFile) and os.path.exists(extractedDataFile):
        with open(executionDataFile) as fp:
            #executionData = json.loads(fp.read().encode().decode("utf-8"))
            executionData = fp.read().encode().decode("utf-8")
        with open(extractedDataFile) as fp:
            # extractedData = json.loads(fp.read().encode().decode("utf-8"))
            extractedData = fp.read().encode().decode("utf-8")

            #v = Validation()
            #errors = v.validate(extractedData, executionData)
            errors = runValidations(extractedData, executionData)
            # print(str(errors))



    with open(os.path.join(executionDataFile+".validation"),'w+') as fp:
        import json
        result = json.dumps(errors, indent=4)
        fp.write(result)