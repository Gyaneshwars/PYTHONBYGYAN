# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 19:12:24 2023

@author: gsravane
"""

import hashlib
import time
import functools
import hashlib
import time
import functools
import pandas as pd
import numpy as np
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

def get_scaled_value(value, scale):
    """
    Function to scale value based on the scales_dict
    """
    scales_dict = {'actual': 1, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000, 'trillion': 1000000000000, 'tenth': 0.1, \
                    'hundredth': 0.01, 'thousandth': 0.001, 'tenthousandth': 0.0001, 'dozen': 12, 'hundred': 100, 'lakh': 100000, \
                    'crore': 10000000, 'bit': 12.5, 'score': 20, 'half': 0.5, 'pair': 2, 'gross': 144, 'ten': 10, 'myraid': 10000, \
                    'millionth': 0.000001, 'billionth': 0.000000001, 'percendataItemIde': 100, 'fiveHundred': 500, 'hundred Million': 100000000}
    try:
        if str(scale).lower() in scales_dict.keys():
            return float(value) * scales_dict[str(scale).lower()]
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
            output['value_scaled'] = output.apply(lambda row: get_scaled_value(row['value'], row['scale']), axis=1)
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
    metadata_column_names = ["versionId","companyId","companyName","industry","industryId","country","latestActualizedPeo","latestPeriodType","latestPeriodYear","fiscalYearEnd","filingDate","language","heading","versionFormat","documentId","sourceId","companyrank","priorityid","tier","primaryEpsFlag","feedFileId"]
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
        output['value_scaled'] = output.apply(lambda row: get_scaled_value(row['value'], row['scale']), axis=1)
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
    # Loop through tags, parent instance and childrows in-case cildwors are present.
    for tag in conversionData:
        for parent_id, parent_inst in conversionData[tag].items():
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
    
    


# #Estimates Error Checks
# @add_method(Validation)
# def  Tags_collected_in_wrong_units(historicalData,filingMetadata,extractedData,parameters):
# # move to testing
#     # Finalized
#     errors = []
#     dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
#     scale_list=get_dataItemIds_list('scalelist', parameters)
#     try:

#         temp = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&~(extractedData_parsed['scale'].isin(scale_list)))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

#         dataItemIds=[]        
#         peos=[]
#         tid=[]
#         parentflag=[]
#         accounting=[]
#         fyc=[]        
#         diff=[]
#         perc=[]         
#         for ind, row in temp.iterrows():

#                 dataItemIds.append(row['dataItemId'])
#                 peos.append(row['peo'])
#                 tid.append(row['tradingItemId']) 
#                 parentflag.append(row['parentFlag']) 
#                 accounting.append(row['accountingStandardDesc']) 
#                 fyc.append(row['fiscalChainSeriesId']) 
#                 diff='NA'
#                 perc='NA'            
#         diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})    

#         temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
#                                                       &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

#         for ind, row in temp1.iterrows():  
                                           
#             result = {"highlights": [], "error": "dataItemIds collected in wrong units"}
#             result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#             result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
#             result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
#             errors.append(result)  
            
#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors



# @add_method(Validation)
# def decimal_value_dataItemId(historicalData,filingMetadata,extractedData,parameters):
#     errors = []
#     dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)

#     try:
#         temp = extractedData_parsed[ extractedData_parsed['dataItemId'].isin(dataItemId_list)][['dataItemId','peo','value','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','periodTypeId','fiscalYear','fiscalChainSeriesId']]

#         dataItemIds=[]
#         peos=[]
#         tid=[]
#         parentflag=[]
#         accounting=[]
#         fyc=[]
#         diff=[]
#         perc=[]

#         for ind, row in temp.iterrows():
#             if ((row['value'] !='')& (row['scale']=='actual')):
#                 if(pd.to_numeric(row['value'])%1!=0):
#                     dataItemIds.append(row['dataItemId'])
#                     peos.append(row['peo'])
#                     tid.append(row['tradingItemId']) 
#                     parentflag.append(row['parentFlag']) 
#                     accounting.append(row['accountingStandardDesc']) 
#                     fyc.append(row['fiscalChainSeriesId'])                     
#                     diff='NA'
#                     perc='NA'

#         diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
#         temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
#                                               &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

#         for ind, row in temp2.iterrows():
   
        

#             result = {"highlights": [], "error": "dataItemIds with Decimal Values"}
#             result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#             result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
#             result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
#             errors.append(result)

#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors


# @add_method(Validation)
# def noCurrencySelected(historicalData,filingMetadata,extractedData,parameters):
#     errors = []
#     dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
#     try:
#         temp = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','periodTypeId','fiscalYear','fiscalChainSeriesId']]

#         dataItemIds=[]
#         peos=[]
#         tid=[]
#         parentflag=[]
#         accounting=[]
#         fyc=[]        
#         diff=[]
#         perc=[]

#         for ind, row in temp.iterrows():

#             if ((row['value'] !='') & (row['currency']=='NA')):
#                 dataItemIds.append(row['dataItemId'])
#                 peos.append(row['peo'])
#                 tid.append(row['tradingItemId']) 
#                 parentflag.append(row['parentFlag']) 
#                 accounting.append(row['accountingStandardDesc']) 
#                 fyc.append(row['fiscalChainSeriesId']) 
#                 diff='NA'
#                 perc='NA'

#         diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
#         temp2=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
#                                                       &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

#         for ind, row in temp2.iterrows():
#             result = {"highlights": [], "error": "currency not entered for the  data item"}
#             result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#             result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
#             result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
#             errors.append(result)
#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors


# ##Estimates Error Check    
# @add_method(Validation)
# def  Semi_annual_reporting_cycle_for_US_and_Canada_companies(historicalData,filingMetadata,extractedData,parameters):
# # Move to testing
#     # Finalized
#     errors = []

#     countryCode=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
#     try:
#         if filingMetadata['metadata']['country']  in countryCode:

#             companyid=filingMetadata['metadata']['companyId']

#             temp0 = extractedData_parsed[ ((extractedData_parsed['periodTypeId']==10)&(extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId']]

#             history = historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['periodTypeId']==10))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId']]
            
#             final = temp0[~(temp0['periodTypeId'].isin(history['periodTypeId']))]
            
#             dataItemIds=[]
#             peos=[]
#             tid=[]
#             parentflag=[]
#             accounting=[]
#             fyc=[]
#             diff=[]
#             perc=[]            
#             for ind, row in final.iterrows():
            
#                 dataItemIds.append(row['dataItemId'])
#                 peos.append(row['peo'])
#                 tid.append(row['tradingItemId']) 
#                 parentflag.append(row['parentFlag']) 
#                 accounting.append(row['accountingStandardDesc']) 
#                 fyc.append(row['fiscalChainSeriesId']) 
#                 diff='NA'
#                 perc='NA'            
#             diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})  
        
#             temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
#                                                           &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
#             for ind, row in temp1.iterrows():
                               
#                 result = {"highlights": [], "error": "Semi-annual for US and Canada companies which are not having semi reporting cycle"}
#                 result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#                 result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
#                 result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
#                 errors.append(result)  
            
#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors 


# #Estimates Error Checks 
# @add_method(Validation)
# def Variation_in_fiscal_Year_series(historicalData,filingMetadata,extractedData,parameters):
#     # Move to testing
#     # Finalized
#     errors = []

#     operator = get_dataItemIds_list('Operation', parameters)
    
#     try:
#         temp = extractedData_parsed[(((extractedData_parsed['Data_item_flag']=='G')|(extractedData_parsed['Data_item_flag']=='A'))&(extractedData_parsed['value']!=""))][['fiscalChainSeriesId']]
#         temp['filing_date']=pd.to_datetime(filingMetadata['metadata']['filing_date'])
#         temp['companyId']=filingMetadata['metadata']['companyId']
        
#         previous = historicalData_parsed[(historicalData_parsed['companyId'].isin(temp['companyId'])
#                                   &((historicalData_parsed['Data_item_flag']=='G')|(historicalData_parsed['Data_item_flag']=='A'))&(historicalData_parsed['value']!=""))][['fiscalChainSeriesId','filing_date','companyId']]
        
#         maxprevious=previous.groupby(['companyId'])['filing_date'].max().reset_index()

#         previous=previous[previous['filing_date'].isin(maxprevious['filing_date'])]

#         merged_df=pd.merge(temp,previous,on=['companyId'],how='inner')

#         filingdate=[]
#         diff=[]
#         perc=[]
#         series1=[]
#         series2=[]
#         for ind,row in merged_df.iterrows():

#             if execute_operator(row['fiscal_chain_series_id_x'],row['fiscal_chain_series_id_y'],operator[0]):
#                 filingdate.append(row['filing_date_y'])             
#                 difference='NA'
#                 series1.append(row['fiscal_chain_series_id_x'])
#                 series2.append(row['fiscal_chain_series_id_y'])
#                 diff.append(difference)
#                 perc='NA'

#         diff_df=pd.DataFrame({"diff":diff,"perc":perc,"filing_date":filingdate,"curseries":series1,"preseries":series2})
       
#         temp1 = extractedData_parsed[extractedData_parsed['fiscalChainSeriesId'].isin(series1)]
#         temp2 = historicalData_parsed[(historicalData_parsed['filing_date'].isin(diff_df['filing_date'])&historicalData_parsed['fiscalChainSeriesId'].isin(series2))]
        
#         if len(temp1)>0 and len(temp2)>0:

#             for ind, row in temp1.iterrows():       
#                 result = {"highlights": [], "error": "Variation in fiscal Year series compared to the previous document"}
#                 result["highlights"].append({"row": {"fiscal_chain_series_id": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#                 result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
#                 result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
#                 errors.append(result)
           
#             for ind, row in temp2.iterrows():
#                 result = {"highlights": [], "error": "Variation in fiscal Year series compared to the previous document"}
#                 result["highlights"].append({"row": {"fiscal_chain_series_id": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#                 result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"]}
#                 result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
#                 errors.append(result)

#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors 


# Estimates Error Checks 
@add_method(Validation)
def EBIT_Minus_DA_Equalto_EBITDA(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT, DA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    
    try:
        companyid=filingMetadata['metadata']['companyId']

        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, DA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
        
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df['value_scaled'] = lhs_df.apply(lambda x: x['value_scaled']*(-1) if x['dataItemId']== left_dataItemIds_list[1] else x['value_scaled'], axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['value_scaled'].sum().reset_index() #EBIT-DA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['value_scaled'].sum().reset_index()


        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
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
                diff.append(difference)
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
                
        
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "EBIT-DA=EBITDA"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "EBIT-DA=EBITDA"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
   
# Estimates Error Checks
@add_method(Validation)
def NAVPS_actual_greaterthan_BVPS_actual(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #NAVPS
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #BVPS
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NAVPS
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #NAVPS
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
                diff.append(difference)
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
   
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
       
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "261_NAVPS actual greater than BVPS actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)            

        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "261_NAVPS actual greater than BVPS actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors



# Estimates Error Checks
@add_method(Validation)
def EBITDA_LESSTHAN_EBIT_and_EBITDA_consensus_Gtreaterthan_EBIT_Consensus_or_viceversa(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    operator = get_dataItemIds_list('Operation', parameters) #["<" & ">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        
        if len(lhs_df)>0:
            lhs_df['consValue_scaled'] = lhs_df.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)        
        
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        if len(rhs_df)>0:
            rhs_df['consValue_scaled'] = rhs_df.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
   

        if (len(lhs_df)>0 and len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["consValue_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["consValue_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        
 
        print(lhs_df)
        print(rhs_df)
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        print(merged_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                if execute_operator(row['consValue_scaled_x'],row['consValue_scaled_y'],operator[1]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            elif execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[1]):
                if execute_operator(row['consValue_scaled_x'],row['consValue_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)            

        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
      
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
   
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "EBITDA<EBIT and EBITDA consensus > EBIT Consensus or viceversa"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "EBITDA<EBIT and EBITDA consensus > EBIT Consensus or viceversa"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors




# Estimates Error Checks
@add_method(Validation)
def EPSNormalized_Actual_greaterthan_CashEPS_Actual(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EPSNormalized
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #CashEPS
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPSNormalized
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPSNormalized
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
              
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EPSNormalized            
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()  #CashEPS
        
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
  
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
   
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "430_EPS Normalized Actual is greater than Cash EPS for Recent Quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "430_EPS Normalized Actual is greater than Cash EPS for Recent Quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result) 
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def CAPEX_EQUALTO_FCF(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
             
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #CAPEX
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()  #FCF 

        
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
   
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
   
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Capex=FCF"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Capex=FCF"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBITDA_is_greaterthan_Revenue_for_Actual(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Revenue
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Revenue
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()  #Revenue
 
        
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
   
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "168_EBITDA is greater than Revenue for Actual (Company Screen)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "168_EBITDA is greater than Revenue for Actual (Company Screen)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Revenue_actual_and_EBIT_actual_is_same(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #REVENUE
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #REVENUE
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
              
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #REVENUE
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()  #EBIT

        
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "198_Revenue actual and EBIT actual is same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "198_Revenue actual and EBIT actual is same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Revenue_Actual_is_Zero_and_EBIT_in_Positive(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #REVENUE
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    #operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #REVENUE
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
            
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        
        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #REVENUE        
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()  #EBIT  
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]

        for ind,row in merged_df.iterrows():
                if row['value_scaled_x']==0:
                    if row['value_scaled_y']>0:
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                           
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "171 _Revenue Actual is Zero and EBIT in Positive for latest quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "171 _Revenue Actual is Zero and EBIT in Positive for latest quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBITDA_EQUALTO_EBIT_WHEN_DA_AVAILABLE(historicalData,filingMetadata,extractedData,parameters):
 
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        DA_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        
        if DA_df["dataItemId"].nunique()!=len(tag_list):
            DA_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        DA_df_peo_count=DA_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=DA_df_peo_count[(DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=DA_df[DA_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            DA_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            DA_df=DA_df.append(DA_df_missing_data,ignore_index=True)            


        if (len(lhs_df)>0 & len(rhs_df)>0 & len(DA_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            DA_df["value_scaled"] = DA_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        DA_df=DA_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_DA_df = pd.merge(DA_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_DA_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "497_ EBITDA Actual & EBIT Actual same and Deprication available"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "497_ EBITDA Actual & EBIT Actual same and Deprication available"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Depre_and_Amort_collected_when_EBITDA_nd_EBIT_same(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        DA_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
        
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if DA_df["dataItemId"].nunique()!=len(tag_list):
            DA_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        DA_df_peo_count=DA_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=DA_df_peo_count[(DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=DA_df[DA_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            DA_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            DA_df=DA_df.append(DA_df_missing_data,ignore_index=True)            

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(DA_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            DA_df["value_scaled"] = DA_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        DA_df=DA_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

                
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_DA_df = pd.merge(DA_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_DA_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
   
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Depreciation and Amortization collected when EBITDA and EBIT are the same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Depreciation and Amortization collected when EBITDA and EBIT are the same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBITDA_less_EBIT_result_5times_greaterthan_DA(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA, EBIT
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #DA*5
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    factor=get_parameter_value(parameters,'MultiplicationFactor') 
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, EBITDA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
        
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df['value_scaled'] = lhs_df.apply(lambda x: x['value_scaled']*(-1) if x['dataItemId']== left_dataItemIds_list[1] else x['value_scaled'], axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
        
        print(lhs_df)
        print(rhs_df)
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_df['multipled_value']=merged_df['value_scaled_y']*factor[0]
        print(merged_df)
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():
            if (row['value_scaled_x']!=0) & (row['value_scaled_y']!=0):
                if execute_operator(row['value_scaled_x'],row['multipled_value'],operator[0]):
                    peos.append(row['peo'])               
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "511_EBITDA actual less EBIT actual result 5 times greater than DA actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "511_EBITDA actual less EBIT actual result 5 times greater than DA actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def CFO_PLUS_CAPEX_EQUALTO_FCF(historicalData,filingMetadata,extractedData,parameters):
 
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO, CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        
        #print(left_dataItemIds_list)
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX
       
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')     
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
                 
 
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        lhs_df['value']=lhs_df['value'].replace('',np.NaN)
        rhs_df['value']=rhs_df['value'].replace('',np.NaN)
        

        if (lhs_df['value'].notna().all()) and (rhs_df['value'].notna().all()):
        #if (lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].nunique()==2).all()):
        #lhs_df_dt_count=lhs_df.groupby(['peo'])['dataItemId'].nunique().reset_index(name='dtcount') 
        #print(lhs_df_dt_count['dtcount'])
        #if (lhs_df_dt_count['dtcount'].any())==2:
            if (len(lhs_df)>0 & len(rhs_df)>0):   
                base_currency=lhs_df.currency.mode()[0]
                lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
     
        # if lhs_df.groupby(['peo']).count()['dataItemId'].min() >= 2:
        #     summed_df = lhs_df.groupby(['peo']).sum()
        #     print(summed_df)
 
        # grouped = lhs_df.groupby(['peo','dataItemId'])
        # #print(grouped)
        # for group, lhs_df_grouped in grouped:
        #     if len(lhs_df_grouped)>1 and not lhs_df_grouped['value_scaled'].isna().all():
        #if lhs_df.groupby('peo').size().ge(2).any():
        # mask=lhs_df.groupby(['peo'])['dataItemId'].count()>= 2
        # filtered_df = lhs_df[lhs_df['peo'].isin(mask[mask].index)]
        #filtered_df = lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].nunique()==2)
        
        #filtered_peos = filtered_df[filtered_df].index.tolist() # get the peos that satisfy the condition
        #print(filtered_df)
        #result_df = lhs_df[lhs_df['peo'].isin(filtered_peos)] # filter the original DataFrame
        #filtered_df = lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].count()>= 2)
        #if (lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].nunique()==2).all()):            
        #lhs_df = lhs_df.groupby(['dataItemId','fiscalYear','periodTypeId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']).agg(dt_Count=('dataItemId','count'),PEO_Sum=('value_scaled','sum')).reset_index()
        #if lhs_df_dt_count['dtcount'].any()==2:
        if (lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].nunique()==3).all()):
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #CFO+Capex
        #print(lhs_df)
    
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()       
    

        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        print(merged_df)

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        #if (lhs_df['value_scaled'].notna().all()) and (rhs_df['value_scaled'].notna().all()):
        #if (lhs_df.groupby('peo').apply(lambda group: group['dataItemId'].nunique()==2).all()): 
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
     
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "CFO+Capex=FCF"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "CFO+Capex=FCF"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors




# Estimates Error Checks
@add_method(Validation)
def EBTGAAP_PLUS_DA_EQUALTO_EBITDA(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBT_GAAP, DA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT_GAAP, DA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT_GAAP, DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
     
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "216_EBT GAAP Actual plus DA Actual equals to EBITDA and EBITDA & EBT GAAP are not same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "216_EBT GAAP Actual plus DA Actual equals to EBITDA and EBIT & EBT GAAP are not same"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Sumof_EBIT_and_Interest_Expense_equalto_EBTGAAP(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT, INTEREST_EXP
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT_GAAP
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, INTEREST_EXP
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, INTEREST_EXP
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Sum of EBIT and Interest Expense is equal to EBT GAAP"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Sum of EBIT and Interest Expense is equal to EBT GAAP"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks 
@add_method(Validation)
def EBIT_act_less_DA_act_equalsto_EBITDA_Act(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT, DA
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, DA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df['value_scaled'] = lhs_df.apply(lambda x: x['value_scaled']*(-1) if x['dataItemId']== left_dataItemIds_list[1] else x['value_scaled'], axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "330_EBIT actual less DA actual equals to EBITDA Actual for latest quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "330_EBIT actual less DA actual equals to EBITDA Actual for latest quarter"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def EBIT_is_greaterthan_EBITDA(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        docuement_date=pd.to_datetime(filingMetadata['metadata']['filingDate'])        
        result_date=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo'])

        if result_date and docuement_date is not None:
            if float(((docuement_date)-(result_date)).days)<8.0:
    
                lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
                rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
                    lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
                extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
                lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
                    
                if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                    missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                    collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                    lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                    lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)        
              
                if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
                    rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
                rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
                    
                if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                    missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                    collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                    rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                    rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
                if (len(lhs_df)>0 & len(rhs_df)>0):
                    base_currency=lhs_df.currency.mode()[0]
                    lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                    rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
                rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
                
                
                merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
    
                
                peos=[]
                diff=[]
                perc=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]
                    
                if merged_df is not None:
    	            for ind,row in merged_df.iterrows():
    	            	if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
    		                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
    		                    peos.append(row['peo'])               
    		                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
    		                    tid.append(row['tradingItemId'])
    		                    parentflag.append(row['parentFlag'])
    		                    accounting.append(row['accountingStandardDesc']) 
    		                    fyc.append(row['fiscalChainSeriesId'])
    		                    diff.append(difference)
    		                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
       
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                        &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                        |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                        &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
                for ind, row in temp1.iterrows():
                    result = {"highlights": [], "error": "82_EBIT is greater than EBITDA for Recent Quarter Actual or Recent Annual Actual for last 7 days results announcement"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                for ind, row in temp2.iterrows():
                    result = {"highlights": [], "error": "82_EBIT is greater than EBITDA for Recent Quarter Actual or Recent Annual Actual for last 7 days results announcement"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors
    
    # errors = []
    # left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT
    # right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    # operator = get_dataItemIds_list('Operation', parameters) #[">"]
    # try:
    #     companyid=filingMetadata['metadata']['companyId']
    #     docuement_date=pd.to_datetime(filingMetadata['metadata']['filingDate'])       
    #     result_date=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo'])
    #     #print(result_date)
    #     if result_date is not None:

    #         if (docuement_date-result_date).days<8:
    
    #             lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
    #             rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
    #             if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
    #                 lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
    #             extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
    #             lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
                    
    #             if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
    #                 missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
    #                 collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
    #                 lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
    #                 lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)        
              
    #             if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
    #                 rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
    #             extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
    #             rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
                    
    #             if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
    #                 missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
    #                 collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
    #                 rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
    #                 rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
    #             if (len(lhs_df)>0 & len(rhs_df)>0):
    #                 base_currency=lhs_df.currency.mode()[0]
    #                 lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    #                 rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    #             lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
    #             rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
                
                
    #             merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
    
    #             print(merged_df)
    #             peos=[]
    #             diff=[]
    #             perc=[]
    #             tid=[]
    #             parentflag=[]
    #             accounting=[]
    #             fyc=[]
                
    #             for ind,row in merged_df.iterrows():
    #                 if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
    #                     peos.append(row['peo'])               
    #                     difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
    #                     tid.append(row['tradingItemId'])
    #                     parentflag.append(row['parentFlag'])
    #                     accounting.append(row['accountingStandardDesc']) 
    #                     fyc.append(row['fiscalChainSeriesId'])
    #                     diff.append(difference)
    #                     perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
    #             diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
       
    #             temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #                     &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
    #                     |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #                     &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
    #             temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
    #                     &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
    #                     |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
    #                     &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
    #             for ind, row in temp1.iterrows():
    #                 result = {"highlights": [], "error": "82_EBIT is greater than EBITDA for Recent Quarter Actual or Recent Annual Actual for last 7 days results announcement"}
    #                 result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
    #                 result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
    #                 result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
    #                 errors.append(result)
    #             for ind, row in temp2.iterrows():
    #                 result = {"highlights": [], "error": "82_EBIT is greater than EBITDA for Recent Quarter Actual or Recent Annual Actual for last 7 days results announcement"}
    #                 result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
    #                 result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
    #                 result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
    #                 errors.append(result)
    #             print(errors)
    #             return errors
    # except Exception as e:
    #     print(e)
    #     return errors

# Estimates Error Checks
@add_method(Validation)
def EBIT_Guid_High_Greaterthan_EBITDA_guid_High(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT_Guidance_High
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA_Guidance_High
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT_Guidance_High
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)        
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        #print(diff_df)      
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "516_EBIT Guidance  High greater than EBITDA Guidance High"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "516_EBIT Guidance  High greater than EBITDA Guidance High"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBITDA_High_Guidance_Lessthan_EBIT_High_guidance(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA_Guidance_High
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT_Guidance_High
    operator = get_dataItemIds_list('Operation', parameters) #["<"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA_Guidance_High
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA_Guidance_High
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)        
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
   
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "248_EBITDA High guidance less than EBIT High Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "248_EBITDA High guidance less than EBIT High Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Capex_Guid_High_equalsto_Maint_Capex_Guid_High(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CAPEX_Guidance_High
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Maintenance_Capex_Guidance_High
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX_Guidance_High
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX_Guidance_High
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)        
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
  
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "514_Capex Guidance High equals to Maintenance Capex Guidance High"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "514_Capex Guidance High equals to Maintenance Capex Guidance High"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def FCF_is_equalto_sum_of_Capex_M_capex_and_CFO(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CFO, CAPEX, M.CAPEX
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #FCF
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX, M.CAPEX
        print(lhs_df)
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CFO, CAPEX, M.CAPEX
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        lhs_df['value']=lhs_df['value'].replace('',np.NaN)
        rhs_df['value']=rhs_df['value'].replace('',np.NaN)
        

        if (lhs_df['value'].notna().all()) and (rhs_df['value'].notna().all()):
            if (len(lhs_df)>0 & len(rhs_df)>0):
                base_currency=lhs_df.currency.mode()[0]
                lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA-EBIT
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
            
        # lhs_df['value']=lhs_df['value'].replace('',np.NaN)
        # rhs_df['value']=rhs_df['value'].replace('',np.NaN)

        if len(left_dataItemIds_list)==3:
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        print(len(left_dataItemIds_list)==3)
        print(left_dataItemIds_list[0])
        #print(lhs_df[0])
        print(merged_df)

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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Free Cash Flow is equal to sum of Capex, M.capex and Cash from Operations"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Free Cash Flow is equal to sum of Capex, M.capex and Cash from Operations"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def Actual_not_inline_with_consensus(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #['<]
    variation=get_parameter_value(parameters,'Threshold') #100%
    
    try:
        #companyid=filingMetadata['metadata']['companyId']
        
        temp = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")][['dataItemId','peo','value','value_scaled','currency','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        #consensus_df= extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['peo'].isin(actual_df['peo']))][["dataItemId","peo","cons_value",'consScale']]
        print(temp[['dataItemId','value_scaled','consValue']])
    
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

        base_currency=temp.currency.mode()[0]
        temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
        #temp['multipled_value']=temp['consValue_scaled']*factor[0]
        temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100
     
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]

        for ind,row in temp.iterrows():
            if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(difference)                
                perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Actual not in line with consensus.threshold limit of 100% for all the data items"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors     



# Estimates Error Checks
@add_method(Validation)
def Guidance_not_inline_with_consensus(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #['>]
    variation=get_parameter_value(parameters,'Min_Threshold') #100%
    
    try:

        yesscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']!=-1))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        noscale = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="")&(extractedData_parsed['scaleId']==-1))][['dataItemId','peo','scaleId','value_scaled','currency','consValue','consScaleId','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        

        if len(yesscale)>0:
            yesscale['consValue_scaled'] = yesscale.apply(lambda row: get_scaled_value(row['consValue'], row['consScaleId']), axis=1)

            base_currency=yesscale.currency.mode()[0]
            yesscale["consValue_scaled"] = yesscale.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            

        if len(noscale)>0: 
            noscale['consValue_scaled']=float(noscale['consValue'])*1
        
        
        if (len(yesscale)>0 or len(noscale)>0):

            temp=pd.concat([yesscale,noscale]) 
            
            temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100

    
            dataItemIds=[]
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
    
            if temp is not None:
     	        for ind,row in temp.iterrows():

                    if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])  
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId']) 
              
                        difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                        diff.append(difference)
                        
                        perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds})

            if len(diff_df)>0:
                diff_df['compressed']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                extractedData_parsed['compressed']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                

                temp1 = extractedData_parsed[((extractedData_parsed['compressed'].isin(diff_df['compressed'])) & (extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
            
                for ind, row in temp1.iterrows():
        
                    result = {"highlights": [], "error": "Guidance not in line with consensus"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"Units": row["scale"],"Currency": row["currency"],"Trading Item": row["tradingItemName"],"Accounting Standard": row["accountingStandardDesc"],"Parent/ consolidated flag": row["parentFlag"],"Fiscal Chain series": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                              
                    errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors   
    

    # errors = []
    # dataItemid_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    # operator = get_dataItemIds_list('Operation', parameters) #['>']
    # variation=get_parameter_value(parameters,'Min_Threshold') #20%
    # try:
    #     temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemid_list))&((extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!="")))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
    #         #consensus_df= extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['peo'].isin(actual_df['peo']))][["dataItemId","peo","cons_value",'consScale']]
    
    #     if len(temp)>0:
    #         temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

    #     base_currency=temp.currency.mode()[0]
    #     temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
    #     #temp['multipled_value']=temp['consValue_scaled']*factor[0]
    #     temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100

    #     peos=[]
    #     dataItemIds=[]
    #     diff=[]
    #     perc=[]
    #     tid=[]
    #     parentflag=[]
    #     accounting=[]
    #     fyc=[]
        
    #     print(temp)
        
    #     for ind,row in temp.iterrows():
    #         if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
    #             dataItemIds.append(row['dataItemId'])
    #             peos.append(row['peo'])               
    #             difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
    #             tid.append(row['tradingItemId'])
    #             parentflag.append(row['parentFlag'])
    #             accounting.append(row['accountingStandardDesc']) 
    #             fyc.append(row['fiscalChainSeriesId'])
    #             diff.append(difference)                
    #             perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
        
    #     diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

    #     temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #                                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]


    #     for ind, row in temp1.iterrows():
    #         result = {"highlights": [], "error": "Guidance not in line with consensus which is greater than 20%"}
    #         result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
    #         result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
    #         result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
    #         errors.append(result)
    #     print(errors)
    #     return errors
    # except Exception as e:
    #     print(e)
    #     return errors     

    
    # errors = []
    # #tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    # operator = get_dataItemIds_list('Operation', parameters) #['>']
    # variation=get_parameter_value(parameters,'Threshold') #20%
    # try:
    #     temp = extractedData_parsed[(extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!="")][['dataItemId','peo','value','value_scaled','currency','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
    #         #consensus_df= extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['peo'].isin(actual_df['peo']))][["dataItemId","peo","cons_value",'consScale']]
    
    #     if len(temp)>0:
    #         temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

    #     base_currency=temp.currency.mode()[0]
    #     temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
    #     #temp['multipled_value']=temp['consValue_scaled']*factor[0]
    #     temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100

    #     peos=[]
    #     dataItemIds=[]
    #     diff=[]
    #     perc=[]
    #     tid=[]
    #     parentflag=[]
    #     accounting=[]
    #     fyc=[]
        
    #     #print(temp)
        
    #     for ind,row in temp.iterrows():
    #         if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
    #             dataItemIds.append(row['dataItemId'])
    #             peos.append(row['peo'])               
    #             difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
    #             tid.append(row['tradingItemId'])
    #             parentflag.append(row['parentFlag'])
    #             accounting.append(row['accountingStandardDesc']) 
    #             fyc.append(row['fiscalChainSeriesId'])
    #             diff.append(difference)                
    #             perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
        
    #     diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

    #     temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #             &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
    #             |((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #             &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]


    #     for ind, row in temp1.iterrows():
    #         result = {"highlights": [], "error": "Guidance not in line with consensus which is greater than 20%"}
    #         result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
    #         result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
    #         result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
    #         errors.append(result)
    #     print(errors)
    #     return errors
    # except Exception as e:
    #     print(e)
    #     return errors     



# Estimates Error Checks
@add_method(Validation)
def EBITDA_EBIT_same_where_DA_Act_greaterthan_Zero(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBITDA 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA 
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        DA_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if DA_df["dataItemId"].nunique()!=len(tag_list):
            DA_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        DA_df_peo_count=DA_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=DA_df_peo_count[(DA_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=DA_df[DA_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            DA_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            DA_df=DA_df.append(DA_df_missing_data,ignore_index=True)            

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(DA_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            DA_df["value_scaled"] = DA_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        DA_df=DA_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_DA_df = pd.merge(DA_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')


        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_DA_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "154_EBITDA FY Actual, EBIT FY Actual same where DA Actual greater than Zero"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "154_EBITDA FY Actual, EBIT FY Actual same where DA Actual greater than Zero"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors



# Estimates Error Checks
@add_method(Validation)
def NI_EBT_Same_but_Taxrate_collected(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #NI 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT
    tag_list=get_dataItemIds_list('TAG1', parameters) #Taxrate
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&((extractedData_parsed['periodTypeId']==2)|(extractedData_parsed['periodTypeId']==10))&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NI 
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&((extractedData_parsed['periodTypeId']==2)|(extractedData_parsed['periodTypeId']==10))&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT
        ETR_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&((extractedData_parsed['periodTypeId']==2)|(extractedData_parsed['periodTypeId']==10))&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Taxrate
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if ETR_df["dataItemId"].nunique()!=len(tag_list):
            ETR_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        ETR_df_peo_count=ETR_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=ETR_df_peo_count[(ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=ETR_df[ETR_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            ETR_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&((historicalData_parsed['periodTypeId']==2)|(historicalData_parsed['periodTypeId']==10))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            ETR_df=ETR_df.append(ETR_df_missing_data,ignore_index=True)            

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(ETR_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            ETR_df["value_scaled"] = ETR_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        ETR_df=ETR_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_ETR_df = pd.merge(ETR_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_ETR_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "NI&EBT Same but Tax rate collected"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "NI&EBT Same but Tax rate collected"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def NIA_and_PBTA_same_but_taxrte_capt_for_ltst_Annual(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #NI 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT
    tag_list=get_dataItemIds_list('TAG1', parameters) #Taxrate
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NI 
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT
        ETR_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Taxrate
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
        
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if ETR_df["dataItemId"].nunique()!=len(tag_list):
            ETR_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        ETR_df_peo_count=ETR_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=ETR_df_peo_count[(ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=ETR_df[ETR_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            ETR_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            ETR_df=ETR_df.append(ETR_df_missing_data,ignore_index=True)            

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(ETR_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            ETR_df["value_scaled"] = ETR_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        ETR_df=ETR_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_ETR_df = pd.merge(ETR_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]  
        
        for ind,row in merged_ETR_df.iterrows():
            if (row['value_scaled']!=0):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
   
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "438_NI Actual and PBT Actual are same, but tax rate is captured for latest Annual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "438_NI Actual and PBT Actual are same, but tax rate is captured for latest Annual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Efftve_Tax_Rate_collected_EBTN_nd_EBTG_in_Negative(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBT_GAAP 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT_Norm
    tag_list=get_dataItemIds_list('TAG1', parameters) #Taxrate
    #operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NI 
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT
        ETR_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Taxrate
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if ETR_df["dataItemId"].nunique()!=len(tag_list):
            ETR_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        ETR_df_peo_count=ETR_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=ETR_df_peo_count[(ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=ETR_df[ETR_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            ETR_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            ETR_df=ETR_df.append(ETR_df_missing_data,ignore_index=True)            

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(ETR_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            ETR_df["value_scaled"] = ETR_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        ETR_df=ETR_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA

        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_ETR_df = pd.merge(ETR_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]          
        
        for ind,row in merged_ETR_df.iterrows():
            if (row['value_scaled']!=0):
                if row['value_scaled_x']<0:
                    if row['value_scaled_y']<0:
                #if execute_operator(row['value_scaled_x']<0,row['value_scaled_y']<0,operator[0]):
                        peos.append(row['peo'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
  
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "427_Effective Tax Rate Actual collected and EBT Normalized and EBT GAAP in Negative for last 30 days announced results."}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "427_Effective Tax Rate Actual collected and EBT Normalized and EBT GAAP in Negative for last 30 days announced results."}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def AGVC_Guidance_Records_Lessthan_900(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #['<]
    variation=get_parameter_value(parameters,'Threshold') #900%
    try:
        temp = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag_list))&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
        #consensus_df= extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['peo'].isin(actual_df['peo']))][["dataItemId","peo","cons_value",'consScale']]

        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

        base_currency=temp.currency.mode()[0]
        temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
        #temp['multipled_value']=temp['consValue_scaled']*factor[0]
        temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[] 
        
        for ind,row in temp.iterrows():
            if execute_operator(row['consensusvariation'],float(variation[0]),operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(difference)                
                perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "AGVC_Guidance Records< 900%"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors     

#Not added to Rule association
# Estimates Error Checks
@add_method(Validation)
def Sum_of_three_dataitems_not_equalto_Fourth_dataitem(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #dt1, dt2, dt3
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #dt4
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2, dt3
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2, dt3
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 + dt2 + dt3
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4

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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Sum of three dataitems not equal to Fourth dataitem"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Sum of three dataitems not equal to Fourth dataitem"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Not added to Rule association
# Estimates Error Checks
@add_method(Validation)
def Difference_of_two_dataItemId_not_equalto_third_tag(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #dt1, dt2
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #dt3
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt3
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df['value_scaled'] = lhs_df.apply(lambda x: x['value_scaled']*(-1) if x['dataItemId']== left_dataItemIds_list[1] else x['value_scaled'], axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 + dt2 + dt3
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4


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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Difference of two tags not equal to the third tag"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Difference of two tags not equal to the third tag"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Not added to Rule association    
# Estimates Error Checks
@add_method(Validation)
def Tags_has_greater_value_than_Related_Tag(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Tag
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Related tag
    operator = get_dataItemIds_list('Operation', parameters) #[">="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Tag
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Tag
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 + dt2 + dt3
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4

        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],as_index=False)['value_scaled'].apply(list).reset_index() #dt1 , dt2 , dt3
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],as_index=False)['value_scaled'].apply(list).reset_index() #dt4
        
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
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
  
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Tag(s) has greater value (Related Tag)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Tag(s) has greater value (Related Tag)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Not added to Rule association    
# Estimates Error Checks
@add_method(Validation)
def Tags_value_Greaterthan_Related_DataItem_value(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Tags
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Related tags
    operator = get_dataItemIds_list('Operation', parameters) #[">="]
    try:
        companyid=filingMetadata['metadata']['companyId']
        datacomb=list(zip(left_dataItemIds_list,right_dataItemIds_list))
        comparable=pd.DataFrame(datacomb,columns=['dataitem1','dataitem2'])
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Tag
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            lhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Tag
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)      
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        # if (len(lhs_df)>0 & len(rhs_df)>0):
        #     base_currency=lhs_df.currency.mode()[0]
        #     lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
        #     rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        # # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 , dt2 , dt3
        # # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4

        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],as_index=False)['value_scaled'].apply(list).reset_index() #dt1 , dt2 , dt3
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],as_index=False)['value_scaled'].apply(list).reset_index() #dt4

        if len(lhs_df)>0 and len(rhs_df)>0:
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
            comparable['compressed']=comparable.apply(lambda x:'%s%s' % (x['dataitem1'],x['dataitem2']),axis=1)
            merged_df['compressed']=merged_df.apply(lambda x:'%s%s' % (x['dataItemId_x'],x['dataItemId_y']),axis=1)
            
            print(merged_df)

            dataItemIds_x=[]
            dataItemIds_y=[]
            peos=[]
            parentflag=[]
            accounting=[]
            tid=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            for ind,row in merged_df.iterrows():
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])               
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    dataItemIds_x.append(row['dataItemId_x'])
                    dataItemIds_y.append(row['dataItemId_y'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"peo":peos,"diff":diff,"perc":perc})
            
            if len(diff_df)>0:
                 diff_df['peocomb1']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId_x'],x['peo']),axis=1)
                 diff_df['peocomb2']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId_y'],x['peo']),axis=1)
                 extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                 historicalData_parsed['peocomb']=historicalData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                                
      
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                    |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "Tags have value Greater than the Related Data Item value"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "Tags have value Greater than the Related Data Item value"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def Revenue_actual_3_times_lessthan_EBIT_actual(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Revenue*3
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, EBITDA
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT, EBITDA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
      
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            # rhs_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&historicalData_parsed['peo'].isin(extractedData_parsed['peo'])][["dataItemId","peo","value_scaled"]]
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])   
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
                missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
                collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
                rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 + dt2 + dt3
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_df['multipled_value']=merged_df['value_scaled_y']*float(factor[0])
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]  
        
        for ind,row in merged_df.iterrows():
            if execute_operator(row['value_scaled_x'],row['multipled_value'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "277_Revenue actual 3 times less than EBIT actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "277_Revenue actual 3 times less than EBIT actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Check 
@add_method(Validation)
def FCF_guidance_greaterthan_CFO_guidance_for_FY1(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #FCF Guidance
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #CFO Guidance
    operator=get_dataItemIds_list('Operation', parameters)  #['>']
    
    try:

        currentyear=filingMetadata['metadata']['latestPeriodYear']
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['fiscalYear']==currentyear+1)&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','currency','fiscalYear','fiscalQuarter']]
        if len(lhs_df)==0:
            lhs_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(left_dataItemIds_list))&(historicalData_parsed['fiscalYear']==currentyear+1)&(historicalData_parsed['periodTypeId']==1) & (historicalData_parsed['peo'].isin(extractedData_parsed['currency','peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        
        rhs_df=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['fiscalYear']==currentyear+1)&(extractedData_parsed['periodTypeId']==1)][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
        if len(rhs_df)==0:
            rhs_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(right_dataItemIds_list))&(historicalData_parsed['fiscalYear']==currentyear+1)&(extractedData_parsed['periodTypeId']==1)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
  

        if lhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)&(historicalData_parsed['fiscalYear']==currentyear+1)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(lhs_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)            

        # base_currency=lhs_df.currency.mode()[0]
        # lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    
        if rhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)&(historicalData_parsed['fiscalYear']==currentyear+1)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(rhs_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
       
        # rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
 
        #print(lhs_df , rhs_df)
        
        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 + dt2 + dt3
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt4
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        #print(merged_df)
        #merged_df['multipled_value']=merged_df['value_scaled_y']*factor[0]
        
        
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
                diff.append(difference)                
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
    
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "373_FCF guidance greater than CFO guidance for FY+1"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "373_FCF guidance greater than CFO guidance for FY+1"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors



# Estimates Error Checks
@add_method(Validation)
def EBITDA_EBTN_same_value_nd_EBIT_EBTG_same_values(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    tag1_list=get_dataItemIds_list('TAG1', parameters) #EBITDA
    tag2_list=get_dataItemIds_list('TAG2', parameters) #EBTN
    tag3_list=get_dataItemIds_list('TAG3', parameters) #EBIT
    tag4_list=get_dataItemIds_list('TAG4', parameters) #EBTG
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    countryCode=get_dataItemIds_list('COUNTRY_EXCLUDE',parameters)
    try:
        if filingMetadata['metadata']['country'] not in countryCode:
            
            companyid=filingMetadata['metadata']['companyId']
        
            tag1_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag1_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA
            if len(tag1_df)==0:
                tag1_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag1_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            
            tag2_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag2_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBTN
            if len(tag2_df)==0:
                tag2_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                    
            if tag1_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                tag1_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag1_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag1_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                tag1_df=tag1_df.append(tag1_df_missing_data,ignore_index=True)            
        
            if tag2_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                tag2_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag2_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag2_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                tag2_df=tag2_df.append(tag2_df_missing_data,ignore_index=True)            
                
            if (len(tag1_df)>0 & len(tag2_df)>0):
                base_currency=tag1_df.currency.mode()[0]
                tag1_df["value_scaled"] = tag1_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
                tag2_df["value_scaled"] = tag2_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

            
            merged1_df=pd.merge(tag1_df,tag2_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

            
            tag3_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag3_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
            if len(tag3_df)==0:
                tag3_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
       
            tag4_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag4_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBTG
            if len(tag4_df)==0:
                tag4_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag4_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
    
            if tag3_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                tag3_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag3_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag3_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                tag3_df=tag3_df.append(tag3_df_missing_data,ignore_index=True)            
    
            
            if tag4_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                tag4_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag4_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag4_df['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
                tag4_df=tag4_df.append(tag4_df_missing_data,ignore_index=True)            
    

            if (len(tag3_df)>0 & len(tag2_df)>0):
                base_currency=tag3_df.currency.mode()[0]
                tag3_df["value_scaled"] = tag3_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
                tag4_df["value_scaled"] = tag4_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
   
            
            merged2_df=pd.merge(tag3_df,tag4_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
            
            
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[] 
            
            for ind,row in merged1_df.iterrows():
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    for ind,row in merged2_df.iterrows():
                        if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                            peos.append(row['peo'])
                            tid.append(row['tradingItemId'])
                            parentflag.append(row['parentFlag'])
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
            
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag1_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                    |((extractedData_parsed['dataItemId'].isin(tag2_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                    |((extractedData_parsed['dataItemId'].isin(tag3_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))    
                    |((extractedData_parsed['dataItemId'].isin(tag4_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

                                                                                                                                                                                                                      
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(tag1_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                    |((historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                    |((historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                    |((historicalData_parsed['dataItemId'].isin(tag4_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                    &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
                                                                                                                                                                                                                      
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "604_EBITDA & EBT Norm have  same value and EBIT & EBT GAAP collected same values- excluding JAPAN, Korea(South & North)"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
                errors.append(result)
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "604_EBITDA & EBT Norm have  same value and EBIT & EBT GAAP collected same values- excluding JAPAN, Korea(South & North)"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
                errors.append(result)
            print(errors)
            return errors
    except Exception as e:
        print(e)
        return errors
    
#Estimates Error Checks 
@add_method(Validation)
def New_flavor_captured_for_the_company(historicalData,filingMetadata,extractedData,parameters):

    errors = []

    try:
       
        current = extractedData_parsed[((extractedData_parsed['value']!="")&(~(extractedData_parsed['parentFlag'].isin(historicalData_parsed['parentFlag']))
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

        temp= extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) &(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp.iterrows():
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
    

    # errors = []
    # #tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    # try:
    #     temp = extractedData_parsed[~(extractedData_parsed['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'].isin(historicalData_parsed['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']))]

    #     parentflag=[]
    #     diff=[]
    #     perc=[]
    #     tid=[]
    #     accounting=[]
    #     fyc=[] 
        
    #     for ind,row in temp.iterrows():
           
    #         parentflag.append(row['parentFlag'])               
    #         difference='NA'
    #         diff.append(difference)
    #         perc='NA'
    #         tid.append(row['tradingItemId'])
    #         accounting.append(row['accountingStandardDesc']) 
    #         fyc.append(row['fiscalChainSeriesId'])
        
    #     diff_df=pd.DataFrame({"parentFlag":parentflag,"diff":diff,"perc":perc})        
          
    #     for ind, row in temp.iterrows():
    #         result = {"highlights": [], "error": "New flavor captured for the company"}
    #         result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
    #         result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
    #         result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
    #         errors.append(result)
    #     print(errors)
    #     return errors
    # except Exception as e:
    #     print(e)
    #     return errors    

# Estimates Error Check    
@add_method(Validation)
def spltfctr_btwn_crnt_feedfile_FD_nd_prevs_max_actPEO(historicalData,filingMetadata,extractedData,parameters):

   errors = []
   #operator = get_dataItemIds_list('Operation', parameters)
    
   try:
       temp = extractedData_parsed[(((extractedData_parsed['dataItemFlag']=='A') | (extractedData_parsed['dataItemFlag']=='G'))&(extractedData_parsed['value']!=""))][['dataItemId','peo','splitFactor']]
       temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
       temp['latestActualizedPeo']=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo'])    

       filtered_df = temp[(temp['splitFactor'] == temp['splitFactor']) & (temp['filingDate'] <= temp['latestActualizedPeo']) & (temp['latestActualizedPeo'] >= temp['filingDate'])]
       
       #print(filtered_df)
       
       peos=[]
       diff=[]
       perc=[]
       tid=[]
       parentflag=[]
       accounting=[]
       fyc=[] 
       for ind,row in temp.iterrows():
           if not filtered_df.empty:
               peos.append(row['peo'])
               difference='NA'   #if max_peo_filing["splitFactor"].between(current_filingdate['filingDate'], max_peo['latestActualizedPeo']):              
               diff.append(difference)
               perc='NA'
               tid.append(row['tradingItemId'])
               parentflag.append(row['parentFlag'])
               accounting.append(row['accountingStandardDesc']) 
               fyc.append(row['fiscalChainSeriesId'])

       diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

       temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(temp)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
       for ind, row in temp1.iterrows():
           result = {"highlights": [], "error": "If we have split factor between the current feed file filing date and previous max actual PEO then this check will generate"}
           result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
           result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
           result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
           errors.append(result)
       print(errors)
       return errors
   except Exception as e:
       print(e)
       return errors

#     errors = []
#     #tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
#     try:

#         temp=extractedData_parsed[extractedData_parsed['splitFactor','peo','periodEndDate','actualizedDate','tradingItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId']]
#         temp['latestActualizedPeo']=pd.to_datetime(temp['latestActualizedPeo']).dt.normalize()
#         temp['filingDate']=pd.to_datetime(temp['filingDate']).dt.normalize()
        
#         peos=[]
#         diff=[]
#         perc=[]
#         tid=[]
#         parentflag=[]
#         accounting=[]
#         fyc=[] 

#         if temp is not None:
# 	        for ind,row in temp.iterrows():
# 	            if temp.loc[temp["splitFactor"].between(temp['filingDate'], temp['actualizedDate'])]:
# 	                peos.append(row['peo'])               
# 	                difference='NA'
# 	                diff.append(difference)
# 	                perc='NA'
# 	                tid.append(row['tradingItemId'])
# 	                parentflag.append(row['parentFlag'])
# 	                accounting.append(row['accountingStandardDesc']) 
# 	                fyc.append(row['fiscalChainSeriesId'])

#         diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

#         temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(temp)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
#                 &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
#         for ind, row in temp1.iterrows():
#             result = {"highlights": [], "error": "If we have split factor between the current feed file filing date and previous max actual PEO then this check will generate"}
#             result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
#             result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
#             result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
#             errors.append(result)
#         print(errors)
#         return errors
#     except Exception as e:
#         print(e)
#         return errors

# Estimates Error Check 
@add_method(Validation)
def Reverse_direction_of_NI_Flavors_EPS_Flavors(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    ni_gaap=get_dataItemIds_list('TAG1', parameters) #ni_gaap_df
    ni_nor=get_dataItemIds_list('TAG2', parameters) #ni_nor_df
    eps_gaap=get_dataItemIds_list('TAG3', parameters) #eps_gaap_df
    eps_nor=get_dataItemIds_list('TAG4', parameters) #eps_nor_df
    operator=get_dataItemIds_list('Operation', parameters) #['>','<']
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        ni_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_gaap)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        ni_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_nor)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          
        eps_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_gaap)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        eps_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_nor)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          
        print(ni_gaap_df['value_scaled'],ni_nor_df['value_scaled'],eps_gaap_df['value_scaled'],eps_nor_df['value_scaled'])
        current_peo_count=extractedData_parsed['peo'].nunique()
        if len(ni_gaap_df['dataItemId'])==0:
            ni_gaap_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_gaap) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((ni_gaap_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            ni_gaap_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_gaap_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            ni_gaap_df=ni_gaap_df.append(ni_gaap_df_missing_data,ignore_index=True)
        if len(ni_nor_df['dataItemId'])==0:
            ni_nor_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_nor) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((ni_nor_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            ni_nor_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_nor_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            ni_nor_df=ni_nor_df.append(ni_nor_df_missing_data,ignore_index=True)
        if len(eps_gaap_df['dataItemId'])==0:
            eps_gaap_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_gaap) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((eps_gaap_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            eps_gaap_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_gaap_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            eps_gaap_df=eps_gaap_df.append(eps_gaap_df_missing_data,ignore_index=True)
        if len(eps_nor_df['dataItemId'])==0:
            eps_nor_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_nor) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((eps_nor_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            eps_nor_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_nor_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            eps_nor_df=eps_nor_df.append(eps_nor_df_missing_data,ignore_index=True)
        
        #print(ni_gaap_df['value_scaled'],ni_nor_df['value_scaled'],eps_gaap_df['value_scaled'],eps_nor_df['value_scaled'])

        
        ni_merged_df=pd.merge(ni_gaap_df,ni_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        eps_merged_df=pd.merge(eps_gaap_df,eps_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df=pd.merge(ni_merged_df,eps_merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')



        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():
            if (execute_operator(row['value_scaled_x_x'],row['value_scaled_y_x'],operator[0]) & execute_operator(row['value_scaled_x_y'],row['value_scaled_y_y'],operator[1])):
                peos.append(row['peo'])
                diff='NA'
                perc='NA'
                tid.append(row['tradingItemId'])
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
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

                                                                                                                                                                                                                  
        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(ni_gaap)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(ni_nor)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(eps_gaap)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(eps_nor)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Reverse direction of NI Flavors & EPS Flavors"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Reverse direction of NI Flavors & EPS Flavors"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)            
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Check 
@add_method(Validation)
def NIG_NIN_Increases_whereas_EPSG_EPSN_decreases_vicversa(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    ni_gaap=get_dataItemIds_list('TAG1', parameters) #ni_gaap_df
    ni_nor=get_dataItemIds_list('TAG2', parameters) #ni_nor_df
    eps_gaap=get_dataItemIds_list('TAG3', parameters) #eps_gaap_df
    eps_nor=get_dataItemIds_list('TAG4', parameters) #eps_nor_df
    operator=get_dataItemIds_list('Operation', parameters) #['>','<']
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        ni_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_gaap)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        ni_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_nor)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          
        eps_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_gaap)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        eps_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_nor)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          

        current_peo_count=extractedData_parsed['peo'].nunique()
        if len(ni_gaap_df['dataItemId'])==0:
            ni_gaap_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_gaap) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((ni_gaap_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            ni_gaap_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_gaap_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            ni_gaap_df=ni_gaap_df.append(ni_gaap_df_missing_data,ignore_index=True)
        if len(ni_nor_df['dataItemId'])==0:
            ni_nor_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_nor) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((ni_nor_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            ni_nor_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(ni_nor_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            ni_nor_df=ni_nor_df.append(ni_nor_df_missing_data,ignore_index=True)
        if len(eps_gaap_df['dataItemId'])==0:
            eps_gaap_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_gaap) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((eps_gaap_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            eps_gaap_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_gaap_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            eps_gaap_df=eps_gaap_df.append(eps_gaap_df_missing_data,ignore_index=True)
        if len(eps_nor_df['dataItemId'])==0:
            eps_nor_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_nor) 
                                               &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                               &(historicalData_parsed['periodTypeId'].isin(extractedData_parsed['periodTypeId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        elif ((eps_nor_df.groupby(['dataItemId'])['peo'].nunique())<current_peo_count).any():
            eps_nor_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(eps_nor_df['dataItemId'])&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
            eps_nor_df=eps_nor_df.append(eps_nor_df_missing_data,ignore_index=True)
       
        #print(ni_gaap_df['value_scaled'],ni_nor_df['value_scaled'],eps_gaap_df['value_scaled'],eps_nor_df['value_scaled'])

        ni_merged_df=pd.merge(ni_gaap_df,ni_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        eps_merged_df=pd.merge(eps_gaap_df,eps_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df=pd.merge(ni_merged_df,eps_merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():
            if (execute_operator(row['value_scaled_x_x'],row['value_scaled_y_x'],operator[0]) & execute_operator(row['value_scaled_x_y'],row['value_scaled_y_y'],operator[1])):
                peos.append(row['peo'])
                diff='NA'
                perc='NA'
                tid.append(row['tradingItemId'])
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
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]

                                                                                                                                                                                                                  
        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(ni_gaap)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(ni_nor)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(eps_gaap)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(eps_nor)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "900_NI GAAP & NI Normalized Increased whereas EPS GAAP & EPS Normalized decreases and vice versa"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "900_NI GAAP & NI Normalized Increased whereas EPS GAAP & EPS Normalized decreases and vice versa"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)            
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors  


#----Till here new rules creation has been completed

# Estimates Error Checks
@add_method(Validation)
def Act_clctd_fr_1_flvr_butnot_duplicated_fr_rltd_flvr(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    tag1_list=get_dataItemIds_list('TAG1', parameters) #NI_N
    tag2_list=get_dataItemIds_list('TAG2', parameters) #NI_G
    tag3_list=get_dataItemIds_list('TAG3', parameters) #EPS_N
    tag4_list=get_dataItemIds_list('TAG4', parameters) #EPS_G
    lhs_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBT_N
    rhs_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT_G
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
        companyid=filingMetadata['metadata']['companyId']
  
        tag1_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag1_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBITDA
        if len(tag1_df)==0:
            tag1_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag1_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
        
        tag2_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag2_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBTN
        if len(tag2_df)==0:
            tag2_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]

                
        if tag1_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            tag1_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag1_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag1_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            tag1_df=tag1_df.append(tag1_df_missing_data,ignore_index=True)            


        if tag2_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            tag2_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag2_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag2_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            tag2_df=tag2_df.append(tag2_df_missing_data,ignore_index=True)


        if (len(tag1_df)>0 & len(tag2_df)>0):
            base_currency=tag1_df.currency.mode()[0]
            tag1_df["value_scaled"] = tag1_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)    
            tag2_df["value_scaled"] = tag2_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        
        tag1_df['value']=tag1_df['value'].replace('',np.NaN)
        tag2_df['value']=tag2_df['value'].replace('',np.NaN)

        
        if (tag1_df['value'].notna().all()) and (tag2_df['value'].notna().all()):
            merged1_df=pd.merge(tag1_df,tag2_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        elif tag1_df['value'].notna().all() and tag2_df['value'].isna().all():
            merged1_df = tag1_df
        elif tag1_df['value'].isna().all() and tag2_df['value'].notna().all():
            merged1_df = tag2_df

        print(tag1_df)
        print(tag2_df)
        print(merged1_df)

        tag3_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag3_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBITDA
        if len(tag3_df)==0:
            tag3_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
        
        tag4_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag4_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBTN
        if len(tag4_df)==0:
            tag4_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag4_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
                
        if tag3_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            tag3_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag3_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag3_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            tag3_df=tag3_df.append(tag3_df_missing_data,ignore_index=True)            


        if tag4_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            tag4_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag4_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(tag4_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            tag4_df=tag4_df.append(tag4_df_missing_data,ignore_index=True)



        if (len(tag3_df)>0 & len(tag4_df)>0):
            base_currency=tag3_df.currency.mode()[0]
            tag3_df["value_scaled"] = tag3_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)    
            tag4_df["value_scaled"] = tag4_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        

        tag3_df['value']=tag3_df['value'].replace('',np.NaN)
        tag4_df['value']=tag4_df['value'].replace('',np.NaN)


        if (tag3_df['value'].notna().all()) and (tag4_df['value'].notna().all()):
            merged2_df=pd.merge(tag3_df,tag4_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        elif tag3_df['value'].notna().all() and tag4_df['value'].isna().all():
            merged2_df=tag3_df
        elif tag3_df['value'].isna().all() and tag4_df['value'].notna().all():
            merged2_df=tag4_df

        print(tag3_df)
        print(tag4_df)
        print(merged2_df)
        
        lhs_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(lhs_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBITDA
        if len(lhs_df)==0:
            lhs_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(lhs_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
        
        rhs_df = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(rhs_list))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']] #EBTN
        if len(rhs_df)==0:
            rhs_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(rhs_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
                
        if lhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            lhs_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(lhs_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(lhs_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)            
   
        if rhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
            rhs_df_missing_data=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(rhs_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(rhs_df['peo'])))&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)


        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)    
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
  
        
        lhs_df['value']=lhs_df['value'].replace('',np.NaN)
        rhs_df['value']=rhs_df['value'].replace('',np.NaN)

        
        if (lhs_df['value'].notna().all()) and (rhs_df['value'].notna().all()):           
            merged3_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        elif lhs_df['value'].notna().all() and rhs_df['value'].isna().all():
            merged3_df = lhs_df
        elif lhs_df['value'].isna().all() and rhs_df['value'].notna().all():
            merged3_df = rhs_df

        # print(lhs_df)
        # print(rhs_df)
        # print(merged3_df)

        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged1_df.iterrows():
            if ('value_scaled_x' in merged1_df.columns) & ('value_scaled_y' in merged1_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    if ('value_scaled_x' not in merged2_df.columns) & ('value_scaled_y' not in merged2_df.columns):
                        if len(tag4_df['consValue'])>0 and len(tag3_df['consValue'])>0:
                            if (tag3_df['value'].notna().any() & tag4_df['value'].isna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].notna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].isna().any()):
                                if ('value_scaled_x' not in merged3_df.columns) & ('value_scaled_y' not in merged3_df.columns):
                                    if len(rhs_df['consValue'])>0 and len(lhs_df['consValue'])>0:
                                        if (lhs_df['value'].notna().any() & rhs_df['value'].isna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].notna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId'])
                                            
        for ind,row in merged1_df.iterrows():
            if ('value_scaled_x' in merged1_df.columns) & ('value_scaled_y' in merged1_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    for ind,row in merged2_df.iterrows():
                        if ('value_scaled_x' in merged2_df.columns) & ('value_scaled_y' in merged2_df.columns):
                            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                                if ('value_scaled_x' not in merged3_df.columns) & ('value_scaled_y' not in merged3_df.columns):
                                    if len(rhs_df['consValue'])>0 and len(lhs_df['consValue'])>0:
                                        if (lhs_df['value'].notna().any() & rhs_df['value'].isna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].notna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId'])
                                            
        for ind,row in merged1_df.iterrows():
            if ('value_scaled_x' in merged1_df.columns) & ('value_scaled_y' in merged1_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    for ind,row in merged3_df.iterrows():
                        if ('value_scaled_x' in merged3_df.columns) & ('value_scaled_y' in merged3_df.columns):
                            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                                if ('value_scaled_x' not in merged2_df.columns) & ('value_scaled_y' not in merged2_df.columns):
                                    if len(tag4_df['consValue'])>0 and len(tag3_df['consValue'])>0:
                                        if (tag3_df['value'].notna().any() & tag4_df['value'].isna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].notna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId'])   
                                            
        for ind,row in merged2_df.iterrows():
            if ('value_scaled_x' in merged2_df.columns) & ('value_scaled_y' in merged2_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    if ('value_scaled_x' not in merged1_df.columns) & ('value_scaled_y' not in merged1_df.columns):
                        if len(tag2_df['consValue'])>0 and len(tag1_df['consValue'])>0:
                            if (tag1_df['value'].notna().any() & tag2_df['value'].isna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].notna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].isna().any()):
                                if ('value_scaled_x' not in merged3_df.columns) & ('value_scaled_y' not in merged3_df.columns):
                                    if len(rhs_df['consValue'])>0 and len(lhs_df['consValue'])>0:
                                        if (lhs_df['value'].notna().any() & rhs_df['value'].isna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].notna().any()) or (lhs_df['value'].isna().any() & rhs_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId']) 
                                            
        for ind,row in merged2_df.iterrows():
            if ('value_scaled_x' in merged2_df.columns) & ('value_scaled_y' in merged2_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    for ind,row in merged3_df.iterrows():
                        if ('value_scaled_x' in merged3_df.columns) & ('value_scaled_y' in merged3_df.columns):
                            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                                if ('value_scaled_x' not in merged1_df.columns) & ('value_scaled_y' not in merged1_df.columns):
                                    if len(tag2_df['consValue'])>0 and len(tag1_df['consValue'])>0:
                                        if (tag1_df['value'].notna().any() & tag2_df['value'].isna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].notna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId'])
                                            
        for ind,row in merged3_df.iterrows():
            if ('value_scaled_x' in merged3_df.columns) & ('value_scaled_y' in merged3_df.columns):
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    if ('value_scaled_x' not in merged1_df.columns) & ('value_scaled_y' not in merged1_df.columns):
                        if len(tag2_df['consValue'])>0 and len(tag1_df['consValue'])>0:
                            if (tag1_df['value'].notna().any() & tag2_df['value'].isna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].notna().any()) or (tag1_df['value'].isna().any() & tag2_df['value'].isna().any()):
                                if ('value_scaled_x' not in merged2_df.columns) & ('value_scaled_y' not in merged2_df.columns):
                                    if len(tag4_df['consValue'])>0 and len(tag3_df['consValue'])>0:
                                        if (tag3_df['value'].notna().any() & tag4_df['value'].isna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].notna().any()) or (tag3_df['value'].isna().any() & tag4_df['value'].isna().any()):
                                            peos.append(row['peo'])
                                            tid.append(row['tradingItemId'])
                                            parentflag.append(row['parentFlag'])
                                            accounting.append(row['accountingStandardDesc']) 
                                            fyc.append(row['fiscalChainSeriesId'])

    

        # temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(tag1_list) & extractedData_parsed['peo'].isin(peos)) | (extractedData_parsed['dataItemId'].isin(tag2_list) & extractedData_parsed['peo'].isin(peos)) | (extractedData_parsed['dataItemId'].isin(tag3_list) & extractedData_parsed['peo'].isin(peos)) | (extractedData_parsed['dataItemId'].isin(tag4_list) & extractedData_parsed['peo'].isin(peos)) | (extractedData_parsed['dataItemId'].isin(lhs_list) & extractedData_parsed['peo'].isin(peos)) | (extractedData_parsed['dataItemId'].isin(rhs_list) & extractedData_parsed['peo'].isin(peos))]
        # temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(tag3_list) & historicalData_parsed['peo'].isin(peos)) | (historicalData_parsed['dataItemId'].isin(tag2_list) & historicalData_parsed['peo'].isin(peos)) | (historicalData_parsed['dataItemId'].isin(tag1_list) & historicalData_parsed['peo'].isin(peos)) | (historicalData_parsed['dataItemId'].isin(tag4_list) & historicalData_parsed['peo'].isin(peos)) | (historicalData_parsed['dataItemId'].isin(lhs_list) & historicalData_parsed['peo'].isin(peos)) | (historicalData_parsed['dataItemId'].isin(rhs_list) & historicalData_parsed['peo'].isin(peos))]

        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(tag1_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(tag2_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(tag3_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))    
                |((extractedData_parsed['dataItemId'].isin(tag4_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((extractedData_parsed['dataItemId'].isin(lhs_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((extractedData_parsed['dataItemId'].isin(rhs_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
                                                                                                                                                                                                                      
        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(tag1_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(tag4_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(lhs_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(rhs_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "A check should fire when Net income GAAP & Normalized are collected as same and EPS Normalized is collected but not duplicated for EPS GAAP or vice versa depending on the consensus availability (similarly for EBT Normalized and GAAP flavors)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "A check should fire when Net income GAAP & Normalized are collected as same and EPS Normalized is collected but not duplicated for EPS GAAP or vice versa depending on the consensus availability (similarly for EBT Normalized and GAAP flavors)"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBTG_EBTN_lessthan_NIG_NIN_n_ETR_clctd_fr_Ltst_act(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBT_GAAP
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #NI_GAAP
    tag_list=get_dataItemIds_list('TAG1', parameters) #Taxrate
    tag2_list=get_dataItemIds_list('TAG2', parameters) #EBT_Norm
    tag3_list=get_dataItemIds_list('TAG3', parameters) #NI_Norm
    operator = get_dataItemIds_list('Operation', parameters) #["<"]
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NI_GAAP
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT _GAAP
        ETR_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Taxrate
        tag2_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag2_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NI_Norm
        tag3_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag3_list)&(extractedData_parsed['value']!="")][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBT_Norm
        if lhs_df["dataItemId"].nunique()!=len(left_dataItemIds_list):
            not_captured= [x for x in left_dataItemIds_list if x not in set(lhs_df["dataItemId"])]
            lhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            lhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
       
        if rhs_df["dataItemId"].nunique()!=len(right_dataItemIds_list):
            not_captured= [x for x in right_dataItemIds_list if x not in set(rhs_df["dataItemId"])]
            rhs_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df = pd.concat([rhs_df,rhs_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=rhs_df_peo_count[(rhs_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=rhs_df[rhs_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            rhs_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
        
        if ETR_df["dataItemId"].nunique()!=len(tag_list):
            ETR_df = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        ETR_df_peo_count=ETR_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=ETR_df_peo_count[(ETR_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=ETR_df[ETR_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            ETR_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            ETR_df=ETR_df.append(ETR_df_missing_data,ignore_index=True)            

        if tag2_df["dataItemId"].nunique()!=len(tag2_list):
            not_captured= [x for x in tag2_list if x not in set(tag2_df["dataItemId"])]
            tag2_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            tag2_df = pd.concat([tag2_df,tag2_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        tag2_df_peo_count=tag2_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (tag2_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=tag2_df_peo_count[(tag2_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=tag2_df[tag2_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            tag2_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            tag2_df=tag2_df.append(tag2_df_missing_data,ignore_index=True)

        if tag3_df["dataItemId"].nunique()!=len(tag3_list):
            not_captured= [x for x in tag3_list if x not in set(tag3_df["dataItemId"])]
            tag3_df_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(not_captured)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
            tag3_df = pd.concat([tag3_df,tag3_df_not_cap])
        extracted_dataItemIds_peo_count=extractedData_parsed['peo'].nunique()
        tag3_df_peo_count=tag3_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')    
            
        if (tag3_df_peo_count['peocount']<extracted_dataItemIds_peo_count).any():
            missed_peo_tag=tag3_df_peo_count[(tag3_df_peo_count['peocount']<extracted_dataItemIds_peo_count)]['dataItemId']
            collected_peo=tag3_df[tag3_df['dataItemId'].isin(missed_peo_tag)][['dataItemId','peo']]
            tag3_df_missing_data=historicalData_parsed[historicalData_parsed['dataItemId'].isin(missed_peo_tag)&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo'])))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            tag3_df=tag3_df.append(tag3_df_missing_data,ignore_index=True)

        if (len(lhs_df)>0 & len(rhs_df)>0 & len(ETR_df)>0 & len(tag2_df)>0 & len(tag3_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            ETR_df["value_scaled"] = ETR_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)      
            tag2_df["value_scaled"] = tag2_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            tag3_df["value_scaled"] = tag3_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBITDA
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT  
        ETR_df=ETR_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #DA
        tag2_df=tag2_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()
        tag3_df=tag3_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()
        

        
        merged1_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        merged2_df=pd.merge(tag2_df,tag3_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        merged_df=pd.merge(merged1_df,merged2_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        merged_ETR_df = pd.merge(ETR_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        print(lhs_df)
        print(rhs_df)
        print(ETR_df)
        print(tag2_df)
        print(tag3_df)

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        
        for ind,row in merged_ETR_df.iterrows():
            if (row['value_scaled']>0):
                if execute_operator(row['value_scaled_x_y'],row['value_scaled_y_y'],operator[0]):
                    peos.append(row['peo'])
                    difference=row[['value_scaled_x_y','value_scaled_y_y']].max()-row[['value_scaled_x_y','value_scaled_y_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x_y','value_scaled_y_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        #print(diff_df)      
        
        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                |((extractedData_parsed['dataItemId'].isin(tag_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))    
                |((extractedData_parsed['dataItemId'].isin(tag2_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((extractedData_parsed['dataItemId'].isin(tag3_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
                                                                                                                                                                                                                      
        temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))  
                |((historicalData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(tag_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(tag2_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))
                |((historicalData_parsed['dataItemId'].isin(tag3_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))
                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "432_EBT GAAP and EBT Normalized less than NI GAAP, NI Normalized and  ETR collected for Latest actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "432_EBT GAAP and EBT Normalized less than NI GAAP, NI Normalized and  ETR collected for Latest actual"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def New_flavor_captured_for_the_company(historicalData,filingMetadata,extractedData,parameters):

    # 
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    try:
        companyid=filingMetadata['metadata']['companyId']

        current = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")][['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        previous = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)][['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        merged_df = pd.merge(current,previous,on=['dataItemId'],how='inner')

       
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]

        for ind,row in merged_df.iterrows():
            if (execute_operator(row['parent_flag_x'],row['parent_flag_y'],operator[0]) | execute_operator(row['accountingStandardDesc_x'],row['accountingStandardDesc_y'],operator[0]) | execute_operator(row['tradingItemId_x'],row['tradingItemId_y'],operator[0]) | execute_operator(row['fiscalChainSeriesId_x'],row['fiscalChainSeriesId_y'],operator[0])):
                peos.append(row['peo'])
                tid.append(row['tradingItemId'])
                parentflag.append(row['parentFlag'])
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])


        for ind, row in merged_df.iterrows():
            result = {"highlights": [], "error": "New flavor captured for the company"}
            result["highlights"].append({"row": {"name": row['dataItemId']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId']}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId']}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors
#Estimates Error Checks 
@add_method(Validation)
def Sum_of_Quarters_not_equal_to_FY(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters) #["!="]    
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    try:
        companyid=filingMetadata['metadata']['companyId']
        FQ = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&((extractedData_parsed["periodTypeId"]==2)|(extractedData_parsed["periodTypeId"]==10))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 
        
        FQ_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(FQ['dataItemId'])&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")&((historicalData_parsed["periodTypeId"]==2)|(historicalData_parsed["periodTypeId"]==10))&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
        FY = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed["periodTypeId"]==1)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
        if len(FY)==0:
            FY = historicalData_parsed[historicalData_parsed['dataItemId'].isin(tag_list)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")&(historicalData_parsed["periodTypeId"]==1)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 
        if (len(FY)>0 & (len(FQ)>0|len(FQ_not_cap)>0)): 
            base_currency=FY.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ_not_cap["value_scaled"] = FQ_not_cap.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        FQ = pd.concat([FQ,FQ_not_cap]).groupby(['dataItemId','fiscalYear','periodTypeId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo','count'),PEO_Sum=('value_scaled','sum')).reset_index()
        
        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId',],how='inner')
        
        merged_df['diff'] = abs((merged_df[['PEO_Sum','value_scaled']].max(axis=1)-merged_df[['PEO_Sum','value_scaled']].min(axis=1))/merged_df[['PEO_Sum','value_scaled']].min(axis=1))*100        
        
        # print(FQ)
        # print(FY)
        # print(merged_df)
        # print(merged_df['diff'])
        # #print(merged_df['PEO_Sum','value_scaled'])


        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
        dataItemIds=[]
        FQs=[]
        for ind,row in merged_df.iterrows():
            
            # print(type(row['periodTypeId_x'])) #float
            # print(type(row['PEO_Count'])) #int
            # print(type(row['PEO_Sum']))   #float
            # print(type(row['value_scaled']))  #float
            # print(type(row['diff']))     #float
            # print(type(Threshold[0]))    #int

            if row['periodTypeId_x']==2:
                if row['PEO_Count']==4:
                    print(row['diff'])
                    if execute_operator(row['diff'],float(Threshold[0]),operator[0]):
                        print(row['value_scaled'])
                        print(row['PEO_Sum'])
                        if execute_operator(row['PEO_Sum'],row['value_scaled'],operator[1]):
                            peos.append(row['fiscalYear'])
                            dataItemIds.append(row['dataItemId'])
                            FQs.append(row['fiscalQuarter'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])                            
                            diff='NA'                            
                            perc='NA'        
        for ind,row in merged_df.iterrows():
            if row['periodTypeId_x']==10:
                if row['PEO_Count']==2:
                    if execute_operator(row['diff'],Threshold[0],operator[0]):
                        if execute_operator(row['PEO_Sum'],row['value_scaled'],operator[1]):
                            peos.append(row['fiscalYear'])
                            dataItemIds.append(row['dataItemId'])
                            FQs.append(row['fiscalQuarter'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])                            
                            diff='NA'                            
                            perc='NA'        
        diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
        

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['fiscalYear'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
        temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) &( historicalData_parsed['fiscalYear'].isin(peos))&(historicalData_parsed['companyId']==companyid)
                                      &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        

        for ind, row in temp1.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "Sum of Quarters not equal to FY"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                    
        for ind, row in temp2.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "Sum of Quarters not equal to FY"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors) 
        return errors    
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def tag_has_greter_value_compare_with_next_year(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    operator = get_dataItemIds_list('Operation', parameters) 
    try:

        companyid=filingMetadata['metadata']['companyId']
        currentyear=int(filingMetadata['metadata']['latestPeriodYear'])
        

        fy1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['fiscalYear']==(currentyear)+1)&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']] 
      
        fy0=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(fy1['dataItemId']))&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['fiscalYear']==currentyear)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']]
        
        print(fy1)
        print(fy0)
        
        if (len(fy1)>0 and len(fy0)>0):
            base_currency=fy0.currency.mode()[0]
            fy0["value_scaled"] = fy0.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
            fy1["value_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
            # fy0['tradingItemId']=fy0['tradingItemId'].replace('',np.NaN)
            # fy1['tradingItemId']=fy1['tradingItemId'].replace('',np.NaN) 
            
            merged_df=pd.merge(fy0,fy1,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
           
            merged_df['multipled_value']=merged_df['value_scaled_x']*float(factor[0])
            
            
            print(fy1)
            print(fy0)


            dataItemIds=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]
    
            for ind,row in merged_df.iterrows():

                if execute_operator(row['value_scaled_y'],row['multipled_value'],operator[0]):
                    dataItemIds.append(row['dataItemId'])               
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(difference)
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])                        
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            

            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc})

            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['fiscalYear']==currentyear+1)&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['tradingItemId'].isin(tid))
                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds))&(historicalData_parsed['companyId']==companyid) & (historicalData_parsed['fiscalYear']==currentyear)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['companyId']==companyid)
                                          &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]    
            

            
            for ind,row in temp1.iterrows():
                
                result = {"highlights": [], "error": "Revenue High guidance for FY+1 10 times greater than FY+0 guidance"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
                             
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "Revenue High guidance for FY+1 10 times greater than FY+0 guidance"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)                
           
        print(errors) 
        return errors                                                                   
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def Effective_Tax_Rate_value_should_not_exeed_100(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # tags
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    operator = get_dataItemIds_list('Operation', parameters) 
  
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','value','peo','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


        dataItemIds=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
    
        for ind, row in temp.iterrows():
            if execute_operator(float(row['value']),float(Threshold[0]),operator[0]):
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA' 
    
        diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
        
        temp2=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp2.iterrows():                
                result = {"highlights": [], "error": "Effective Tax Rate value should not exeed 100"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

    # errors = []
    # dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # tags
    
  
    # try:
    #     temp = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        

    #     dataItemIds=[]
    #     peos=[]
    #     tid=[]
    #     parentflag=[]
    #     accounting=[]
    #     fyc=[]        
    #     diff=[]
    #     perc=[]
    
    #     for ind, row in temp.iterrows():

    #         if float(row['value'])>100:

    #             dataItemIds.append(row['dataItemId'])
    #             peos.append(row['peo'])
    #             tid.append(row['tradingItemId']) 
    #             parentflag.append(row['parentFlag']) 
    #             accounting.append(row['accountingStandardDesc']) 
    #             fyc.append(row['fiscalChainSeriesId'])
    #             diff='NA'
    #             perc='NA' 
    
    #     diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
    #     temp2=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
    #                                               &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

    #     for ind, row in temp2.iterrows():                
    #             result = {"highlights": [], "error": "Effective Tax Rate value should not exeed 100"}
    #             result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
    #             result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
    #             result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
    #             errors.append(result)
    #     print(errors)
    #     return errors
    # except Exception as e:
    #     print(e)
    #     return errors

#Estimates Error Checks 
@add_method(Validation)
def Sum_of_Quarters_DPS_not_equal_to_FY(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    try:
        FQ = extractedData_parsed[extractedData_parsed['dataItemId'].isin(dataItemId_list)&(extractedData_parsed["value"] != "") & (extractedData_parsed["periodTypeId"] == 2)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 
        FQ_not_cap = historicalData_parsed[historicalData_parsed['dataItemId'].isin(FQ['dataItemId']) &(historicalData_parsed["value"] != "")& (historicalData_parsed["periodTypeId"] == 2) &(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))] [['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]

        # print(FQ)

        FY = extractedData_parsed[extractedData_parsed['dataItemId'].isin(dataItemId_list) &(extractedData_parsed["value"] != "")& (extractedData_parsed["periodTypeId"] == 1)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
        
        if len(FY)==0:
            FY = historicalData_parsed[historicalData_parsed['dataItemId'].isin(dataItemId_list) &(historicalData_parsed["value"] != "")& (historicalData_parsed["periodTypeId"] ==1) & (historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 

        if ((len(FY)>0) & (len(FQ)>0|len(FQ_not_cap)>0)): 
            base_currency=FY.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ_not_cap["value_scaled"] = FQ_not_cap.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    

        FQ = pd.concat([FQ,FQ_not_cap])
        FQ_count = FQ.groupby(['dataItemId','fiscalYear','periodTypeId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo','count'),PEO_Sum=('value_scaled','sum')).reset_index()
        

        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        merged_df1=pd.merge(merged_df,FQ_count,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        
        
        peos=[]
        diff=[]
        perc=[]
        dataItemIds=[]
        FQx=[]
        FQy=[]

        for ind,row in merged_df1.iterrows():
            print(row)
            if row['PEO_Count']==4:
                if row['fiscalQuarter_y']==4:
                    if  execute_operator ((row['PEO_Sum']-row['value_scaled_x']),float(Threshold[0]),operator[0]):
                        if execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[1]):
                            peos.append(row['fiscalYear'])
                            dataItemIds.append(row['dataItemId'])
                            FQx.append(row['fiscalQuarter_x'])
                            FQy.append(row['fiscalQuarter_y'])
                            diff='NA'
                            perc='NA'
           
        diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,'fiscalQuarter_x':FQx,'fiscalQuarter_y':FQy})


        temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & extractedData_parsed['fiscalYear'].isin(peos)&((extractedData_parsed['fiscalQuarter'].isin(FQx))|(extractedData_parsed['fiscalQuarter'].isin(FQy))))]
        temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(dataItemIds) & historicalData_parsed['fiscalYear'].isin(peos)&((historicalData_parsed['fiscalQuarter'].isin(FQx))|(historicalData_parsed['fiscalQuarter'].isin(FQy))))]
        

        for ind, row in temp1.iterrows():

                result = {"highlights": [], "error": "DPS Q4 is Equal to FY, But Sum of Q1, Q2 & Q3 Greater than 0"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['description'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                    
        for ind, row in temp2.iterrows():

                result = {"highlights": [], "error": "DPS Q4 is Equal to FY, But Sum of Q1, Q2 & Q3 Greater than 0"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": row['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId'],"versionId": row['versionId'],"filingDate": row['filingDate']})
                result["checkGeneratedFor"]={"statement": "", "tag": row['description'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors

###--------------------------------------------------Estimates Checks---------------------------------------------------------------------------

#Estimates Error Checks 
@add_method(Validation)
def Quarters_values_compare_to_FY_or_Semi_Annual(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Any Dataitemid
    operator = get_dataItemIds_list('Operation', parameters) #[">"]

    try:
        FQ = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull()) & (extractedData_parsed["periodTypeId"] == 2))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #Quarters data
        # print(FQ)
        HY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull()) & (extractedData_parsed["periodTypeId"] == 10))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #semi anual data
        # print(HY)
        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull()) & (extractedData_parsed["periodTypeId"] == 1))][['dataItemId','peo','scale','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]  #Annual data
        # print(FY)
    
        
        if (((len(FY)>0) & (len(FQ)>0)) | ((len(HY)>0) & (len(FQ)>0))):
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
                    diff.append(difference)
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    FQx.append(row['fiscalQuarter_x'])
                    FQy.append(row['fiscalQuarter_y'])
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        if merged_df1 is not None:
            for ind,row in merged_df1.iterrows():
                if  execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(difference)
                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    FQx.append(row['fiscalQuarter_x'])
                    FQy.append(row['fiscalQuarter_y'])
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)            
    
    
            diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,'fiscalQuarter_x':FQx,'fiscalQuarter_y':FQy})
    
            if len(diff_df)>0:
                temp = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & extractedData_parsed['fiscalYear'].isin(peos)&((extractedData_parsed['fiscalQuarter'].isin(FQx))|(extractedData_parsed['fiscalQuarter'].isin(FQy))))]
                
                temp1_revised=temp.dropna()
        
                for ind, row in temp1_revised.iterrows():
        
                        result = {"highlights": [], "error": "Quarterly values should not be more than FY or Semi annual period."}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['description'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                        errors.append(result)                        
                print(errors) 
                return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def Cash_EPS_should_be_greater_than_EPS_normalized_and_GAAP(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #CashEPS
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EPSNormalized
    tag1_list=get_dataItemIds_list('TAG1', parameters) #EPSGAAP
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
        # companyid=filingMetadata['metadata']['companyId']
    
        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CashEPS
        rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPS Normalised
        tag1_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag1_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EPS GAAP
        
        # print(lhs_df,rhs_df,tag1_df)
        
        if (len(lhs_df)>0 & len(rhs_df)>0 | len(lhs_df)>0 & len(tag1_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            tag1_df["value_scaled"] = tag1_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        
        if (((len(lhs_df)>0) & (len(rhs_df)>0)) | ((len(lhs_df)>0) & (len(tag1_df)>0))):
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
            merged_df1=pd.merge(lhs_df,tag1_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        
        # print(merged_df)
        # print(merged_df1)
        
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
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)        
        
        if merged_df1 is not None:
            for ind,row in merged_df1.iterrows():
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])               
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna()  
                
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Cash EPS should be greater than EPS normalized and GAAP."}
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
def FYC_Series_variation(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    operator = get_dataItemIds_list('Operation', parameters) #[<>]
    
    try:
        current = extractedData_parsed[(((extractedData_parsed['dataItemFlag']=='E'))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId']]
        current['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
        current['companyId']=filingMetadata['metadata']['companyId']
        
        previous = historicalData_parsed[(historicalData_parsed['companyId'].isin(current['companyId'])
                                  &((historicalData_parsed['dataItemFlag']=='E'))&(historicalData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['fiscalChainSeriesId','filingDate','companyId']]
        
        maxprevious=previous.groupby(['companyId'])['filingDate'].max().reset_index()

        previous=previous[previous['filingDate'].isin(maxprevious['filingDate'])]

        merged_df=pd.merge(current,previous,on=['companyId'],how='inner')
        
        # print(merged_df)

        filing_date=[]
        diff=[]
        perc=[]
        cseries=[]
        pseries=[]

        if merged_df is not None:        
            for ind,row in merged_df.iterrows():
                if execute_operator(row['fiscalChainSeriesId_x'],row['fiscalChainSeriesId_y'],operator[0]):
                    filing_date.append(row['filingDate_y'])             
                    cseries.append(row['fiscalChainSeriesId_x'])
                    pseries.append(row['fiscalChainSeriesId_y'])
                    difference='NA'
                    diff.append(difference)
                    perc='NA'
    
            diff_df=pd.DataFrame({"diff":diff,"perc":perc,"filingDate":filing_date,"curseries":cseries,"preseries":pseries})
    
            if len(diff_df)>0:        
                temp1 = extractedData_parsed[extractedData_parsed['fiscalChainSeriesId'].isin(cseries)]
                temp2 = historicalData_parsed[(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])&historicalData_parsed['fiscalChainSeriesId'].isin(pseries))]
        
                temp1_revised=temp1.dropna()  
                temp2_revised=temp2.dropna()  
                
                if len(temp1_revised)>0 and len(temp2_revised)>0:
        
                    for ind, row in temp1_revised.iterrows():       
                        result = {"highlights": [], "error": "FYC Series variation"}
                        result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feed_file_id": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "trading_item_id": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscal_quarter":row["fiscalQuarter"], "peo": row["peo"]}]
                        errors.append(result)
                   
                    for ind, row in temp2_revised.iterrows():
                        result = {"highlights": [], "error": "FYC Series variation"}
                        result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feed_file_id": row['feedFileId'], "peo": row["peo"]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "trading_item_id": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscal_quarter":row["fiscalQuarter"], "peo": row["peo"]}]
                        errors.append(result)    
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors 


#Estimates Error Checks 
@add_method(Validation)
def  Recommendation_is_a_compulsory_data_item(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Recommendation
    try:
        #Rating_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(tag_list))&((extractedData_parsed['dataItemFlag']=='G')))][['dataItemId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        Rating_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)][['dataItemId','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId']] #Rating dataitemid
        
        # print(Rating_df)

        if len(Rating_df)>0:
            dataItemids=[]
            peos=[]
            diff=[]
            perc=[]

            if Rating_df is not None:            
                for ind, row in Rating_df.iterrows():
                    if row['value'] =='':
                        dataItemids.append(row['dataItemId'])
                        peos='NA'
                        diff='NA'
                        perc='NA'
                
                diff_df=pd.DataFrame({"dataItemId":dataItemids,"peo":peos,"diff":diff,"perc":perc})
    
                if len(diff_df)>0:            
                    temp1=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemids))]
            
                    temp1_revised=temp1.dropna()  
            
                    for ind, row in temp1_revised.iterrows():
                                    
                        result = {"highlights": [], "error": "Recommendation is compulsory line item and should be collected from the document"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feed_file_id": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "trading_item_id": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscal_quarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                        errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation)
def Data_item_has_zero_values(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # Interest expense
    Threshold=get_parameter_value(parameters,'Min_Threshold') #provide value as zero 0
    operator = get_dataItemIds_list('Operation', parameters)  #['==']
  
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','peo','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


        if len(temp)>0:
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]

            if temp is not None:        
                for ind, row in temp.iterrows():
                    if execute_operator(float(row['value']),float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA' 
            
                diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
    
                if len(diff_df)>0:            
                    temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                    temp1_revised=temp1.dropna()  
            
                    for ind, row in temp1_revised.iterrows():                
                            result = {"highlights": [], "error": "Validation should trigger when data item values captured as 'zero' "}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                            errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation)
def Base_Year_Negative_LTGR(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #eps norm
    right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters) #eps LTGR
    # tag1_list=get_dataItemIds_list('TAG1', parameters) #eps LTGR
    operator = get_dataItemIds_list('Operation', parameters) #["!="]

    try:
        # companyid= filingMetadata['metadata']['companyId']
        # docuement_date= pd.to_datetime(filingMetadata['metadata']['filingDate'])        
        latest_actual =pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo'])
        
        # print(latest_actual)
        
        datazip=list(zip(left_dataItemId_list,right_dataItemId_list))
        comparable=pd.DataFrame(datazip,columns=['dataitem1','dataitem2'])         
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemId_list)&(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['latestActualizedPeo']==latest_actual)&(extractedData_parsed["periodTypeId"] == 1)][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]  #eps norm

        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemId_list)&(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]  #eps LTGR


        lhs_df["valuesign"]=np.sign(lhs_df['value_scaled'])
        rhs_df["valuesign"]=np.sign(rhs_df['value_scaled'])
        
        # print(lhs_df)
        # print(rhs_df)

        if len(lhs_df)>0 and len(rhs_df)>0:
            merged_df=pd.merge(lhs_df,rhs_df,on=['parentFlag','accountingStandardDesc','fiscalChainSeriesId'],how='inner')
            comparable['compressed']=comparable.apply(lambda x:'%s%s' % (x['dataitem1'],x['dataitem2']),axis=1)
            merged_df['compressed']=merged_df.apply(lambda x:'%s%s' % (x['dataItemId_x'],x['dataItemId_y']),axis=1)

            dataItemIds_x=[]
            dataItemIds_y=[]
            peosx=[]
            peosy=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]

            for ind,row in merged_df.iterrows():
                if (row['compressed']==comparable['compressed']).any():

                    if ((row['valuesign_x']!=0)&(row['valuesign_y']!=0)):
                        if execute_operator(np.sign(row['valuesign_x']),np.sign(row['valuesign_y']),operator[0]):
                            dataItemIds_x.append(row['dataItemId_x'])
                            dataItemIds_y.append(row['dataItemId_y'])
                            peosx.append('peo_x')
                            peosy.append('peo_y')
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff='NA'
                            perc='NA'
            diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"peo_x":peosx,"peo_y":peosy,"diff":diff,"perc":perc})

            if len(diff_df)>0:
                diff_df['peocomb1']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId_x'],x['peo_x']),axis=1)
                diff_df['peocomb2']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId_y'],x['peo_y']),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                           
                
                temp1 = extractedData_parsed[~(((extractedData_parsed['peocomb'].isin(diff_df['peocomb1'])) &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())) | ((extractedData_parsed['peocomb'].isin(diff_df['peocomb2']))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())))][['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','tradingItemName']]
                                                                                                                                                                                                                                           
                temp1_revised=temp1.dropna()
                
                for ind, row in temp1_revised.iterrows():
    
                    if row['value']!=0:
                        
                        result = {"highlights": [], "error": "Base Year Negative_LTGR"}
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
def Same_Sign_Missing_Tag(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #[Tags]
    
    try:

        filingdate=filingMetadata['metadata']['filingDate']
        #contributor=filingMetadata['metadata']['researchContributorId']
        
        #print(contributor)

        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()) )][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list))&(historicalData_parsed['dataItemFlag']=='E')&(historicalData_parsed['filingDate']<filingdate)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull()))][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        # print(current[['parentFlag','accountingStandardDesc','fiscalChainSeriesId']])
        # print(previous[['parentFlag','accountingStandardDesc','fiscalChainSeriesId','filingDate']])
        
        
        maxprevious1=previous.groupby(['dataItemId','parentFlag','accountingStandardDesc','fiscalChainSeriesId'])['filingDate'].max().reset_index()

        maxprevious=previous[(previous['filingDate'].isin(maxprevious1['filingDate']))]
        
        
        current['didpeocomb']=current.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        maxprevious['didpeocomb']=maxprevious.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
        
        # print(current)
        # print(maxprevious['didpeocomb'])
        # print(maxprevious)
       
        temp_df=maxprevious[~((maxprevious['didpeocomb'].isin(current['didpeocomb']))&(maxprevious['parentFlag'].isin(current['parentFlag']))
                        &(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]
        
        
        # print(temp_df)

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
           
            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])))] [['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId','filingDate']]
    
    
            temp1_new=temp1.dropna() 
    
        
            for ind, row in temp1_new.iterrows():
    
                result = {"highlights": [], "error": "Same Sign/Missing Tag"}
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
def Gaps_in_Estimate_PEOs_greaterthan_2_Annuals(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    operator=get_dataItemIds_list('Operation', parameters) #>
    Threshold=get_parameter_value(parameters,'Max_Threshold') #2
    
    try:
        
        temp = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"] == 1) )][['dataItemId','peo','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId','fiscalYear','periodEndDate']]

        temp['periodEndDate'] = pd.to_datetime(temp['periodEndDate'],format='%Y-%m-%d')
        
        
        temp['year_diff']=abs(temp['periodEndDate'].diff().dt.days/365)
        
        # print(type(temp['year_diff']))
        
        # print(temp)
        # print(Threshold[0])
        # temp_df=pd.DataFrame(temp)
        
        if len(temp)>0:
            dataItemIds=[]
            parentflag=[]
            # ped=[]
            AS=[]
            fyc=[]
            diff=[]
            perc=[]     
            
            if temp is not None:
                for ind, row in temp.iterrows():
                    if execute_operator(row['year_diff'],float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        parentflag.append(row['parentFlag'])
                        # ped.append(row['periodEndDate'])
                        AS.append(row['accountingStandardDesc'])
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'  
                        
                diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,"parentFlag":parentflag,"accountingStandardDesc":AS,"fiscalChainSeriesId":fyc})
                
                print(diff_df)        
            
                if len(diff_df)>0:
         
                    temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(diff_df['dataItemId']))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(AS))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','scale','currency','value','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','tradingItemName','fiscalChainSeriesId','estimatePeriodId']]     
             
                    temp1_new=temp1.dropna() 
                
                    for ind, row in temp1_new.iterrows():
                
                        result = {"highlights": [], "error": "Gaps in Estimate PEO's (>2 Annuals) (Version Level)"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"],"estimatePeriodId":row["estimatePeriodId"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                        errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e) 
        return errors
        

#Estimates Error Checks 
@add_method(Validation)
def Currency_Difference_between_current_and_Previous_document(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
        
    operator = get_dataItemIds_list('Operation', parameters) #[==,!=]
    
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())
                                          &(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
        

        maxprevious=previous.groupby(['dataItemId','peo','value','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()


        # print(current)
        # print(previous)
        # print(maxprevious)

        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
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

        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                # if execute_operator(float(row['value_x']),float(row['value_y']),operator[0]): # if you want to compare values then enable this
                if execute_operator(row['currency_x'],row['currency_y'],operator[1]):
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
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds))&(historicalData_parsed['companyId']==companyid) & (historicalData_parsed['peo'].isin(peos)) 
                                               & (historicalData_parsed['filingDate'].isin(filingdate))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
               
                temp1_revised=temp1.dropna()  
                temp2_revised=temp2.dropna()  
        
                for ind, row in temp1_revised.iterrows():
        
                    result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
               
                for ind, row in temp2_revised.iterrows():
                    result = {"highlights": [], "error": "Currency Difference between current and Previous document"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e) 
        return errors         


#Estimates Error Checks 
@add_method(Validation)
def Two_revisions_on_the_same_day(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []        
    operator = get_dataItemIds_list('Operation', parameters) #[==]
    Threshold=get_parameter_value(parameters,'Max Threshold')  #0
    
    try:
        contributor=filingMetadata['metadata']['researchContributorId']
        filingdate=filingMetadata['metadata']['filingDate']        
        companyid=filingMetadata['metadata']['companyId']
        
        
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))
                                          &(historicalData_parsed['researchContributorId']==contributor)&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())
                                          &(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','scale','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
        
        #filingdate=pd.to_datetime(filingdate,format='%Y-%m-%d')

        previous['daysdiff']=abs((pd.to_datetime(filingdate,format='%Y-%m-%d')-pd.to_datetime(previous['filingDate'],format='%Y-%m-%d')).dt.days)

        maxprevious=previous.groupby(['dataItemId','peo','value','value_scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

        maxprevious1=previous[(previous['filingDate'].isin(maxprevious['filingDate']))]

        current['peocomb']=current.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['value_scaled']),axis=1)
        maxprevious1['peocomb']=maxprevious1.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['value_scaled']),axis=1)
        
        # print(current)
        # print(previous)
        # print(maxprevious)
        
        temp=maxprevious1[((maxprevious1['peocomb'].isin(current['peocomb']))&(maxprevious1['parentFlag'].isin(current['parentFlag']))
                 &(maxprevious1['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious1['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]

        # merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
        # print(merged_df)
        # print(merged_df[['dataItemId','peo','currency_x','currency_y']])
        dataItemIds=[]
        previousdate=[]
        parentflag=[]
        peo=[]
        AS=[]
        fyc=[]
        diff=[]
        perc=[] 
        
        if temp is not None:
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
                    
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peo,"filingDate":previousdate,"diff":diff,"perc":perc})
            
    
            if len(diff_df)>0:
                diff_df['peocomb']=diff_df.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['value_scaled']),axis=1)
                historicalData_parsed['peocomb']=historicalData_parsed.apply(lambda x:'%s%s%s' % (x['dataItemId'],x['peo'],x['value_scaled']),axis=1)
           
            temp1 = historicalData_parsed[((historicalData_parsed['peocomb'].isin(diff_df['peocomb'])) &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(AS))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['value']!="")&(historicalData_parsed['value'].notnull())&(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])))] [['dataItemId','peo','estimatePeriodId','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','scale','currency','team','description','fiscalYear','fiscalQuarter','tradingItemName','versionId','companyId','feedFileId','filingDate']]
    
    
            temp1_revised=temp1.dropna() 
    
            for ind, row in temp1_revised.iterrows():
    
                result = {"highlights": [], "error": "Two revisions on the same day"}
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
def dataItemIds_which_have_more_than_100_Variation_between_Quarters(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)   #['>']
    Threshold=get_parameter_value(parameters,'Min_Threshold')  #100%
    
    try:
        # companyid=filingMetadata['metadata']['companyId']
        # latestactualquarter=filingMetadata['metadata']['latestPeriodType']        
        
        currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        print(currentquarter)
        
        currentquarter['quarter_numeric'] = currentquarter['fiscalQuarter'].replace({'1': 1, '2': 2, '3': 3, '4': 4})
        
        #print(currentquarter['quarter_numeric'])
        
        currentquarter.sort_values(by=['fiscalYear', 'quarter_numeric'], inplace=True)
        
        currentquarter['variation'] = abs(((currentquarter['value_scaled'] - currentquarter['value_scaled'].shift(1)) / currentquarter['value_scaled'].shift(1)) * 100)
        
        currentquarter['difference'] = ((currentquarter['value_scaled'] - currentquarter['value_scaled'].shift(1)))

        #print(currentquarter['difference'])
        
        currentquarter.drop(columns=['quarter_numeric'], inplace=True)
        
        #print(currentquarter[['dataItemId','peo','value','variation']])
        
        if len(currentquarter) >0:    
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            dataItemIds=[]
            peos=[]
            diff=[]
            perc=[]
            
            if currentquarter is not None:
                for ind,row in currentquarter.iterrows():
                    if execute_operator(float(row['variation']),float(Threshold[0]),operator[0]):
                        peos.append(row['peo'])
                        diff.append(row['difference'])
                        perc.append(row['variation'])
                        dataItemIds.append(row['dataItemId'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        
                diff_df=pd.DataFrame({'peo':peos,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
    
    
                if len(diff_df)>0:        
                    temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
                    temp1_revised=temp1.dropna()  
                     
                    for ind, row in temp1_revised.iterrows():                
                        result = {"highlights": [], "error": "Tags which have more than 100% Variation between Quarters"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result) 
                print(errors) 
                return errors                                                                   
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def Difference_of_two_tags_not_equalto_third_tag(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #dt1, dt2
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #dt3
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
        # companyid=filingMetadata['metadata']['companyId']
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt3

        # print(lhs_df)
        # print(rhs_df)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        lhs_df['value_scaled'] = lhs_df.apply(lambda x: x['value_scaled']*(-1) if x['dataItemId']== left_dataItemIds_list[1] else x['value_scaled'], axis=1)
        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt1 - dt2
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #dt3

        # print(lhs_df)
        # print(rhs_df)

        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if (row['value_scaled_x']!=0 and row['value_scaled_y']!=0):
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])               
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna() 
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Difference of two tags not equal to the third tag"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def EBIT_is_greater_than_EBITDA(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
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
            
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
	            if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
		               if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
		                   peos.append(row['peo'])               
		                   difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
		                   tid.append(row['tradingItemId'])
		                   parentflag.append(row['parentFlag'])
		                   accounting.append(row['accountingStandardDesc']) 
		                   fyc.append(row['fiscalChainSeriesId'])
		                   diff.append(difference)
		                   perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})


            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna() 
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "EBIT is greater than EBITDA "}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Data_Collected_in_Inactive_trading_item(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)  #[>]
    
    try:
        lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['tradingItemStatus']=='Inactive')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','description','securityName','exchangeSymbol','lastTradedDate','tradingItemStatus','tradingItemId','tradingItemName']]
        
        lhs_df['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate']).date()
        
        # print(lhs_df)
        
        if len(lhs_df)>0:
        
            dataItemIds=[]
            tradingitemid=[]
            tradingitem=[]
            
            if lhs_df is not None:    
                for ind,row in lhs_df.iterrows():            
                    if execute_operator(row['filingDate'],pd.to_datetime(row['lastTradedDate']).date(),operator[0]):
                        dataItemIds.append(row['dataItemId'])               
                        tradingitemid.append(row['tradingItemId'])
                        tradingitem.append(row['tradingItemName'])
                        
          
                diff_df=pd.DataFrame({"dataItemId":dataItemIds,"tradingItemId":tradingitemid,"tradingItemName":tradingitem})
    
                if len(diff_df)>0:    
                    temp1=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) &(extractedData_parsed['tradingItemId'].isin(tradingitemid)))]
                    
                    temp1_revised=temp1.dropna()         
                    
                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "DQ_Tradingitem_inactive"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],  "tradingItemName": diff_df[diff_df['dataItemId']==row["dataItemId"]]['tradingItemName'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],  "tradingItemName": diff_df[diff_df['dataItemId']==row["dataItemId"]]['tradingItemName'].iloc[0]}]
                        errors.append(result)    
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def CASH_EPS_AND_CFPS_BOTH_ARE_SAME(historicalData,filingMetadata,extractedData,parameters):

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

        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() 
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
            
        if merged_df is not None:
            for ind,row in merged_df.iterrows():                
                if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc'])
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna()  
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "CASH EPS AND CFPS BOTH ARE SAME"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Revenue_and_EBITDA_with_same_values_from_the_same_document(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Revenue
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:

    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Revenue
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA


        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() 
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
            
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc'])
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna()  
        
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Revenue and EBITDA with same values from the same document"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Units_captured_as_Myriad(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    scale_list=get_dataItemIds_list('scalelist', parameters) #Myriad
    countryCode=get_dataItemIds_list('COUNTRY_EXCLUDE',parameters) #China
    try:
        

        scale_list2=list(map(lambda x: x.capitalize(),scale_list))
        
        countryCode1=list(map(lambda x: x.lower(),countryCode))
        
        # print(scale_list2)
        
        if filingMetadata['metadata']['country'] not in countryCode1:
            temp = extractedData_parsed[ ((extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&((extractedData_parsed['scale'].str.capitalize()).isin(scale_list2)))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

            if len(temp)>0:
                dataItemIds=[]
                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[] 
                
                if temp is not None:
                    for ind, row in temp.iterrows():            
                        peos.append(row['peo'])
                        dataItemIds.append(row['dataItemId'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'            
                    diff_df=pd.DataFrame({"peo":peos,"dataItemId":dataItemIds,"diff":diff,"perc":perc})  
    
                    if len(diff_df)>0:            
                        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                        temp1_revised=temp1.dropna() 
            
                        for ind, row in temp1_revised.iterrows():                               
                            result = {"highlights": [], "error": "Units captured as Myriad (excl china)"}
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
def Tags_which_have_more_than_100_Variation_between_Quarters(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    
    try:
        currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        comparabletquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if len(currentquarter) >0 and len(comparabletquarter)>0: 
            
            merged_df=pd.merge(currentquarter,comparabletquarter,on=['dataItemId','parentFlag','tradingItemId','accountingStandardDesc','fiscalChainSeriesId','periodTypeId',],how='inner')

            base_currency=merged_df.currency_x.mode()[0]
            merged_df['value_scaled_y'] = merged_df.apply(lambda x: currency_converter(currency_from=x['currency_y'], currency_to=base_currency, value=x['value_scaled_y']), axis=1)                
            
            merged_df['variation']=((merged_df[['value_scaled_x','value_scaled_y']].max(axis=1)-merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))/merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))*100

            dataItemIds=[]
            peos_x=[]
            peos_y=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            if merged_df is not None:
                for ind,row in merged_df.iterrows():
                    if row['fiscalQuarter_x']==4:
                        if ((row['fiscalQuarter_y']==1) & (row['fiscalYear_x']==row['fiscalYear_x']+1)& execute_operator(row['variation'],float(Threshold[0]),operator[0])):                                
                            peos_x.append(row['peo_x'])
                            peos_y.append(row['peo_y'])
                            diff.append(float(round(row['variation'])))
                            perc.append(float(round(row['variation'])))
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            dataItemIds.append(row['dataItemId'])
                    else:
                        if ((row['fiscalQuarter_y']==(row['fiscalQuarter_x']+1)) & (row['fiscalYear_x']==row['fiscalYear_y'])& execute_operator(row['variation'],float(Threshold[0]),operator[0])):       
                            peos_x.append(row['peo_x'])
                            peos_y.append(row['peo_y'])
                            diff.append(float(round(row['variation'])))
                            perc.append(float(round(row['variation'])))
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            dataItemIds.append(row['dataItemId'])  
                                
                diff_df=pd.DataFrame({"peo_x":peos_x,'peo_y':peos_y,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
                
                if len(diff_df)>0:
                    
                    diff_df['peocomb1']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo_x']),axis=1)
                    diff_df['peocomb2']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo_y']),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                    
    
                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb1']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId','peocomb']]
        
                    temp1_revised=temp1.dropna() 
    
                    temp2 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb2']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId','peocomb']]
        
                    temp2_revised=temp2.dropna() 
    
                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Tags which have more than 100% Variation between Quarters"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb1']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb1']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb1']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb1']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)
    
                    for ind, row in temp2_revised.iterrows():
                        result = {"highlights": [], "error": "Tags which have more than 100% Variation between Quarters"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb2']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb2']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb2']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb2']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)                               
            print(errors) 
            return errors                                                                   
    except Exception as e:
        print(e)
        return errors   


# Estimates Error Checks
@add_method(Validation)
def Units_captured_in_Lakhs_OR_Crores_for_otherthan_Indian_companies(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    scale_list=get_dataItemIds_list('scalelist', parameters) #Lakhs,Crores
    countryCode=get_dataItemIds_list('COUNTRY_EXCLUDE',parameters) #India
    try:
        

        scale_list2=list(map(lambda x: x.capitalize(),scale_list))
        
        countryCode1=list(map(lambda x: x.lower(),countryCode))
        
        # print(scale_list2)
        
        if filingMetadata['metadata']['country'] not in countryCode1:
            temp = extractedData_parsed[ ((extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&((extractedData_parsed['scale'].str.capitalize()).isin(scale_list2)))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


            if len(temp)>0:
                dataItemIds=[]
                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[] 
                
                if temp is not None:
                    for ind, row in temp.iterrows():            
                        peos.append(row['peo'])
                        dataItemIds.append(row['dataItemId'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'            
                    diff_df=pd.DataFrame({"peo":peos,"dataItemId":dataItemIds,"diff":diff,"perc":perc})  
    
                    if len(diff_df)>0:            
                        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                        temp1_revised=temp1.dropna()
            
                        for ind, row in temp1_revised.iterrows():                               
                            result = {"highlights": [], "error": "DQ_Units captured in Lakhs OR Crores for other than Indian companies"}
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
def Sum_of_Quarters_not_equal_to_FullYear(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
        FQ = extractedData_parsed[extractedData_parsed['dataItemId'].isin(dataItemId_list)&(extractedData_parsed["value"] !="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"] == 2)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 

        # print(FQ)

        FY = extractedData_parsed[extractedData_parsed['dataItemId'].isin(dataItemId_list) &(extractedData_parsed["value"] != "")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"] == 1)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
        
        # print(FY)

        if ((len(FY)>0) & (len(FQ)>0)): 
            base_currency=FY.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        
        FQ_count = FQ.groupby(['dataItemId','fiscalYear','periodTypeId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo','count'),PEO_Sum=('value_scaled','sum')).reset_index()
        
        # print(FQ_count)

        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        
        # print(merged_df)
        
        merged_df1=pd.merge(merged_df,FQ_count,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
        
        # print(merged_df1[['fiscalQuarter_y','value_scaled_x','value_scaled_y','PEO_Sum']])
        
        # print(merged_df1)
        
        peos=[]
        diff=[]
        perc=[]
        dataItemIds=[]
        FQx=[]
        FQy=[]

        if merged_df1 is not None:
            for ind,row in merged_df1.iterrows():
                if row['PEO_Count']==4:
                    if row['fiscalQuarter_y']==4:
                        if execute_operator (float(row['PEO_Sum']),float(row['value_scaled_y']),operator[0]):
                            peos.append(row['fiscalYear'])
                            dataItemIds.append(row['dataItemId'])
                            FQx.append(row['fiscalQuarter_x'])
                            FQy.append(row['fiscalQuarter_y'])
                            diff='NA'
                            perc='NA'
               
            diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds,'fiscalQuarter_x':FQx,'fiscalQuarter_y':FQy})
    
            if len(diff_df)>0:
                temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & extractedData_parsed['fiscalYear'].isin(peos)&((extractedData_parsed['fiscalQuarter'].isin(FQx))|(extractedData_parsed['fiscalQuarter'].isin(FQy))))]
        
                temp1_revised=temp1.dropna()
        
                for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Sum of 4Qs not equal to FY for GM"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['description'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['description'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                        errors.append(result)
                print(errors) 
                return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def NAVPS_Estimate_greater_than_BVPS_Estimate(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #NAVPS
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #BVPS
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:

    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #NAVPS
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #BVPS


        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        # lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() 
        # rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index()        
        
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)
        
        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        
        if merged_df is not None:
	            for ind,row in merged_df.iterrows():
	            	if row['value_scaled_x']!=0 and row['value_scaled_y']!=0:
		                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
		                    peos.append(row['peo'])               
		                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
		                    tid.append(row['tradingItemId'])
		                    parentflag.append(row['parentFlag'])
		                    accounting.append(row['accountingStandardDesc']) 
		                    fyc.append(row['fiscalChainSeriesId'])
		                    diff.append(difference)
		                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

        if len(diff_df)>0:    
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
            temp1_revised=temp1.dropna()  
    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "261_NAVPS Estimate greater than BVPS Estimate"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            print(errors)
            return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Check 
@add_method(Validation)
def NI_GAAP_NI_Norm_Increases_whereas_EP_GAAP_EPS_Norm_decreases_vicversa(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    ni_gaap=get_dataItemIds_list('TAG1', parameters) #NI GAAP
    ni_nor=get_dataItemIds_list('TAG2', parameters) #NI NORM
    eps_gaap=get_dataItemIds_list('TAG3', parameters) #EPS GAAP
    eps_nor=get_dataItemIds_list('TAG4', parameters) #EPS NORM
    operator=get_dataItemIds_list('Operation', parameters) #['>','<']
    try:
        
        ni_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_gaap)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        ni_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(ni_nor)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          
        eps_gaap_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_gaap)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        eps_nor_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(eps_nor)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          


        if (len(ni_gaap_df)>0 & len(ni_nor_df)>0 & len(eps_gaap_df)>0 & len(eps_nor_df)>0):
            base_currency=ni_gaap_df.currency.mode()[0]
            ni_gaap_df["value_scaled"] = ni_gaap_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            ni_nor_df["value_scaled"] = ni_nor_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            eps_gaap_df["value_scaled"] = eps_gaap_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            eps_nor_df["value_scaled"] = eps_nor_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        ni_merged_df=pd.merge(ni_gaap_df,ni_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        eps_merged_df=pd.merge(eps_gaap_df,eps_nor_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df=pd.merge(ni_merged_df,eps_merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if (execute_operator(row['value_scaled_x_x'],row['value_scaled_y_x'],operator[0]) & execute_operator(row['value_scaled_x_y'],row['value_scaled_y_y'],operator[1])):
                    peos.append(row['peo'])
                    diff='NA'
                    perc='NA'
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc}) 
    
            if len(diff_df)>0:        
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(ni_gaap)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(ni_nor)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(eps_gaap)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))    
                        |((extractedData_parsed['dataItemId'].isin(eps_nor)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                                                                                                                                                                                                                          
                temp1_revised=temp1.dropna()
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "900_NI GAAP & NI Normalized Increased whereas EPS GAAP & EPS Normalized decreases and vice versa"}
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
def Sum_of_Quarters_semis_not_equal_to_FY(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    tag_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters) #["!="]    
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    try:
        
        FQ = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&((extractedData_parsed["periodTypeId"]==2)|(extractedData_parsed["periodTypeId"]==10))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']] 
        
        FY = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed["periodTypeId"]==1)][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','currency']]
 
        if (len(FY)>0 & (len(FQ))>0): 
            base_currency=FY.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            FQ["value_scaled"] = FQ.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        
        FQ = FQ.groupby(['dataItemId','fiscalYear','periodTypeId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']).agg(PEO_Count=('peo','count'),PEO_Sum=('value_scaled','sum')).reset_index()
        
        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId',],how='inner')
        
        merged_df['diff'] = abs((merged_df[['PEO_Sum','value_scaled']].max(axis=1)-merged_df[['PEO_Sum','value_scaled']].min(axis=1))/merged_df[['PEO_Sum','value_scaled']].min(axis=1))*100        
        

        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
        dataItemIds=[]
        FQs=[]
        
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if row['periodTypeId_x']==2:
                    if row['PEO_Count']==4:
                        if execute_operator(row['diff'],float(Threshold[0]),operator[0]):
                            if execute_operator(row['PEO_Sum'],row['value_scaled'],operator[1]):
                                peos.append(row['fiscalYear'])
                                dataItemIds.append(row['dataItemId'])
                                FQs.append(row['fiscalQuarter'])
                                tid.append(row['tradingItemId']) 
                                parentflag.append(row['parentFlag']) 
                                accounting.append(row['accountingStandardDesc']) 
                                fyc.append(row['fiscalChainSeriesId'])                            
                                diff='NA'                            
                                perc='NA'        
            for ind,row in merged_df.iterrows():
                if row['periodTypeId_x']==10:
                    if row['PEO_Count']==2:
                        if execute_operator(row['diff'],Threshold[0],operator[0]):
                            if execute_operator(row['PEO_Sum'],row['value_scaled'],operator[1]):
                                peos.append(row['fiscalYear'])
                                dataItemIds.append(row['dataItemId'])
                                FQs.append(row['fiscalQuarter'])
                                tid.append(row['tradingItemId']) 
                                parentflag.append(row['parentFlag']) 
                                accounting.append(row['accountingStandardDesc']) 
                                fyc.append(row['fiscalChainSeriesId'])                            
                                diff='NA'                            
                                perc='NA'        
            diff_df=pd.DataFrame({"fiscalYear":peos,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
            
            if len(diff_df)>0:
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['fiscalYear'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                temp1_revised=temp1.dropna()       
        
                for ind, row in temp1_revised.iterrows():
                    if row['value']!=0:
                        result = {"highlights": [], "error": "Sum of Quarters not equal to FY for GM"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                        errors.append(result)                    
                print(errors) 
                return errors    
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)        
def Semi_annual_reporting_cycle_for_US_and_Canada_company(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    countryCode=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
    try:
        if filingMetadata['metadata']['country'] in countryCode:
                        
            temp = extractedData_parsed[((extractedData_parsed['periodTypeId']==10)&(extractedData_parsed['dataItemFlag']=='E')&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','fiscalChainSeriesId','periodTypeId']]

            # print(temp)
            # print(type(temp))
            
            if len(temp)>0:
                dataItemIds=[]
                peos=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[]  
                
                if temp is not None:
                    for ind, row in temp.iterrows(): 
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA'            
                    diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})  
    
                    if len(diff_df)>0:        
                        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))
                                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                        
                        temp1_revised=temp1.dropna()
                        
                        for ind, row in temp1_revised.iterrows():                               
                            result = {"highlights": [], "error": "Semi-annual for US and Canada companies which are not having semi reporting cycle"}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate'],"companyid": filingMetadata['metadata']['companyId']})
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
def Tag_Crossed_Allowed_value(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # GM/ETR
    Threshold=get_parameter_value(parameters,'Min_Threshold') #100
    operator = get_dataItemIds_list('Operation', parameters) #[>]
  
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','peo','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


        if len(temp)>0:
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
        
            if temp is not None:
                for ind, row in temp.iterrows():
                    if execute_operator(float(row['value']),float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA' 
            
                diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
    
                if len(diff_df)>0:            
                    temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                    temp1_revised = temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():                
                            result = {"highlights": [], "error": "GM/ETR greater than 100"}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                            errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def EffectiveTax_Rate_value_should_not_exeed_100(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # EffectiveTax_Rate
    Threshold=get_parameter_value(parameters,'Min_Threshold') #100
    operator = get_dataItemIds_list('Operation', parameters) #[>]
  
    try:
        ETR_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','peo','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


        if len(ETR_df)>0:
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            if ETR_df is not None:
                for ind, row in ETR_df.iterrows():
                    if execute_operator(float(row['value']),float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA' 
            
                diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
    
                if len(diff_df)>0:            
                    temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                    temp1_revised = temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():                
                            result = {"highlights": [], "error": "Effective Tax Rate value should not exeed 100"}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                            errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def Data_Item_value_threshold_for_GM(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # Gross Margin
    Threshold=get_parameter_value(parameters,'Min_Threshold') #100
    operator = get_dataItemIds_list('Operation', parameters) #[>]
  
    try:
        GM_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','value','peo','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]


        if len(GM_df)>0:
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            if GM_df is not None:
                for ind, row in GM_df.iterrows():
                    if execute_operator(float(row['value']),float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff='NA'
                        perc='NA' 
            
                diff_df = pd.DataFrame({"peo": peos,"dataItemId":dataItemIds, "diff": diff, "perc": perc})
    
                if len(diff_df)>0:            
                    temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
                    temp1_revised = temp1.dropna()
                    
                    for ind, row in temp1_revised.iterrows():                
                            result = {"highlights": [], "error": "For applicable data item if the % collected is more than 100, validation will trigger "}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                            result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                            errors.append(result)
                    print(errors)
                    return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Check    
@add_method(Validation)
def Has_split_Information(historicalData,filingMetadata,extractedData,parameters):

   errors = []
   #operator = get_dataItemIds_list('Operation', parameters)
    
   try:
       temp = extractedData_parsed[(((extractedData_parsed['dataItemFlag']=='E'))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','splitFactor']]
       
       #print(temp)
       
       temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
       
       temp['latestActualizedPeo']=pd.to_datetime(filingMetadata['metadata']['latestActualizedPeo'])    

       filtered_df = temp[(temp['splitFactor'] == temp['splitFactor'])&(temp['filingDate'] <= temp['latestActualizedPeo'])&(temp['latestActualizedPeo'] >= temp['filingDate'])]
       
       #print(temp)
       #print(filtered_df)
       
       if len(temp)>0:
           peos=[]
           diff=[]
           perc=[]
           tid=[]
           parentflag=[]
           accounting=[]
           fyc=[] 
           
           if temp is not None:
               for ind,row in temp.iterrows():
                   if not filtered_df.empty:
                       peos.append(row['peo'])
                       difference='NA'            
                       diff.append(difference)
                       perc='NA'
                       tid.append(row['tradingItemId'])
                       parentflag.append(row['parentFlag'])
                       accounting.append(row['accountingStandardDesc']) 
                       fyc.append(row['fiscalChainSeriesId'])
        
               diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
               if len(diff_df)>0:    
                   temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(temp)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                            &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                   temp1_revised=temp1.dropna()
                    
                   for ind, row in temp1_revised.iterrows():
                       result = {"highlights": [], "error": "Has split Information"}
                       result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                       result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                       result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
                       errors.append(result)
                   print(errors)
                   return errors
   except Exception as e:
       print(e)
       return errors

# Estimates Error Checks
@add_method(Validation)
def Revenue_is_Zero_and_EBIT_in_Positive(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #REVENUE
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBIT

    try:
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #REVENUE
        
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        
        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
                
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]

        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                    if row['value_scaled_x']==0:
                        if row['value_scaled_y']>0:
                            peos.append(row['peo'])
                            tid.append(row['tradingItemId'])
                            parentflag.append(row['parentFlag'])
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                               
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                    |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                    &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
    
            temp1_revised=temp1.dropna()
    
            for ind, row in temp1_revised.iterrows():
                result = {"highlights": [], "error": "171 _Revenue Estimate is Zero and EBIT in Positive for the particular PEO"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
                errors.append(result)
            print(errors)
            return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Depre_Is_zero_when_EBIT_less_than_EBITDA(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT 
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBITDA
    tag_list=get_dataItemIds_list('TAG1', parameters) #DA
    operator = get_dataItemIds_list('Operation', parameters) #["<"]
    try:
        
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBITDA 
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #EBIT
        DA_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(tag_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #DA
 
        # print(lhs_df)
        # print(rhs_df)
        # print(DA_df)        
 
        if (len(lhs_df)>0 & len(rhs_df)>0 & len(DA_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            DA_df["value_scaled"] = DA_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
 
        
        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')
        merged_DA_df = pd.merge(DA_df,merged_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)
        # print(merged_DA_df)

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        if merged_DA_df is not None:
            for ind,row in merged_DA_df.iterrows():
                if (row['value_scaled']==0):
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna()
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Dep Is zero when EBIT less than EBITDA"}
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
def New_flavor_captured_for_company(historicalData,filingMetadata,extractedData,parameters):

    errors = []

    try:
       
        current = extractedData_parsed[((extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(~(extractedData_parsed['parentFlag'].isin(historicalData_parsed['parentFlag']))
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
            
            if current is not None:
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
    
            if len(diff_df)>0:
                temp= extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) &(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
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
def Tags_which_have_more_than_100_Variation_absolutes(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) # any data item id
    operator=get_dataItemIds_list('Operation', parameters) #['>']
    Threshold=get_parameter_value(parameters,'Min_Threshold') #100
    
    try:
        currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['scale']=="ACTUAL")&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        comparabletquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(extractedData_parsed['scale']=="ACTUAL")&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if len(currentquarter) >0 and len(comparabletquarter)>0: 
            
            merged_df=pd.merge(currentquarter,comparabletquarter,on=['dataItemId','parentFlag','tradingItemId','accountingStandardDesc','fiscalChainSeriesId','periodTypeId',],how='inner')

            base_currency=merged_df.currency_x.mode()[0]
            merged_df['value_scaled_y'] = merged_df.apply(lambda x: currency_converter(currency_from=x['currency_y'], currency_to=base_currency, value=x['value_scaled_y']), axis=1)                
            
            merged_df['variation']=((merged_df[['value_scaled_x','value_scaled_y']].max(axis=1)-merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))/merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))*100

            dataItemIds=[]
            peos_x=[]
            peos_y=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            if merged_df is not None:
                for ind,row in merged_df.iterrows():
                    if row['fiscalQuarter_x']==4:
                        if ((row['fiscalQuarter_y']==1) & (row['fiscalYear_x']==row['fiscalYear_x']+1)& execute_operator(row['variation'],float(Threshold[0]),operator[0])):                                
                            peos_x.append(row['peo_x'])
                            peos_y.append(row['peo_y'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff.append(float(round(row['variation'])))
                            perc.append(float(round(row['variation'])))
                            dataItemIds.append(row['dataItemId'])
                    else:
                        if ((row['fiscalQuarter_y']==(row['fiscalQuarter_x']+1)) & (row['fiscalYear_x']==row['fiscalYear_y'])& execute_operator(row['variation'],float(Threshold[0]),operator[0])):        
                            peos_x.append(row['peo_x'])
                            peos_y.append(row['peo_y'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff.append(float(round(row['variation'])))
                            perc.append(float(round(row['variation'])))
                            dataItemIds.append(row['dataItemId'])  
                                
                diff_df=pd.DataFrame({"peo_x":peos_x,'peo_y':peos_y,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
                
                if len(diff_df)>0:
                    
                    diff_df['peocomb1']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo_x']),axis=1)
                    diff_df['peocomb2']=diff_df.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo_y']),axis=1)
                    extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x:'%s%s' % (x['dataItemId'],x['peo']),axis=1)
                    
    
                    temp1 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb1']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId','peocomb']]
        
                    temp1_revised=temp1.dropna() 
    
                    temp2 = extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb2']))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','team','description','tradingItemName','fiscalYear','fiscalQuarter','fiscalChainSeriesId','peocomb']]
        
                    temp2_revised=temp2.dropna() 
    
                    for ind, row in temp1_revised.iterrows():
                        result = {"highlights": [], "error": "Tags which have more than 100% Variation between Quarters - Only if comparable and collected value are in absolutes"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb1']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb1']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb1']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb1']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)
    
                    for ind, row in temp2_revised.iterrows():
                        result = {"highlights": [], "error": "Tags which have more than 100% Variation between Quarters - Only if comparable and collected value are in absolutes"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peocomb2']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb2']==row["peocomb"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],"value": row["value"],"units": row["scale"],"currency": row["currency"],"tradingItemName": row["tradingItemName"],"accountingStdDesc": row["accountingStandardDesc"],"parentConsolidatedFlag": row["parentFlag"],"fiscalChainSeries": row["fiscalChainSeriesId"], "diff": diff_df[diff_df['peocomb2']==row["peocomb"]]['diff'].iloc[0], "percent": diff_df[diff_df['peocomb2']==row["peocomb"]]['perc'].iloc[0]}]
                        errors.append(result)                               
            print(errors) 
            return errors                                                                   
    except Exception as e:
        print(e)
        return errors   


#Estimates Error Checks 
@add_method(Validation)
def Tags_have_same_value_for_different_data_items_for_the_same_PEO (historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Any two data item
    operator=get_dataItemIds_list('Operation', parameters) #[!=]
    try:

        temp_x = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        temp_y = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','value_scaled','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if len(temp_x) >0 and len(temp_y)>0: 
            merged_df=pd.merge(temp_x,temp_y,on=['parentFlag','peo','accountingStandardDesc','fiscalChainSeriesId','fiscalYear','periodTypeId','tradingItemId','value_scaled'],how='inner')
                                                        
 
            dataItemIds_x=[]
            dataItemIds_y=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff='NA'
            perc='NA'
            
            if merged_df is not None:
                for ind, row in merged_df.iterrows():           
                    if (execute_operator(row['dataItemId_x'],row['dataItemId_y'],operator[0])):
                        dataItemIds_x.append(row['dataItemId_x'])
                        dataItemIds_y.append(row['dataItemId_y'])
                        peos.append(row['peo'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        
                diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"diff":diff,"perc":perc,'peo':peos}) 
    
                if len(diff_df)>0:
                    temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_x)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                    
                    temp1_revised=temp1.dropna()
                    
                    temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_y)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
                    temp2_revised=temp2.dropna()
                         
                    for ind, row in temp1_revised.iterrows():        
                        result = {"highlights": [], "error": "Tags have same value for different data items for the same PEO"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result)
                        
                    for ind, row in temp2_revised.iterrows():        
                        result = {"highlights": [], "error": "Tags have same value for different data items for the same PEO"}
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
def NI_Flavors_captured_in_one_sign_but_EPS_Flavors_captured_in_another_sign(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #NI GAAP
    right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EPS GAAP
    ni_nor=get_dataItemIds_list('TAG1', parameters) #NI NORM
    eps_nor=get_dataItemIds_list('TAG2', parameters) #EPS NORM
    operator=get_dataItemIds_list('Operation', parameters) #[!=]
    
    try:
        nig_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        epsg_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          
 
        nin_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(ni_nor))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        epsn_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(eps_nor))&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]                          

        
        nig_df["valuesign"]=np.sign(nig_df['value_scaled'])
        epsg_df["valuesign"]=np.sign(epsg_df['value_scaled'])

        nin_df["valuesign"]=np.sign(nin_df['value_scaled'])
        epsn_df["valuesign"]=np.sign(epsn_df['value_scaled'])
        
        # print(nig_df)
        # print(epsg_df)
        
        if (len(nig_df) & len(epsg_df) | len(nin_df) & len(epsn_df) ):
            merged_df_gaap=pd.merge(nig_df,epsg_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
    
            merged_df_norm=pd.merge(nin_df,epsn_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
            
  
            
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]

            if merged_df_gaap is not None:
                for ind,row in merged_df_gaap.iterrows():
                    if ((row['valuesign_x']!=0)&(row['valuesign_y']!=0)):
                        if execute_operator(np.sign(row['valuesign_x']),np.sign(row['valuesign_y']),operator[0]):
                            peos.append(row['peo'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff='NA'
                            perc='NA'
            if merged_df_norm is not None:
                for ind,row in merged_df_norm.iterrows():
                    if ((row['valuesign_x']!=0)&(row['valuesign_y']!=0)):
                        if execute_operator(np.sign(row['valuesign_x']),np.sign(row['valuesign_y']),operator[0]):
                            peos.append(row['peo'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            diff='NA'
                            perc='NA'
                                                    
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
    
                if len(diff_df)>0:        
                    temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                                                  | ((extractedData_parsed['dataItemId'].isin(right_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
                   
                    temp1_revised=temp1.dropna()
                                                                                                                                                                                                                                               
                    for ind, row in temp1_revised.iterrows():
                        if row['value']!=0:                
                            result = {"highlights": [], "error": "NI Flavors are captured in one sign but EPS Flavors captured in another sign"}
                            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                            errors.append(result)                                         
                    print(errors) 
                    return errors                                                                   
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Sum_of_EBIT_and_interest_is_equal_to_EBT_Normalized(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #EBIT,INTEREST EXP
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #EBT
    operator = get_dataItemIds_list('Operation', parameters) #["=="]
    try:
       
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt1, dt2
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #dt3

        # print(lhs_df)
        # print(rhs_df)

        if (len(lhs_df)>0 & len(rhs_df)>0):
            base_currency=lhs_df.currency.mode()[0]
            lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)        
            rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)

        lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBIT + Int exp
        rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])['value_scaled'].sum().reset_index() #EBT

        # print(lhs_df)
        # print(rhs_df)

        merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'],how='inner')

        # print(merged_df)

        peos=[]
        diff=[]
        perc=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if (row['value_scaled_x']!=0 and row['value_scaled_y']!=0):
                    if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                        peos.append(row['peo'])               
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        tid.append(row['tradingItemId'])
                        parentflag.append(row['parentFlag'])
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        diff.append(difference)
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
            if len(diff_df)>0:    
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna() 
        
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "DQ_sum of EBIT and interest is equal to EBT Normalized"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                print(errors)
                return errors
    except Exception as e:
        print(e)
        return errors


# Estimates Error Checks
@add_method(Validation)
def Maintenance_Capex_is_greater_than_Capex(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemIds_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Maintenance_Capex
    right_dataItemIds_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Capex
    operator = get_dataItemIds_list('Operation', parameters) #[">"]
    try:
    
        lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Maintenance_Capex
        rhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #Capex

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
        
        if merged_df is not None:
            for ind,row in merged_df.iterrows():
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    peos.append(row['peo'])               
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
                    
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
            
            if len(diff_df)>0:
        
                temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                        |((extractedData_parsed['dataItemId'].isin(right_dataItemIds_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                        &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        
                temp1_revised=temp1.dropna()
                
                for ind, row in temp1_revised.iterrows():
                    result = {"highlights": [], "error": "Maintenance Capex is greater than Capex"}
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
def Initiation_at_Company_Level_check(historicalData,filingMetadata,extractedData,parameters):
    errors = []    
    try:
        companyid=filingMetadata['metadata']['companyId']
        
        temp0 = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="E")&(extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull()))]
 
        # print(temp0)
        
        if len(temp0)>0:
            
            temp1=historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(historicalData_parsed['dataItemFlag']=="E"))]

            # print(temp1)

            final=temp0[~(temp0['dataItemId'].isin(temp1['dataItemId'])|temp0['currency'].isin(temp1['currency']))]
            
            # print(final)
              
            dataItemIds=[]                                                                                                                                                                                         
            peos=[]
            diff=[]
            perc=[]
            currency=[]
    
            if final is not None:
                for ind,row in final.iterrows():
                    dataItemIds.append(row['dataItemId'])
                    currency.append(row['currency'])
                    peos.append(row['peo'])               
                    difference='NA'
                    diff.append(difference)
                    perc='NA'
                
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})      
                
                if len(diff_df)>0:
                    final1=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds)&extractedData_parsed['peo'].isin(peos)&extractedData_parsed['currency'].isin(currency))]
                    
                    final1_revised = final1.dropna()
                    
                    for ind, row in final1_revised.iterrows():
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

@add_method(Validation)
def Data_collected_in_Negative_sign_for_which_data_having_positive_tag(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Capital Expenditure
    try:
        capex_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value'] != "") & (extractedData_parsed['value'].notnull()))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        
        capex_df['valuesign']=np.sign((capex_df['value']).astype(float))
        
        dataItemIds=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        diff=[]
        perc=[]
        
        if capex_df is not None:
            for ind, row in capex_df.iterrows():
                if row['valuesign']<0:
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag'])
                    accounting.append(row['accountingStandardDesc'])
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'
    
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
            if len(diff_df)>0:
                diff_df['peocomb']=diff_df.apply(lambda x: '%s%s' % (x['dataItemId'],x['peo']),axis=1)
                extractedData_parsed['peocomb']=extractedData_parsed.apply(lambda x: '%s%s' % (x['dataItemId'],x['peo']),axis=1)
    
                capex_df1=extractedData_parsed[((extractedData_parsed['peocomb'].isin(diff_df['peocomb']))&(extractedData_parsed['tradingItemId'].isin(tid)) &
                                            (extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting)) &
                                            (extractedData_parsed['fiscalChainSeriesId'].isin(fyc))&(extractedData_parsed['value'] != "") &
                                            (extractedData_parsed['value'].notnull()))][['dataItemId', 'peo', 'estimatePeriodId', 'value', 'parentFlag', 'accountingStandardDesc','tradingItemId', 'fiscalChainSeriesId', 'scale', 'currency', 'team', 'description', 'fiscalYear','fiscalQuarter', 'tradingItemName']]
    
                capex_df1_revised=capex_df1.dropna()
                
                
                for ind, row in capex_df1_revised.iterrows():
                        result = {"highlights": [], "error": "In general we are not collecting Positive Capex i.e. in negative value-Positive value Tag"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                        result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                        errors.append(result)
            print(errors)
            return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def EETag_not_in_current_document_compared_to_previous_document(historicalData,filingMetadata,extractedData,parameters):

    errors = []

    try:
       
        newflavor = extractedData_parsed[((extractedData_parsed['value']!="")&(extractedData_parsed['value'].notnull())&(~(extractedData_parsed['parentFlag'].isin(historicalData_parsed['parentFlag']))
                                        |~(extractedData_parsed['tradingItemId'].isin(historicalData_parsed['tradingItemId']))
                                        |~(extractedData_parsed['accountingStandardDesc'].isin(historicalData_parsed['accountingStandardDesc']))
                                        |~(extractedData_parsed['fiscalChainSeriesId'].isin(historicalData_parsed['fiscalChainSeriesId']))
                                        ))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        if len(newflavor)>0:
            dataItemIds=[]
            parentflag=[]
            accounting=[]
            peos=[]
            tid=[]
            fyc=[]        
            diff=[]
            perc=[]
            
            if newflavor is not None:
                for ind,row in newflavor.iterrows():
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId'])
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    peos.append(row['peo'])
                    diff='NA'
                    perc='NA'
            diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo':peos})
            if len(diff_df)>0:
                temp= extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) &(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
                for ind, row in temp.iterrows():
                    result = {"highlights": [], "error": "New flavor captured for the company-EE Tag not in current document compared to previous document"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"],"companyid": filingMetadata['metadata']['companyId']}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": filingMetadata['metadata']['versionId'],"versionId": filingMetadata['metadata']['versionId'],"filingDate": filingMetadata['metadata']['filingDate']})
                    result["checkGeneratedFor"]={"statement": "", "tag": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"tag": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
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
    
    # extractedDataFile = r'C:\Users\gsravane\Downloads\estimatessample_new.json'
    extractedDataFile = r'C:\Users\gsravane\Downloads\EstimatesErrorChecks.json'

    errors={}

    if os.path.exists(executionDataFile) and os.path.exists(extractedDataFile):
        with open(executionDataFile) as fp:
            #executionData = json.loads(fp.read().encode().decode("utf-8"))
            executionData = fp.read().encode().decode("utf-8")
        with open(extractedDataFile) as fp:
            #extractedData = json.loads(fp.read().encode().decode("utf-8"))
            extractedData = fp.read().encode().decode("utf-8")

            #v = Validation()
            #errors = v.validate(extractedData, executionData)
            errors = runValidations(extractedData, executionData)
            # print(str(errors))

    with open(os.path.join(executionDataFile+".validation"),'w+') as fp:
        import json
        result = json.dumps(errors, indent=4)
        fp.write(result)


