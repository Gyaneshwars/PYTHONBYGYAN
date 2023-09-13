import json
import os
from datetime import datetime
import pandas as pd
import time


# with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8") as f:
#   data = json.load(f)

# extractedData = data["extractedData"]
# historicalData = data["historicalData"]
# filingMetadata = data["filingMetadata"]


data1=pd.read_excel(r"C:\Users\gsravane\Downloads\DCS_Data.xlsx")
print(type(data1))

associatedStrings=[{"id":21846,"group":1,"includeFlag":1,"keyword":".*sit.*"}]

parameters = { "LHSTags": [ { "value": "Slovenian Tolar" } ], "RHSTags": [ { "value": ".*sit." } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": " 1190" }, { "value": "1010" } ], "StatementType": [ { "value": "IncomeStatement" } ], "CheckDescription": [ { "value": "Cleaned Description contains string ‘sit’ for a component when the issued currency is ‘Slovenian Tolar’" } ], "MultiplicationFactor": [ { "value": "{\u0027RD\u0027:1,\u0027RDS\u0027:1}" } ] }

# def DCS1(parameters,data1):
    

#     def get_param_value(parameters,key):
#         try:
#             param_value=[]
#             for pam in parameters[key]:
#                 param_value.append(pam.get('value'))
#         except:
#             param_value=''
#         return param_value
#     def get_param_id(parameters,key):
#         try:
#             param_id=[]
#             for pam in parameters[key]:
#                 param_id.append(pam.get('id'))
#         except:
#             param_id=''
#         return param_id 


#     # industry_inc=get_param_id(parameters,'INDUSTRY_INCLUDE')
#     # industry_exc=get_param_id(parameters,'INDUSTRY_EXCLUDE')
#     # template=get_param_id(parameters,'template')
#     # gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
#     # gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
#     # country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
#     # country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
#     # AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
#     # PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
#     # Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
#     # SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
#     # stmt=parameters['StatementType'][0]['value']
#     # #LHStag=[]
#     LHStag=get_param_value(parameters,'LHSTags')
#     description = get_param_value(parameters,'CheckDescription')
#     RHStag=get_param_value(parameters,'RHSTags')
#     # #print(LHStag)

#     # if AsReportedPeriodEndDate!="":
#     #     ARoperator=AsReportedPeriodEndDate[0]
#     #     AsReportedPeriodEndDate=AsReportedPeriodEndDate[1:]


#     # def check_period(ARoperator):
#     #     if ARoperator=='>':
#     #         ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')>datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
#     #     elif ARoperator=='<':
#     #         ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')<datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
#     #     elif ARoperator=='=':
#     #         ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')==datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
#     #     else:
#     #         ret='True'
#     #     return ret

#     # results=[]
#     # errors = []

#     # if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
#     # and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
#     # and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
#     # and (len(country_inc)==0 or filingMetadata["metadata"]["countryCode"] in country_inc)\
#     # and (len(country_exc)==0 or filingMetadata["metadata"]["countryCode"] not in country_exc)\
#     # and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
#     # and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
#     # and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
#     # and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys())))):
#     #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
#     #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\
#     # import re

#     inc={}
#     exc={}
#     for st in associatedStrings:
#         if st['includeFlag']==1:
#             if st['group'] in inc.keys():
#                 inc[st['group']].append(st['keyword'])
#             else:
#                 inc[st['group']]=[st['keyword']]
#         if st['includeFlag']==0:
#             if st['group'] in exc.keys():
#                 exc[st['group']].append(st['keyword'])
#             else:
#                 exc[st['group']]=[st['keyword']]

#     print(inc)     #Reading associatedStrings
#     print(inc.keys())  #Reading & fetching groupid
    
#     import re

    
#     for desc in data1[data1['capitalStructureCleanedDescription']]:
#         for cur in data1[data1['issuedCurrencyName']]:
#             if re.search(inc[st['group']],desc,re.IGNORECASE):
#                 if re.search(LHStag,cur,re.IGNORECASE):
#                     print(description)
            
            
# description = data1[data1["capitalStructureCleanedDescription"]]
# currency = data1[data1["issuedCurrencyName"]]
# def trigger_test(description,currency,data1):

#     description_pattern = 'sit'
#     currency_pattern = 'Slovenian Tolar'


#     description_match = re.search(description_pattern, description, re.IGNORECASE)
#     currency_match = re.search(currency_pattern, currency, re.IGNORECASE)

#     print(description_match,currency_match)
#     if description_match and currency_match:
#         print("Cleaned Description contains string sit and issued currency is Slovenian Tolar")
    

# trigger_test(description, currency)


 
 
data1 = 'Slovenian Tolar'.isin(data1[data1["issuedCurrencyName"]]) and 'sit'.isin(data1[data1["capitalStructureCleanedDescription"]])
print(data1)
        
    # data2 = data2[data2["tag"].isin(LHSTags)]
    # print(data2)
        # for ind, current in finalresult.iterrows():
        #     result = {"highlights": [], "error": "", "checkGeneratedForList": []}
        #     result["error"] = CheckDescription

        # for ind, higvalues in curhighvalue.iterrows():
        #     result["highlights"].append({"section": statement,"row": {"name": higvalues["tag"], "id": higvalues["instance"]},"cell": {"peo": higvalues["peo"],"scale": higvalues["scale"],"value": higvalues["value"],"currency": higvalues["currency"],"fpo": higvalues["fpo"]},"filingId": filingMetadata["metadata"]["filingId"]})
        #     result["checkGeneratedFor"] = {"statement": statement,"tag": higvalues["tag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": higvalues["peo"],"fpo": higvalues["fpo"],"diff": current["diff"]}
        #     result["checkGeneratedForList"].append({"statement": statement,"tag": higvalues["tag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": higvalues["peo"],"fpo": higvalues["fpo"],"diff": current["diff"]})



# if __name__ == "__main__":
#     start_time = time.time()
#     data1 = trigger_test(data1)
#     print(data1)
#     print("Total time: ", time.time() - start_time)

    
