import json
import os
from datetime import datetime
import pandas as pd
import time
import regex
import re


with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8",) as f:
    data = json.load(f)

df=pd.read_excel(r"C:\Users\gsravane\Downloads\DCS_Data.xlsx")

# with open("C:\\Users\\gsravane\\Downloads\\DCS_Data.json",encoding="utf8",) as g:
#     df = json.load(g)



extractedData = data["extractedData"]
historicalData = data["historicalData"]
filingMetadata = data["filingMetadata"]

associatedStrings = [{ "id": 17050, "group": 1, "includeFlag": 1, "keyword": "sit" },{ "id": 17051, "group": 2, "includeFlag": 1, "keyword": "Slovenian Tolar" }]


parameters = {
              "Tag1": [ { "value": "capitalStructureCleanedDescription" } ],
              "Tag2": [ { "value": "issuedCurrencyName" } ],
              "CheckDescription": [ { "value": "Cleaned Description contains string (Buyer Credit) or (Buyers Credit) and tag is other than DBBL"} ]}
# , 
#               "BlockId": [ { "value": " 1190" }, { "value": "1010" } ], 
#               "StatementType": [ { "value": "DCS" } ], 
#               "CheckDescription": [ { "value": "Cleaned Description contains string (Buyer Credit) or (Buyers Credit) and tag is other than DBBL"} ]}

def DCS1(historicalData, filingMetadata, extractedData, parameters,data1):

    def get_param_value(parameters,key):
                try:
                                param_value=[]
                                for pam in parameters[key]:
                                            param_value.append(pam.get('value'))
                except:
                                param_value=''
                return param_value
    def get_param_id(parameters,key):
                try:
                                param_id=[]
                                for pam in parameters[key]:
                                            param_id.append(pam.get('id'))
                except:
                                param_id=''
                return param_id 


    industry_inc=get_param_id(parameters,'INDUSTRY_INCLUDE')
    industry_exc=get_param_id(parameters,'INDUSTRY_EXCLUDE')
    template=get_param_id(parameters,'template')
    gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
    gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
    country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
    country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
    AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
    PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
    Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
    SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
    #stmt=parameters['StatementType'][0]['value']
    CheckDescription=parameters['CheckDescription'][0]['value']
    #LHStag=[]
    LHStag=get_param_value(parameters,'LHSTags')
    Tag1=parameters['Tag1'][0]['value']
    Tag2=parameters['Tag2'][0]['value']
    #print(LHStag)

    if AsReportedPeriodEndDate!="":
        ARoperator=AsReportedPeriodEndDate[0]
        AsReportedPeriodEndDate=AsReportedPeriodEndDate[1:]

    def check_period(ARoperator):
        if ARoperator=='>':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')>datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
        elif ARoperator=='<':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')<datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
        elif ARoperator=='=':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y/%m/%d')==datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y/%m/%d')
        else:
            ret='True'
        return ret

    results=[]
    errors = []

    if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
    and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
    and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
    and (len(country_inc)==0 or filingMetadata["metadata"]["countryCode"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["countryCode"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys())))):
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\

        df.fillna('', inplace=True)

        # group_1_data = [item for item in associatedStrings if item['group'] == 1]
        # group_2_data = [item for item in associatedStrings if item['group'] == 2]
        
        group_1_data = []
        group_2_data = []
        
        for item in associatedStrings:
            if item['group'] == 1:
                group_1_data.append(item)
            elif item['group'] == 2:
                group_2_data.append(item)
                

        def check_conditions(s, group_data):
            if not group_data:
                return True
            
            include_strings = []
            exclude_strings = []

            for item in group_data:
                keyword = item['keyword'].lower()
                if item['includeFlag'] == 1:
                    include_strings.append(keyword)
                elif item['includeFlag'] == 0:
                    exclude_strings.append(keyword)
            
            # include_strings = [item['keyword'].lower() for item in group_data if item['includeFlag'] == 1]
            # exclude_strings = [item['keyword'].lower() for item in group_data if item['includeFlag'] == 0]

            s_lower = s.lower() 

            # return (not include_strings or any(keyword in s_lower for keyword in include_strings)) and not any(keyword in s_lower for keyword in exclude_strings)
                
            should_include = False
            should_exclude = False

            for keyword in include_strings:
                if keyword in s_lower:
                    should_include = True
                    break

            for keyword in exclude_strings:
                if keyword in s_lower:
                    should_exclude = True
                    break

            return should_include and not should_exclude

        df_filtered = df[df[Tag1].apply(lambda s: check_conditions(s, group_1_data)) & df[Tag2].apply(lambda s: check_conditions(s, group_2_data))]

        # print(df_filtered)

        
        if len(df_filtered)<1:
            #return results
            results = []

            
        

        for ind, current in df_filtered.iterrows():
            
        
        # if len(df_filtered)>0:
            result = {"highlights": [], "error": "", "checkGeneratedForList": []}
            result["error"] = CheckDescription
            result["highlights"].append({"section": "statement","row": {"name": df_filtered["tag"], "id": df_filtered["filingId"]},"cell": {"peo": df_filtered["primaryPeriodEndDate"],"scale": df_filtered["scaleName"],"value": df_filtered["value"],"currency": df_filtered["currency"],"fpo": ""},"filingId": filingMetadata["metadata"]["filingId"]})
            result["checkGeneratedFor"] = {"statement": "statement","tag": df_filtered["tag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": df_filtered["primaryPeriodEndDate"],"fpo": "","diff": ""}
            result["checkGeneratedForList"].append({"statement": "statement","tag": df_filtered["tag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": df_filtered["primaryPeriodEndDate"],"fpo": "","diff": ""})
            results.append(result)

    return results





        # df['int_column'] = df['string_column'].apply(lambda x: int(x) if x != 'null' else np.nan)
        # Filter data based on group 1 related strings in "capitalStructureCleanedDescription"
        # group_1_include_strings = [item['keyword'] for item in associatedStrings if item['group'] == 1 and item['includeFlag'] == 1]
        # group_1_exclude_strings = [item['keyword'] for item in associatedStrings if item['group'] == 1 and item['includeFlag'] == 0]

        # group_2_include_strings = [item['keyword'] for item in associatedStrings if item['group'] == 2 and item['includeFlag'] == 1]
        # group_2_exclude_strings = [item['keyword'] for item in associatedStrings if item['group'] == 2 and item['includeFlag'] == 0]


        # df_group_1_filtered = df[df[Tag1].apply(
        #     lambda s: any(keyword in s for keyword in group_1_include_strings) and
        #             not any(keyword in s for keyword in group_1_exclude_strings)
        # )]


        # if len(group_2_include_strings)<1:
        #     df_group_2_filtered = df_group_1_filtered[df_group_1_filtered[Tag2].apply(
        #         lambda s: not any(keyword in s for keyword in group_2_exclude_strings))]
                
        # else:
        #     df_group_2_filtered = df_group_1_filtered[df_group_1_filtered[Tag2].apply(
        #         lambda s: any(keyword in s for keyword in group_2_include_strings) and
        #                 not any(keyword in s for keyword in group_2_exclude_strings)
        #     )]

        #print(df_group_2_filtered)




if __name__ == "__main__":
    start_time = time.time()
    results1 = DCS1(historicalData, filingMetadata, extractedData, parameters,df)
    print(results1)
    print("Total time: ", time.time() - start_time)

    