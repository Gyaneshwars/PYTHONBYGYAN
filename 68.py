import json
import os
from datetime import datetime
import pandas as pd
import time


with open(
    "C:\\Users\\nseelam\\OneDrive - S&P Global\\Desktop\\From DT\\GitHub\\fundamentals-error-checks\\source\\Naveen\\T7\\T7.json",
    encoding="utf8",
) as f:
    data = json.load(f)

extractedData = data["extractedData"]
historicalData = data["historicalData"]
filingMetadata = data["filingMetadata"]

associatedStrings = [{ "id": 17050, "group": 1, "includeFlag": 0, "keyword": "No" }]

df=pd.read_excel(r"C:\Users\nseelam\OneDrive - S&P Global\Desktop\From DT\GitHub\fundamentals-error-checks\source\Naveen\Debtprofiling\DCS_Data.xlsx")

parameters = {
              "Tag1": [ { "value": "capitalStructureIsSecuredTypeName" } ],
              "LHSTags": [ { "value": "DBTP" } ],
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
    print(Tag1)

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

        group_1_data = [item for item in associatedStrings if item['group'] == 1]

        def check_conditions(s, group_data):
            if not group_data:
                return True
            
            include_strings = [item['keyword'].lower() for item in group_data if item['includeFlag'] == 1]
            exclude_strings = [item['keyword'].lower() for item in group_data if item['includeFlag'] == 0]

            s_lower = s.lower()

            return (not include_strings or any(keyword in s_lower for keyword in include_strings)) and \
                not any(keyword in s_lower for keyword in exclude_strings)

        df_filtered = df[(df["dataItemTag"].isin(LHStag))  & df[Tag1].apply(lambda s: check_conditions(s, group_1_data))]

        print(df_filtered)


        if len(df_filtered)<1:
            return results

        for ind, current in df_filtered.iterrows():
            result = {"highlights": [], "error": "", "checkGeneratedForList": []}
            result["error"] = CheckDescription
            result["highlights"].append({"section": "statement","row": {"name": current["dataItemTag"], "id": current["filingDataItemId"]},"cell": {"peo": current["primaryPeriodEndDate"],"scale": current["scaleName"],"value": current["dataitemvalue"],"currency": current["currencyid"],"fpo": ""},"filingId": filingMetadata["metadata"]["filingId"]})
            result["checkGeneratedFor"] = {"statement": "statement","tag": current["dataItemTag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": current["primaryPeriodEndDate"],"fpo": "","diff": ""}
            result["checkGeneratedForList"].append({"statement": "statement","tag": current["dataItemTag"],"description": "","refFilingId": "","filingId": filingMetadata["metadata"]["filingId"],"objectId": "","peo": current["primaryPeriodEndDate"],"fpo": "","diff": ""})
            results.append(result)
    return results





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

    
