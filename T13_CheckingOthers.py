#%%

import json
import time

with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8") as f:
    data = json.load(f)

extractedData=data['extractedData']
historicalData=data["historicalData"]
#filingMetadata=json.loads(data["filingMetadata"])
filingMetadata=data["filingMetadata"]
associatedStrings=[{ "id": 860, "group": 1, "includeFlag": 0, "keyword": ".*bond.*debentur.*note.*payable.*"},
                   { "id": 861, "group": 1, "includeFlag": 1, "keyword": ".*Premium.*premium.*unamortise.*Unamortized.*"}]
parameters = { "LHSTags": [ { "value": "DBUP" } ], 
            #   "ExcludeDataitemTag": [ { "value": "SUBIS" }, { "value": "HDIS" }, { "value": "NI" } ], 
              "Main/Breakup/All": [ { "value": "Main Only" } ], 
              "StatementType": [ { "value": "Cashflow" } ], 
              "BlockId": [ { "value": " 859" }, { "value": " 13" }, { "value": " 3" }, { "value": "113" }, { "value": " 1186" }, { "value": " 1185" }, { "value": " 213" } ], 
              "CheckDescription": [ { "value": "Line item description having the strings related to tag GLSA but tag given is other than GLSA (Gain or Loss on sale of Asset)" } ] }

def T13(historicalData,filingMetadata,extractedData,parameters):

    import datetime
    #reading parameter values
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
    block_id1=get_param_value(parameters,'BlockId')
    template=get_param_id(parameters,'Template')
    gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
    gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
    country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
    country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
    AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
    PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
    Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
    SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
    #lhs_tag=parameters['LHSTags'][0]['value'].split(',')
    lhs_tag=get_param_value(parameters,'LHSTags')
    lhs_tags=[]
    for tg in lhs_tag:
        lhs_tags.append(str(tg))
        lhs_tags.append(str(tg)+'#CV')
        lhs_tags.append(str(tg)+'#IL')
    exclude_tag=get_param_value(parameters,'ExcludeDataitemTag')
    exclude_tags=[]
    for tg in exclude_tag:
        exclude_tags.append(str(tg))
        exclude_tags.append(str(tg)+'#CV')
        exclude_tags.append(str(tg)+'#IL')
        
    MBSflag=get_param_value(parameters,'Main/Breakup/All')
    if MBSflag=="":
        MBSflag='All'
    stmt=parameters['StatementType'][0]['value']

    block_id=[]
    if len(block_id1)>0:
        for ele in block_id1:
            block_id.append(ele.strip())


    if AsReportedPeriodEndDate!="":
        ARoperator=AsReportedPeriodEndDate[0]
        AsReportedPeriodEndDate=AsReportedPeriodEndDate[1:]


    def check_period(ARoperator):
        if ARoperator=='>':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')>datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        elif ARoperator=='<':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')<datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        elif ARoperator=='=':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')==datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        else:
            ret='True'
        return ret

    results=[]
    high=[]
    CTC=[]
    tag=get_param_value(parameters,'LHSTags')
    filingid=filingMetadata['metadata']['filingId']
    #validating parameter values

    if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
    and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
    and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
    and (len(country_inc)==0 or filingMetadata["metadata"]["countryCode"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["countryCode"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminayflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys()))))\
    and len(tag)!=0:
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\	

        import re

        inc={}
        exc={}
        for st in associatedStrings:
            if st['includeFlag']==1:
                if st['group'] in inc.keys():
                    inc[st['group']].append(st['keyword'])
                else:
                    inc[st['group']]=[st['keyword']]
            if st['includeFlag']==0:
                if st['group'] in exc.keys():
                    exc[st['group']].append(st['keyword'])
                else:
                    exc[st['group']]=[st['keyword']]
                

        line_items=[]
        high1=[]
        high=[]
        
        for key in extractedData.keys():
            for ins in extractedData[key].keys():
                if extractedData[key][ins]['section']==stmt:
                    if 'childRows' in extractedData[key][ins].keys() and ('Breakup Only' in MBSflag or 'All' in MBSflag):
                        for chins in extractedData[key][ins]['childRows'].keys():
                            if extractedData[key][ins]['childRows'][chins]['tag'] not in lhs_tags and extractedData[key][ins]['childRows'][chins]['tag'] not in exclude_tags and str(extractedData[key][ins]['childRows'][chins]['blockId']) in block_id:
                                line_items.append(extractedData[key][ins]['childRows'][chins]['description'])
                                high1.append({'tag':extractedData[key][ins]['childRows'][chins]['tag'],'description':extractedData[key][ins]['childRows'][chins]['description'],'ins':chins})

                    if 'childRows' not in extractedData[key][ins].keys() and ('Main Only' in MBSflag or 'All' in MBSflag) and extractedData[key][ins]['isChildRow'] is False:
                        if extractedData[key][ins]['tag'] not in lhs_tags and extractedData[key][ins]['tag'] not in exclude_tags and str(extractedData[key][ins]['blockId']) in block_id:
                            line_items.append(extractedData[key][ins]['description'])
                            high1.append({'tag':extractedData[key][ins]['tag'],'description':extractedData[key][ins]['description'],'ins':ins})



        line_items_exc_match=[]
        for line in line_items:
            for st in exc.keys():
                t=len(exc[st])
                i=0
                for s in exc[st]:
                    if re.search(s.lower(),line.lower()):
                        i=i+1
                if i!=0 and i==t:
                    line_items_exc_match.append(line)
                
        if len(line_items_exc_match)>0:
            for l in line_items_exc_match:
                if l in line_items:
                    line_items.remove(l)
            
            
        line_items_inc_match=[]
        for line in line_items:
            for st in inc.keys():
                t=len(inc[st])
                i=0
                for s in inc[st]:
                    if re.search(s.lower(),line.lower()):
                        i=i+1
                if i!=0 and i==t:
                    line_items_inc_match.append(line)


        #high=[]
        CTC=[]
        for h in high1:
            if h['description'] in line_items_inc_match:
                high.append({"section": stmt, "row": {"name": h['tag'], "id": h['ins']}, "filingId": int(filingMetadata['metadata']['filingId'])})
                CTC.append({"statement": stmt, "tag": h['tag'], "description": h['description'],"filingId": filingid})

    if len(high)>0:
        err="Line item description having the strings related to tag" + str(lhs_tag) +" but given is other than the tag" 
        result={"highlights": [],"error":""}
        result["highlights"]=high     
        result["error"] = err
        result["checkGeneratedFor"]={"statement": stmt, "tag": str(lhs_tag), "description": "", "refFilingId": "","filingId": int(filingid),"objectId": "", "peo": "","fpo": "", "diff": ""}
        result["checkGeneratedForList"]=CTC
        results.append(result)
    # err=str(line_items)
    # result={"highlights": [],"error":""}



    return results    

    
   
if __name__ == '__main__':
  start_time = time.time()
  results1 = T13(historicalData,filingMetadata,extractedData,parameters)
  print(results1)
  print('Total time: ', time.time() - start_time)
    

# %%
