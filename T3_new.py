
import json
import time

with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8") as f:
  data = json.load(f)

extractedData=data['extractedData']
historicalData=data["historicalData"]
filingMetadata=data["filingMetadata"]

parameters = { "LHSTags": [ { "value": "DBUD" } ], "StatementType": [ { "value": "Cashflow" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": " 568" }], "CheckDescription": [ { "value": "Tag DBUD (Unamortized Discount -Total) has Positive value pl. check" } ] }

def T3(historicalData,filingMetadata,extractedData,parameters):
    
    import datetime
    import math
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


    industry_inc=get_param_value(parameters,'INDUSTRY_INCLUDE')
    industry_exc=get_param_value(parameters,'INDUSTRY_EXCLUDE')
    template=get_param_value(parameters,'template')
    gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
    gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
    country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
    country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
    AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
    PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
    Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
    SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
    MBSflag=get_param_value(parameters,'Main/Breakup/All')
    if MBSflag=="":
        MBSflag='All'
    stmt=parameters['StatementType'][0]['value']
    lhs_tags1=parameters['LHSTags'][0]['value'].split(',')
    lhs_tags=[]
    for tg in lhs_tags1:
        lhs_tags.append(str(tg))
        lhs_tags.append(str(tg)+"#CV")
        lhs_tags.append(str(tg)+"#IL")
    print(lhs_tags)    
    rhs_tags=[]
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
    filingid=filingMetadata['metadata']['filingId']
    #validating parameter values

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


        def getparent(par):
            pe=[]
            me=[]
            for p in par['childRows']:
                for v in par['childRows'][p]['values']:
                    if v['value']!=0 or v['value']!="":
                        pe.append(v['peo']+'_'+v['fpo'])
            
            for v in par['values']:
                if v['value']!=0 or v['value']!="":
                    me.append(v['peo']+'_'+v['fpo'])
            
            return (list(set(me).difference(pe)))

        cons=[]
        for key in extractedData.keys():
            for ins in extractedData[key].keys():
                if extractedData[key][ins]['section']==stmt:
                    if 'childRows' in extractedData[key][ins].keys() and ('Breakup Only' in MBSflag or 'All' in MBSflag):
                        for chins in extractedData[key][ins]['childRows'].keys():
                            if extractedData[key][ins]['childRows'][chins]['tag'] in lhs_tags or extractedData[key][ins]['childRows'][chins]['tag'] in rhs_tags:
                                for v in extractedData[key][ins]['childRows'][chins]['values']:
                                    if v['value']!='0' and  v['value']!="":
                                        cons.append({'inst':chins, 'Tag': extractedData[key][ins]['childRows'][chins]['tag'], 'Description': extractedData[key][ins]['childRows'][chins]['description'],'Displayorder':extractedData[key][ins]['childRows'][chins]['displayOrderId'],'PEO':v['peo'],'FPO':v['fpo'], 'Value':float(v['value']),'Scale':v['scale'],'Currency':v['currency'],'isChild':'Child','ParentInst':"",'consider':1})

                    if 'childRows' not in extractedData[key][ins].keys() and ('Main Only' in MBSflag or 'All' in MBSflag) and extractedData[key][ins]['isChildRow'] is False:
                        if extractedData[key][ins]['tag'] in lhs_tags or extractedData[key][ins]['tag'] in rhs_tags:
                            for v in extractedData[key][ins]['values']:
                                if v['value']!='0' and  v['value']!="":
                                    cons.append({'inst':ins, 'Tag': extractedData[key][ins]['tag'], 'Description': extractedData[key][ins]['description'],'Displayorder':extractedData[key][ins]['displayOrderId'],'PEO':v['peo'],'FPO':v['fpo'],'Value':float(v['value']),'Scale':v['scale'],'Currency':v['currency'],'isChild':'Main','ParentInst':"",'consider':1})


                    if 'childRows' in extractedData[key][ins].keys() and ('Main Only' in MBSflag):
                        if extractedData[key][ins]['tag'] in lhs_tags or extractedData[key][ins]['tag'] in rhs_tags:
                            for v in extractedData[key][ins]['values']:
                                if v['value']!='0' and  v['value']!="":
                                    cons.append({'inst':ins, 'Tag': extractedData[key][ins]['tag'], 'Description': extractedData[key][ins]['description'],'Displayorder':extractedData[key][ins]['displayOrderId'],'PEO':v['peo'],'FPO':v['fpo'], 'Value':float(v['value']),'Scale':v['scale'],'Currency':v['currency'],'isChild':'Parent','ParentInst':"",'consider':1})


                    if 'childRows' in extractedData[key][ins].keys() and ('All' in MBSflag):
                        miss=getparent(extractedData[key][ins])
                        if extractedData[key][ins]['tag'] in lhs_tags or extractedData[key][ins]['tag'] in rhs_tags:
                            for v in extractedData[key][ins]['values']:
                                if (v['value']!='0' and  v['value']!="") and  v['peo']+"_"+v['fpo'] in miss:
                                    cons.append({'inst':ins, 'Tag': extractedData[key][ins]['tag'], 'Description': extractedData[key][ins]['description'],'Displayorder':extractedData[key][ins]['displayOrderId'],'PEO':v['peo'],'FPO':v['fpo'], 'Value':float(v['value']),'Scale':v['scale'],'Currency':v['currency'],'isChild':'Parent','ParentInst':"",'consider':1})
        st_peo=[]
        for ins in filingMetadata['sections'][stmt].keys():
            for peo in filingMetadata['sections'][stmt][ins]['peos'].keys():
                st_peo.append(filingMetadata['sections'][stmt][ins]['peos'][peo]['peo']+'_'+str(filingMetadata['sections'][stmt][ins]['peos'][peo]['fpo'])) 
        high1={}
        gene={}
        if len(cons)>0:
            for pe in st_peo:
                for con in cons:
                    if float(con['Value'])>0 and con['PEO']+"_"+con['FPO']==pe:
                        high1.setdefault(pe,[]).append({"section": stmt, "row": {"name": con['Tag'], "id": con['inst']}, "cell": {'fpo':con['FPO'],'peo':con['PEO'],'value':con['Value'],'currency':con['Currency'],'scale':con['Scale']}, "filingId": filingid})
                        gene.setdefault(pe,[]).append({"statement": stmt, "tag": str(', '.join(lhs_tags1)), "description": "", "refFilingId": "","filingId": filingid,"objectId": "", "peo": con['PEO'],"fpo": con['FPO'], "diff":""})

        if len(high1)>0:
            for h in high1.keys():
                a,b=h.split("_")
                err=str(lhs_tags1[0]) +' Tag value is positive, please check'
                result={"highlights": [], "error": ""}
                result["error"] = err
                result["highlights"]=high1[h]
                result["checkGeneratedFor"]={"statement": stmt, "tag":str(', '.join(lhs_tags1)) , "description": "", "refFilingId": "","filingId": filingid,"objectId": "", "peo": a,"fpo": b, "diff": ""}
                result["checkGeneratedForList"]=gene[h]
                results.append(result)  
                        
    # err=' Tag value is in Positive, please check'
    # result={"highlights": [], "error": ""}
    # result["error"] = err
    # results.append(result) 
    return results



   
if __name__ == '__main__':
  start_time = time.time()
  results1 = T3(historicalData,filingMetadata,extractedData,parameters)
  print(results1)
  print('Total time: ', time.time() - start_time)
    
