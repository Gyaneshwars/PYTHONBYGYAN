import json
import time

# with open('Downloads\gaaptesting.json',encoding="utf8") as f:
#   data = json.load(f)
# C:\Users\nseelam\OneDrive - S&P Global\Desktop\Checks Debugging\extracteddata (2).json
with open(
    "C:\\Users\\gsravane\\Downloads\\T13 (1).json",
    encoding="utf8",
) as f:
    data = json.load(f)

extractedData = data["extractedData"]
historicalData = data["historicalData"]
filingMetadata = data["filingMetadata"]

#parameters = { "LHSTags": [ { "value": "MCLC" } ], "RHSTags": [ { "value": "LCX" } ], "ValueType": [ { "id": 1, "value": "Percentage" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": "568" } ], "StatementType": [ { "value": "DCS" } ], "FlagType": [ { "id": 14, "value": "IgnoreObjectIdFlag(Auto-Generated)" } ], "MultiplicationFactor": [ { "value": "{'MCLC':1,'LCX':1}" } ], "Value": [ { "value": "100" } ], "Operation": [ { "value": "<" } ], "CheckDescription": [ { "value": "Tag MCLC 1 to N (Facility for letter of Credit) < LCX 1 to N (Stand alone Outstanding Letter of credit)" }] }
#parameters = { "LHSTags": [ { "value": "MCLC" } ], "RHSTags": [ { "value": "DBLC" } ], "ValueType": [ { "id": 1, "value": "Percentage" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": "568" } ], "StatementType": [ { "value": "DCS" } ], "FlagType": [ { "id": 14, "value": "IgnoreObjectIdFlag(Auto-Generated)" } ], "MultiplicationFactor": [ { "value": "{'DBLC':1,'MCLC':1}" } ], "Value": [ { "value": "100" } ], "Operation": [ { "value": "<" } ], "CheckDescription": [ { "value": "Tag MCLC 1 to N (Facility for Letters of Credit) < DBLC 1 to N (Letters of credit)" }] }
#parameters = { "LHSTags": [ { "value": "MCRC" } ], "RHSTags": [ { "value": "LCRC" },{ "value": "CPRC" },{ "value": "DBRC" } ], "ValueType": [ { "id": 1, "value": "Percentage" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": "568" } ], "StatementType": [ { "value": "DCS" } ], "FlagType": [ { "id": 14, "value": "IgnoreObjectIdFlag(Auto-Generated)" } ], "MultiplicationFactor": [ { "value": "{'DBRC':1,'MCRC':1,'LCRC':1,'CPRC':1}" } ], "Value": [ { "value": "100" } ], "Operation": [ { "value": "<" } ], "CheckDescription": [ { "value": "Tag MCRC 1 to N (Maximum Credit Facility for Revolving Credi) < DBRC/CPRC (Commercial Paper Outstanding in a Revolving Credit Facility)" }] }
#parameters = { "LHSTags": [ { "value": "MCSF" } ], "RHSTags": [ { "value": "DBSF" } ], "ValueType": [ { "id": 1, "value": "Percentage" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": "568" } ], "StatementType": [ { "value": "DCS" } ], "FlagType": [ { "id": 14, "value": "IgnoreObjectIdFlag(Auto-Generated)" } ], "MultiplicationFactor": [ { "value": "{'DBSF':1,'MCSF':1}" } ], "Value": [ { "value": "100" } ], "Operation": [ { "value": "<" } ], "CheckDescription": [ { "value": "Tag MCSF (Maximum Credit Facility for Securitization Facility) < DBSF (Outstanding Securitization Facility) o pl. check)" }] }
parameters = { "LHSTags": [ { "value": "LCX" } ], "RHSTags": [ { "value": "UDLC" } ], "ValueType": [ { "id": 1, "value": "Percentage" } ], "Main/Breakup/All": [ { "value": "All" } ], "BlockId": [ { "value": "568" } ], "StatementType": [ { "value": "DCS" } ], "FlagType": [ { "id": 14, "value": "IgnoreObjectIdFlag(Auto-Generated)" } ], "MultiplicationFactor": [ { "value": "{'LCX':1,'UDLC':1}" } ], "Value": [ { "value": "" } ], "Operation": [ { "value": "==" } ], "CheckDescription": [ { "value": "Find cases where sum of LCX=UDLC" }] }


def T8(historicalData, filingMetadata, extractedData, parameters):
    
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
                param_id.append(pam.get('value'))
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
    #MBSflag=get_param_value(parameters,'Main/Breakup/All')
    #if MBSflag=="":
        #         MBSflag='All'
    #stmt=parameters['StatementType'][0]['value']
    #lhs_tags=parameters['LHSTags'][0]['value'].split(',')
    #rhs_tags=[]
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

    #filingid=filingMetadata['metadata']['filingId']
    import ast

    errors=[]    
    results=[]
    LHStags=[]
    RHStags =[]
    #validating parameter values
    LHStags1=get_param_value(parameters,'LHSTags')
    #LHStags=[]
    #RHStags =[]
    RHStags1=get_param_value(parameters,'RHSTags')
    mul_fact1=ast.literal_eval(parameters.get('MultiplicationFactor')[0].get('value'))
    
    mul_fact={}
    OPERATOR=parameters.get('Operation')[0].get('value')
    #OPERATOR='!='
    #print(OPERATOR)
    stmt=parameters.get('StatementType')[0].get('value')
    Ctype= parameters.get('Main/Breakup/All')[0].get('value')
    crossstatement=get_param_value(parameters,'CrossStatement') 
    CheckDescription= parameters.get('CheckDescription')[0].get('value')

    for tag in LHStags1:
        LHStags.append(tag)
        LHStags.append(str(tag)+'#CV')
        LHStags.append(str(tag)+'#IL')
    for tag in RHStags1:
        RHStags.append(tag)
        RHStags.append(str(tag)+'#CV')
        RHStags.append(str(tag)+'#IL')

    for k,v in mul_fact1.items():
        mul_fact[k]=v
        mul_fact[k+'#CV']=v
        mul_fact[k+'#IL']=v
    if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
    and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
    and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
    and (len(country_inc)==0 or filingMetadata["metadata"]["classification"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["classification"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys())))):
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\

        import datetime
        checkmeantfortag = LHStags1+RHStags1
        #print(checkmeantfortag)
        errors = []
        finaldataset = {}
        summaryVal = {}
        maind = extractedData
        processed = []
        actualvalue = 0
        parameters= LHStags + RHStags
        #print(parameters)

        def executeoperator(lhsval,rhsval,operator):
            if (operator == "!="):
                return lhsval != rhsval
            elif (operator == ">"):
                return lhsval > rhsval
            elif (operator == "<"):
                return lhsval < rhsval
            elif (operator == "=="):
                return lhsval == rhsval
                
            else:
                return True

        def comparevalueswithfpo(lhsvalues,rhsvalues):
            results = {}
            for lhsval in lhsvalues:    
                for rhsval in rhsvalues:
                    
                    if (lhsval[0] == rhsval[0] and lhsval[1] == rhsval[1]):
                        if (executeoperator(lhsval[2],rhsval[2],OPERATOR)):
                            
                            higvalues=[]
                            result={"highlights": [], "error": ""}
                            result["error"] = CheckDescription

                            highvalues = gethighvalueswithfpo(lhsval[0],lhsval[1])
                            for item in highvalues:
                                result["highlights"].append({"section": item[2], "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                result["checkGeneratedFor"]={"statement": item[2], "tag": str(checkmeantfortag)[1:-1], "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": "" }
                            errors.append(result)
            
        def gethighvalueswithfpo(withpeo, withfpo):
            reslist = []
            for listitems in summaryVal.values():
                for listitem in listitems:
                    for tagkey in listitem[4].keys():
                        for instancekey in listitem[4][tagkey].keys():
                            for itemvalue in listitem[4][tagkey][instancekey]:
                                if (int(itemvalue['fpo']) == withfpo and itemvalue['peo'] == withpeo):
                                    reslist.append( list((tagkey,instancekey,listitem[3],itemvalue)) )
            return reslist

        def comparevaluesnofpo(lhsvalues,rhsvalues):
            results = {}
            for lhsval in lhsvalues:    
                for rhsval in rhsvalues:
                    if (lhsval[0] == rhsval[0]):
                        if (executeoperator(lhsval[2],rhsval[2],OPERATOR)):
                            higvalues=[]
                            result={"highlights": [], "error": ""}
                            result["error"] = CheckDescription

                            highvalues = gethighvaluesnofpo(lhsval[0],lhsval[1])
                            for item in highvalues:
                                result["highlights"].append({"section": item[2], "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                result["checkGeneratedFor"]={"statement": item[2], "tag": str(checkmeantfortag)[1:-1], "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": "" }
                            errors.append(result)

        def gethighvaluesnofpo(withpeo, withfpo):
            reslist = []
            for listitems in summaryVal.values():
                for listitem in listitems:
                    for tagkey in listitem[4].keys():
                        for instancekey in listitem[4][tagkey].keys():
                            for itemvalue in listitem[4][tagkey][instancekey]:
                                if (itemvalue['peo'] == withpeo):
                                    reslist.append( list((tagkey,instancekey,listitem[3],itemvalue)) )
            return reslist


        def getvalueindex(peoval,fpoval,conlist):
            index = 0
            for item in conlist:
                if (item[0] == peoval and item[1] == fpoval):
                    return index
                index = index + 1
            return -1

        def getactualvalue(value,scale):
            if (scale.lower() == 'actual'):
                return float(value)
            elif (scale.lower() == 'thousand'):
                return float(value) * 1000
            elif (scale.lower() == 'million'):
                return float(value) * 1000000
            elif (scale.lower() == 'billion'):
                return float(value) * 1000000000
            elif (scale.lower() == 'trillion'):
                return float(value) * 1000000000000
            elif (scale.lower() == 'tenth'):
                return float(value) * 0.1
            elif (scale.lower() == 'hundredth'):
                return float(value) * 0.01
            elif (scale.lower() == 'thousandth'):
                return float(value) * 0.001
            elif (scale.lower() == 'tenthousandth'):
                return float(value) * 0.0001
            elif (scale.lower() == 'dozen'):
                return float(value) * 12
            elif (scale.lower() == 'hundred'):
                return float(value) * 100  
            elif (scale.lower() == 'lakh'):
                return float(value) * 100000
            elif (scale.lower() == 'crore'):
                return float(value) * 10000000
            elif (scale.lower() == 'bit'):
                return float(value) * 12.5
            elif (scale.lower() == 'score'):
                return float(value) * 20
            elif (scale.lower() == 'half'):
                return float(value) * 0.5
            elif (scale.lower() == 'pair'):
                return float(value) * 2
            elif (scale.lower() == 'gross'):
                return float(value) * 144
            elif (scale.lower() == 'ten'):
                return float(value) * 10
            elif (scale.lower() == 'myriad'):
                return float(value) * 10000
            elif (scale.lower() == 'millionth'):
                return float(value) * 0.000001  
            elif (scale.lower() == 'billionth'):
                return float(value) * 0.000000001
            elif (scale.lower() == 'percentage'):
                return float(value) * 100
            elif (scale.lower() == 'fivehundred'):
                return float(value) * 500
            elif (scale.lower() == 'hundredmillion'):
                return float(value) * 100000000          
            else:
                return 0

        def gettagvalue(tag):
            for t in mul_fact.keys():
                if (tag == t):
                    return mul_fact[t]

        def istagexists(summaryVal,tags):
            for t in summaryVal.keys():
                for tag in tags:
                    if (t == tag):
                        return True
            return False

        def getruntimevalues(tags):    
            finalvalues = []
            for k in tags:
                if (k not in summaryVal.keys()):
                    continue
                tagvalue =  gettagvalue(k)
                for item1 in summaryVal[k]:
                    findIndex = getvalueindex(item1[0],int(item1[1]),finalvalues)
                    if (findIndex >= 0):
                        finalvalues[findIndex][2] = finalvalues[findIndex][2] + ( item1[2] * tagvalue)
                    else:      
                        finalvalues.append( list( (item1[0], int(item1[1]), item1[2] * tagvalue )) ) 
            return finalvalues

        def keyexists(values,outerkey,innerkey,isouter):
            for k in values.keys():
                if (outerkey == k):
                    if (isouter):
                        return True
                else:
                    for k1 in values[outerkey].keys():
                        if (innerkey == k1):
                            return True
            return False

        def prepareresult(peo,fpo,outertag,innertag,value,templist):
            validations = {}
            index = 0
            for i in range(len(templist)):
                item = templist[i]
                if (item[0] == peo and item[1] == fpo):
                    validations = item[4]
                    break
                index = index + 1

            if (len(validations) > 0 and len(validations.keys()) > 0):
                outerexists = keyexists(validations,outertag,innertag,True)
                innerexists = keyexists(validations,outertag,innertag,False)

                if (outerexists and not innerexists):
                    templist[index][4][outertag][innertag] = [value]
                elif (outerexists and innerexists):
                    templist[index][4][outertag][innertag].append(value)
                elif (not outerexists and not innerexists):
                    templist[index][4] = {outertag: {innertag: [value]}}
            else:
                templist[index][4] = {outertag: {innertag: [value]}}

            return templist

        def getfinalvalue(childict,fpo,peo,summary):
            templist = []
            for instancekey in childict.keys():
                templist = []
                tempdict = childict[instancekey]
                childtag = tempdict['tag']
                childsection =  tempdict['section']
                if (childtag in LHStags or childtag in RHStags):
                    if (childtag in  summary.keys()):
                        templist = summary[childtag]
                    tempvalues = tempdict.get('values')
                    for item in tempvalues:
                        if (item['value'] == ''):
                            continue
                        if (item['peo'] == peo and item['fpo'] == fpo):
                            index = getvalueindex(item['peo'],item['fpo'],templist)
                            value = getactualvalue(item['value'],item['scale'])
                            if (index >= 0):
                                templist[index][2] = templist[index][2] + value
                            else:
                                templist.append( list( (item['peo'], item['fpo'],value,childsection, {} )) )
                            templist = prepareresult(peo,fpo,childtag,instancekey,item,templist)
                            summary[childtag] = templist
            return summary



    # checking if in the childrows, the given peo,fpo combination exists
        def childexists(fpo,peo,childdata):
            for childvalue in childdata.values():
                for val in childvalue.get('values'):
                    if (val['fpo'] == fpo and val['peo'] == peo):
                        return True
            return False


        def preparemainds(extdata):
            summary = {}
            for tagkey in extdata.keys(): #maind
                for keytag in extdata[tagkey].items():
                    conlist = []
                    innertag = keytag[0]
                    keytagdict = dict(keytag[1])
                    tag = keytagdict.get('tag')
                    section = keytagdict.get('section')
                    ischild = keytagdict.get('isChildRow')
                    for item in keytagdict.get('values'):
                        if (tag in summary):
                            conlist = summary[str(tag)]
                        if (item['value'] == '' or item['value'] == '0' or item['value'] == '0.00'):
                            continue
                        if (ischild == True):
                            continue
                        if (Ctype == 'All' \
                            and keytagdict.get('childRows') \
                            and childexists(item['fpo'],item['peo'],keytagdict.get('childRows'))):
                            summary = getfinalvalue(keytagdict.get('childRows'),item['fpo'],item['peo'],summary)
                        elif (Ctype == 'Breakup' \
                            and keytagdict.get('childRows')):
                            summary = getfinalvalue(keytagdict.get('childRows'),item['fpo'],item['peo'],summary)
                        elif (Ctype == 'All' or Ctype == 'Main Only'): 
                            if ( tag in LHStags or tag in RHStags):
                                findIndex = getvalueindex(item['peo'],item['fpo'],conlist)
                                actualvalue = getactualvalue(item['value'],item['scale'])
                                if (findIndex >= 0):
                                    conlist[findIndex][2] = conlist[findIndex][2] + actualvalue
                                else:          
                                    conlist.append( list( (item['peo'], item['fpo'],actualvalue,section, {} )) )
                                conlist = prepareresult(item['peo'],item['fpo'],tag,innertag,item,conlist)                
                                summary[tag] = conlist   
            return summary

        summaryVal = preparemainds(maind)
        
        # if ( not ( len(LHStags) > 0 and istagexists(summaryVal,LHStags) and len(RHStags) > 0 and istagexists(summaryVal,RHStags) ) ):
        #     return errors

        lhsvalues = getruntimevalues(LHStags)
        rhsvalues = getruntimevalues(RHStags)

        if len(crossstatement)>0:
            comparevaluesnofpo(lhsvalues,rhsvalues)
        else:
            comparevalueswithfpo(lhsvalues,rhsvalues)


    return errors


if __name__ == "__main__":
    start_time = time.time()
    results1 = T8(historicalData, filingMetadata, extractedData, parameters)
    print(results1)
    print("Total time: ", time.time() - start_time)

    
