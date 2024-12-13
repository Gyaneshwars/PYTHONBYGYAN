import json
import time
from decimal import Decimal

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

# parameters = { "LHSTags": [ { "value": "DBCL" } ], "RHSTags": [ { "value": "CL1" }, { "value": "CAPL" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "23887" } ], "StatementType": [ { "value": "DCS" } ,{ "value": "Supple" }], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "DBCL<>CL1+CAPL" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBCL':1,'CL1':1,'CAPL':1}" } ], "Operation": [ { "value": "<>" } ] }	

# parameters = { "LHSTags": [ { "value": "DBCL" } ], "RHSTags": [ { "value": "PVCL" }, { "value": "TOLO" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "23886" } ], "StatementType": [ { "value": "DCS" } ,{ "value": "Supple" }], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "DBCL<>PVCL" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'TOLO':1,'DBCL':1,'PVCL':1}" } ], "Operation": [ { "value": "<>" } ] }

# parameters = { "LHSTags": [ { "value": "MCBP" } ], "RHSTags": [ { "value": "DBBP" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "26089" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCBP < DBBP plz check" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBBP':1,'MCBP':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "MCBO" } ], "RHSTags": [ { "value": "DBBO" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "26088" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCBO < DBBO plz check" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBBO':1,'MCBO':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "MCNP" } ], "RHSTags": [ { "value": "DBNP" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "26078" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCNP 1 to N < DBNP 1 to N" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBNP':1,'MCNP':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "MCFH" } ], "RHSTags": [ { "value": "DBFH" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "26068" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCFH 1 to N < DBFH 1 to N" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBFH':1,'MCFH':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "MCTL" } ], "RHSTags": [ { "value": "DBTL" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "26058" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCTL 1 to N < DBTL 1 to N" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBTL':1,'MCTL':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "MCCP" } ], "RHSTags": [ { "value": "DBCP" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "25283" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCCP 1 to N < DBCP 1 to N" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBCP':1,'MCCP':1}" } ], "Operation": [ { "value": "<" } ] }

# parameters = { "LHSTags": [ { "value": "DBCL" } ], "RHSTags": [ { "value": "CCL" },{ "value": "LCL" },{ "value": "LLL" },{ "value": "CLL" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "25282" } ], "StatementType": [ { "value": "DCS" },{ "value": "Balancesheet" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "DBCL <> CCL + LCL+LLL+CLL" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBCL':1,'CCL':1,'LCL':1,'LLL':1,'CLL':1}" } ], "Operation": [ { "value": "<>" } ] }

parameters = { "LHSTags": [ { "value": "MCRC" } ], "RHSTags": [ { "value": "LCRC" },{ "value": "CPRC" },{ "value": "DBRC" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "97018" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "CheckDescription": [ { "value": "MCRC < Sum of DBRC,LCRC,CPRC plz. Check" } ], "Value": [ { "value": "100" } ], "MultiplicationFactor": [ { "value": "{'DBRC':1,'MCRC':1,'LCRC':1,'CPRC':1}" } ], "Operation": [ { "value": "<" } ] }



def T9(historicalData, filingMetadata, extractedData, parameters):

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
    template=get_param_id(parameters,'Template')
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
    errors = []
    results=[]
    #validating parameter values
    LHStags=[]
    RHStags =[]
    RHStags1=get_param_value(parameters,'RHSTags')
    LHStags1=get_param_value(parameters,'LHSTags')
    mul_fact1=ast.literal_eval(parameters.get('MultiplicationFactor')[0].get('value'))
    #print(mul_fact1)
    mul_fact={}
    OPERATOR=parameters.get('Operation')[0].get('value')
    #print(OPERATOR)
    CompulsoryTags=[]
    CompulsoryTags=get_param_value(parameters,'CompulsoryTags')
    crossstatement=get_param_value(parameters,'CrossStatement')
    ToleranceLimit=get_param_value(parameters,'ToleranceLimit')
    stmt=parameters.get('StatementType')[0].get('value')
    Ctype= parameters.get('Main/Breakup/All')[0].get('value')
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
    and (len(country_inc)==0 or filingMetadata["metadata"]["countryCode"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["countryCode"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys())))):
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\

        import datetime
        checkmeantfortag = LHStags1+RHStags1

        finaldataset = {}
        summaryVal = {}
        maind = extractedData
        processed = []
        actualvalue = 0
        parameters= LHStags + RHStags

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
                    print(LHStags1,RHStags1)
                    print(lhsval[0],rhsval[0])
                    print(lhsval[2],rhsval[2])
                    if (lhsval[0] == rhsval[0] and lhsval[1] == rhsval[1]):
                        if executeoperator(lhsval[2],rhsval[2],OPERATOR) and len(ToleranceLimit)>0:
                            try:
                                diffper = abs((rhsval[2]-lhsval[2])/rhsval[2])*100
                            except:
                                diffper = abs(rhsval[2]-lhsval[2])    
                            if diffper>float(ToleranceLimit[0]):    
                                diff=(lhsval[2]- rhsval[2])
                                result={"highlights": [], "error": ""}
                                result["error"] = CheckDescription
                                highvalues = gethighvalues(lhsval[0],lhsval[1])
                                for item in highvalues:
                                    result["highlights"].append({"section": stmt, "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                    result["checkGeneratedFor"]={"statement": stmt, "tag": str(checkmeantfortag), "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": diff }
                                errors.append(result)
                        elif (executeoperator(lhsval[2],rhsval[2],OPERATOR) and len(ToleranceLimit)<1):
                            higvalues=[]
                            diff=(lhsval[2]- rhsval[2])
                            result={"highlights": [], "error": ""}
                            result["error"] = CheckDescription
                            highvalues = gethighvalues(lhsval[0],lhsval[1])
                            for item in highvalues:
                                result["highlights"].append({"section": stmt, "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                result["checkGeneratedFor"]={"statement": stmt, "tag": str(checkmeantfortag), "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": diff }
                            errors.append(result)

        def comparevaluesnofpo(lhsvalues,rhsvalues):
            results = {}
            for lhsval in lhsvalues:    
                for rhsval in rhsvalues:
                    if (lhsval[0] == rhsval[0]):
                        if executeoperator(lhsval[2],rhsval[2],OPERATOR) and len(ToleranceLimit)>0:
                            try:
                                diffper = abs((rhsval[2]-lhsval[2])/rhsval[2])*100
                            except:
                                diffper = abs(rhsval[2]-lhsval[2])    
                            if diffper>float(ToleranceLimit[0]):    
                                diff=(lhsval[2]- rhsval[2])
                                result={"highlights": [], "error": ""}
                                result["error"] = CheckDescription
                                highvalues = gethighvaluesnofpo(lhsval[0],lhsval[1])
                                for item in highvalues:
                                    result["highlights"].append({"section": stmt, "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                    result["checkGeneratedFor"]={"statement": stmt, "tag": str(checkmeantfortag), "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": diff }
                                errors.append(result)
                        elif (executeoperator(lhsval[2],rhsval[2],OPERATOR) and len(ToleranceLimit)<1):
                            diff=(lhsval[2]- rhsval[2])
                            result={"highlights": [], "error": ""}
                            result["error"] = CheckDescription
                            highvalues = gethighvaluesnofpo(lhsval[0],lhsval[1])
                            for item in highvalues:
                                result["highlights"].append({"section": stmt, "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                result["checkGeneratedFor"]={"statement": stmt, "tag": str(checkmeantfortag), "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": "", "peo": lhsval[0],"fpo": lhsval[1], "diff": diff }
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


        def gethighvalues(withpeo, withfpo):
            reslist = []
            for listitems in summaryVal.values():
                for listitem in listitems:
                    for tagkey in listitem[4].keys():
                        for instancekey in listitem[4][tagkey].keys():
                            for itemvalue in listitem[4][tagkey][instancekey]:
                                if (int(itemvalue['fpo']) == withfpo and itemvalue['peo'] == withpeo):
                                    reslist.append( list((tagkey,instancekey,listitem[3],itemvalue)) )
            return reslist

        def getvalueindex(peoval,fpoval,conlist):
            index = 0
            for item in conlist:
                if (item[0] == peoval and item[1] == fpoval):
                    return index
                index = index + 1
            return -1

        def getactualvalue(value, scale):
            """
            Function to scale value based on the scales_dict
            """
            scales_dict = {'actual': 1, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000, 'trillion': 1000000000000, 'tenth': 0.1, \
                            'hundredth': 0.01, 'thousandth': 0.001, 'tenthousandth': 0.0001, 'dozen': 12, 'hundred': 100, 'lakh': 100000, \
                            'crore': 10000000, 'bit': 12.5, 'score': 20, 'half': 0.5, 'pair': 2, 'gross': 144, 'ten': 10, 'myriad': 10000, \
                            'millionth': 0.000001, 'billionth': 0.000000001, 'percentage': 100, 'fivehundred': 500, 'hundredmillion': 100000000, \
                            'myriad': 10000}
            try:
                if str(scale).lower() in scales_dict.keys():
                    return Decimal(value) * scales_dict[str(scale).lower()]
                else:
                    return 0
            except:
                return 0

        def getactualvalue1(value,scale):
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
            elif (scale.lower() == 'fiveHundred'):
                return float(value) * 500
            elif (scale.lower() == 'hundred Million'):
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



        def getfinalvalue(childict,fpo,peo):
            templist = [] 
            for instancekey in childict.keys(): 
                templist =[]
                tempdict = childict[instancekey]
                childtag = tempdict['tag']   
                childsection =  tempdict['section']
                if (childtag in LHStags or childtag in RHStags):
                    if (childtag in  summaryVal.keys()):
                        templist = summaryVal[childtag]
                    tempvalues = tempdict.get('values')
        # get instance values and update the final dataset for those tags
                    for item in tempvalues:
                        if (item['value'] == ''):
                            continue
                        if (item['peo'] == peo and item['fpo'] == fpo):
                            index = getvalueindex(item['peo'],item['fpo'],templist)
                            value = getactualvalue(item['value'],item['scale'])
                            if (index >= 0):
            # Updating an existing value for given tag, peo and fpo
                                templist[index][2] = templist[index][2] + value
                            else:          
            # if the tag is not found in result, it will insert the record
                                templist.append( list( (item['peo'], item['fpo'],value,childsection, {} )) )
                            templist = prepareresult(peo,fpo,childtag,instancekey,item,templist)
                            summaryVal[childtag] = templist

    # checking if in the childrows, the given peo,fpo combination exists
        def childexists(fpo,peo,childdata):
            for childvalue in childdata.values():
                for val in childvalue.get('values'):
                    if (val['fpo'] == fpo and val['peo'] == peo):
                        return True
            return False


        for tagkey in maind.keys():
            for keytag in maind[tagkey].items():    
                conlist = []
                innertag = keytag[0]
                keytagdict = dict(keytag[1])
                tag = keytagdict.get('tag')
                section = keytagdict.get('section')
                ischild = keytagdict.get('isChildRow')
                for item in keytagdict.get('values'):
                    if (tag in summaryVal):
                        conlist = summaryVal[str(tag)]
                    if (item['value'] == ''):
                        continue
                    if (ischild == True):
                        continue
        #if Mains Breakup and contain child rows and in child records fpo,peo combination exists
                    if (Ctype == 'All' and keytagdict.get('childRows') and childexists(item['fpo'],item['peo'],keytagdict.get('childRows'))):
                        getfinalvalue(keytagdict.get('childRows'),item['fpo'],item['peo'])
                    else: # else take the parent value if exists in lhs/rhs and update final dataset
                        if ( tag in LHStags or tag in RHStags ):
                            findIndex = getvalueindex(item['peo'],item['fpo'],conlist)
                            actualvalue = getactualvalue(item['value'],item['scale'])
                            if (findIndex >= 0):
            # Updating an existing value for given tag, peo and fpo
                                conlist[findIndex][2] = conlist[findIndex][2] + actualvalue        
                            else:          
            # if the tag is not found in result, it will insert the record
                                conlist.append( list( (item['peo'], item['fpo'],actualvalue,section, {} )) )

                            conlist = prepareresult(item['peo'],item['fpo'],tag,innertag,item,conlist)
        
                            summaryVal[tag] = conlist            

        #print(summaryVal)
        summkeys=[]
        for keys in  summaryVal.keys():
            summkeys.append(keys)

        if len(CompulsoryTags)>0:
            for comptags in CompulsoryTags:
                if comptags not in summkeys:
                    return errors


        
        if ( not ( len(LHStags) > 0 and istagexists(summaryVal,LHStags) and len(RHStags) > 0 and istagexists(summaryVal,RHStags) ) ):
            return errors

        lhsvalues = getruntimevalues(LHStags)
        rhsvalues = getruntimevalues(RHStags)
        #print(lhsvalues,rhsvalues)
        if len(crossstatement)>0:
            comparevaluesnofpo(lhsvalues,rhsvalues)
        else:
            comparevalueswithfpo(lhsvalues,rhsvalues)


        return errors
    else:
        return errors

if __name__ == "__main__":
    start_time = time.time()
    results1 = T9(historicalData, filingMetadata, extractedData, parameters)
    print(results1)
    print("Total time: ", time.time() - start_time)

    
