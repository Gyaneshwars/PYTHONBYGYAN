import json
import time
import pandas as pd

# pd.options.mode.chained_assignment = None
import sys
import os

# Get root dir path. This will be paths to the source directory
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Insert path to system path
sys.path.insert(0, root_dir)

import utils
# Importing functions from utils
# from utils.json_parser import *
# from utils.helpers import *


with open(r"C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8",) as f:
    data = json.load(f)


extractedData =  data["extractedData"]
historicalData = data["historicalData"]
filingMetadata = data["filingMetadata"]

# parameters = { "LHSTags": [ { "value": "DBTL" }],"RHSTags": [ { "value": "MCTL" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "322" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "CheckDescription": [ { "value": "Find cases where DBTL Lessthan MCTL for same component in a filingId" } ], "MultiplicationFactor": [ { "value": "{'DBTL':1,'MCTL':1}" } ], "Operation": [ { "value":"<" } ] }

parameters = { "LHSTags": [ { "value": "DBFH" }],"RHSTags": [ { "value": "MCFH" }], "ValueType": [ { "id": 1, "value": "Percentage" } ], "CheckId": [ { "value": "322" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "BlockId": [ { "value": "568" } ], "CheckDescription": [ { "value": "Find cases where  DBFH Lessthan MCFH for same component in a filingId" } ], "MultiplicationFactor": [ { "value": "{'DBFH':1,'MCFH':1}" } ], "Operation": [ { "value":"<" } ] }

def TT8(historicalData, filingMetadata, extractedData, parameters):
    import datetime
    import math

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

    filingid=filingMetadata['metadata']['filingId']
    import ast

    results=[]
    #validating parameter values
    LHStags1=parameters.get('LHSTags')[0].get('value',"").split(',')
    
    LHStags=[]
    RHStags =[]
    RHStags1=parameters.get('RHSTags')[0].get('value',"").split(',')
    
    # RHStags1=get_param_value(parameters,'RHSTags')
    # LHStags1=get_param_value(parameters,'LHSTags')
    mul_fact1=ast.literal_eval(parameters.get('MultiplicationFactor')[0].get('value'))
    
    #print(LHStags1)
    mul_fact={}
    OPERATOR=parameters.get('Operation')[0].get('value')
    CheckDescription= parameters.get('CheckDescription')[0].get('value')
    #flagtype=get_param_id(parameters,'FlagType')

    #OPERATOR='!='
    #stmt=parameters.get('StatementType')[0].get('value')
    Ctype= parameters.get('Main/Breakup/All')[0].get('value')

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
        errors = []
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


        def gethighvalues(withpeo, withfpo,OID):
            reslist = []
            for listitems in summaryVal.values():
                for listitem in listitems:
                    for tagkey in listitem[4].keys():
                        for instancekey in listitem[4][tagkey].keys():
                            for itemvalue in listitem[4][tagkey][instancekey]:
                                if (int(itemvalue['fpo']) == withfpo and itemvalue['peo'] == withpeo  and listitem[5] == OID):
                                    reslist.append( list((tagkey,instancekey,listitem[3],itemvalue)) )
            return reslist

        def comparevalues(lhsvalues,rhsvalues):
            results = {}
            for lhsval in lhsvalues: 
                #print(lhsvalues)   
                for rhsval in rhsvalues:
                    #print(rhsvalues)
                    # print(lhsval[0],rhsval[0])
                    # print(lhsval[1],rhsval[1])
                    #print(lhsval[0],rhsval[0])
                    
                    # print(lhsval[2],rhsval[2],OPERATOR)
                    # print((lhsval[0] == rhsval[0] and lhsval[1] == rhsval[1]  and lhsval[3] == rhsval[3]))
                    if (lhsval[0] == rhsval[0] and lhsval[1] == rhsval[1]  and lhsval[3] == rhsval[3]):
                        if (executeoperator(lhsval[2],rhsval[2],OPERATOR)):
                            diff= (lhsval[2]-rhsval[2])
                            print(diff)
                            higvalues=[]
                            result={"highlights": [], "error": ""}
                            result["error"] = CheckDescription
                            highvalues = gethighvalues(lhsval[0],lhsval[1],lhsval[3])
                            for item in highvalues:
                                result["highlights"].append({"section": item[2], "row": {"name": item[0], "id": item[1]}, "cell": item[3], "filingId": filingMetadata['metadata']['filingId']})
                                result["checkGeneratedFor"]={"statement": item[2], "tag": str(checkmeantfortag)[1:-1], "description": "", "refFilingId": "","filingId": filingMetadata['metadata']['filingId'],"objectId": lhsval[3], "peo": lhsval[0],"fpo": lhsval[1], "diff": diff }
                            errors.append(result)


        def getvalueindex(peoval,fpoval,conlist,OID):
            index = 0
            for item in conlist:
                if (item[0] == peoval and item[1] == fpoval and item[5] == OID):
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
                    findIndex = getvalueindex(item1[0],int(item1[1]),finalvalues,item1[5])
                    if (findIndex >= 0):
                        finalvalues[findIndex][2] = finalvalues[findIndex][2] + ( item1[2] * tagvalue)
                    else:      
                        finalvalues.append( list( (item1[0], int(item1[1]), item1[2] * tagvalue,item1[5],item1[5],item1[5] )) ) 
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

        def prepareresult(peo,fpo,outertag,innertag,value,templist,OID):
            validations = {}
            index = 0
            for i in range(len(templist)):
                item = templist[i]
                if (item[0] == peo and item[1] == fpo and item[5]==OID):
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

                for item in keytagdict.get('values'):
                    if (tag in summaryVal):
                        conlist = summaryVal[str(tag)]
                    if (item['value'] == ''):
                        continue

        #if Mains Breakup and contain child rows and in child records fpo,peo combination exists
                    if (Ctype == 'All' and keytagdict.get('childRows') and childexists(item['fpo'],item['peo'],keytagdict.get('childRows'))):
                        getfinalvalue(keytagdict.get('childRows'),item['fpo'],item['peo'])
                    else: # else take the parent value if exists in lhs/rhs and update final dataset
                        if ( tag in LHStags or tag in RHStags ):
                            objectid1 = keytagdict.get('objectId')
                            findIndex = getvalueindex(item['peo'],item['fpo'],conlist,objectid1)
                            actualvalue = getactualvalue(item['value'],item['scale'])
                            if (findIndex >= 0):
            # Updating an existing value for given tag, peo and fpo
                                conlist[findIndex][2] = conlist[findIndex][2] + actualvalue        
                            else:          
            # if the tag is not found in result, it will insert the record
                                conlist.append( list( (item['peo'], item['fpo'],actualvalue,section, {},objectid1 )) )

                            conlist = prepareresult(item['peo'],item['fpo'],tag,innertag,item,conlist,objectid1)
        
                            summaryVal[tag] = conlist            


        
        if ( not ( len(LHStags) > 0 and istagexists(summaryVal,LHStags) and len(RHStags) > 0 and istagexists(summaryVal,RHStags) ) ):
            return errors

        lhsvalues = getruntimevalues(LHStags)
        rhsvalues = getruntimevalues(RHStags)

        comparevalues(lhsvalues,rhsvalues)

    return errors


if __name__ == "__main__":
  start_time = time.time()
  results1 = TT8(historicalData,filingMetadata,extractedData,parameters)
  print(results1)
  print('Total time: ', time.time() - start_time)
