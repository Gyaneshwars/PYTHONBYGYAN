# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 12:26:18 2023

@author: jaligi
"""

#Estimates Error Checks 
@add_method(Validation)
def Variation_in_fiscal_Year_series(historicalData,filingMetadata,extractedData,parameters):
    # Move to testing
    # Finalized
    errors = []

    operator = get_dataItemIds_list('Operation', parameters)
    
    try:
        temp = extractedData_parsed[(((extractedData_parsed['dataItemFlag']=='G')|(extractedData_parsed['dataItemFlag']=='A'))&(extractedData_parsed['value']!=""))][['dataItemId','peo','fiscalChainSeriesId']]
        temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
        temp['companyId']=filingMetadata['metadata']['companyId']
        
        
        previous = historicalData_parsed[(historicalData_parsed['companyId'].isin(temp['companyId'])
                                  &((historicalData_parsed['dataItemFlag']=='G')|(historicalData_parsed['dataItemFlag']=='A'))&(historicalData_parsed['value']!=""))][['dataItemId','description','peo','fiscalChainSeriesId','filingDate','companyId']]
        
        maxprevious=previous.groupby(['companyId'])['filingDate'].max().reset_index()

        previous=previous[previous['filingDate'].isin(maxprevious['filingDate'])]
        

        merged_df=pd.merge(temp,previous,on=['companyId'],how='inner')

        filingdate=[]
        diff=[]
        perc=[]
        series1=[]
        series2=[]
        for ind,row in merged_df.iterrows():

            if execute_operator(row['fiscalChainSeriesId_x'],row['fiscalChainSeriesId_y'],operator[0]):
                filingdate.append(row['filingDate_y'])             
                difference='NA'
                series1.append(row['fiscalChainSeriesId_x'])
                series2.append(row['fiscalChainSeriesId_y'])
                diff.append(difference)
                perc='NA'

        diff_df=pd.DataFrame({"diff":diff,"perc":perc,"filingDate":filingdate,"curseries":series1,"preseries":series2})
       
        temp1 = extractedData_parsed[extractedData_parsed['fiscalChainSeriesId'].isin(series1)]
        temp2 = historicalData_parsed[(historicalData_parsed['filingDate'].isin(diff_df['filingDate'])&historicalData_parsed['fiscalChainSeriesId'].isin(series2))]
        

        if len(temp1)>0 and len(temp2)>0:

            for ind, row in temp1.iterrows():       
                result = {"highlights": [], "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
                errors.append(result)
           
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "Variation in fiscal Year series compared to the previous document"}
                result["highlights"].append({"row": {"fiscalChainSeriesId": row['fiscalChainSeriesId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
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
def  Trading_Item_is_Null(historicalData,filingMetadata,extractedData,parameters):
    # Move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:
        temp = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        
        dataItemIds=[]
        peos=[]
        diff=[]
        perc=[]
        for ind, row in temp.iterrows():
            if row['tradingItemId'] =='':
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                diff='NA'
                perc='NA'
        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
        
        temp2=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))]

        for ind, row in temp2.iterrows():
                        
            result = {"highlights": [], "error": "Trading Item is Null"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Check    
@add_method(Validation)
def Duplicate_Actual_Guidance_value(historicalData,filingMetadata,extractedData,parameters):

    # Keep dataItemId name first "Max_Threshold" then parameters
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max_Threshold') #
    try:

        current=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        current['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])        

        history=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))&(historicalData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]


        #current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        #history['tradingItemId']=history['tradingItemId'].replace('',np.NaN) 

        
        merged_df=pd.merge(current,history,on=['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df['date_diff']=(merged_df['filingDate_x'])-pd.to_datetime(merged_df['filingDate_y'])

        
        dataItemIds=[]
        diff=[]
        perc=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        prefilingdate=[]
        
        for ind,row in merged_df.iterrows():

            if execute_operator(row['date_diff'].days,float(Threshold[0]),operator[0]):
                dataItemIds.append(row['dataItemId'])               
                prefilingdate.append(row['filingDate_y'])
                diff='NA'
                perc='NA'
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])

        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo':peos})

        temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(dataItemIds) & (historicalData_parsed['peo'].isin(peos))& (historicalData_parsed['filingDate'].isin(prefilingdate))
                                       &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Duplicate Actual and Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():
             
            result = {"highlights": [], "error": "Duplicate Actual and Guidance"}
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
def Both_Actual_Guidance_collected_in_48_Hrs(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max_Threshold')
    try:
        actual=extractedData_parsed[((extractedData_parsed['dataItemFlag']=="A")&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        actual['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])        
        

        guidance=extractedData_parsed[((extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        guidance['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate']) 


        if len(actual)==0:

            actual=historicalData_parsed[ ((historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))
                                          &(historicalData_parsed['dataItemFlag']=="A")&(historicalData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        if len(guidance)==0:
            guidance=historicalData_parsed[ ((historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))
                                            &(historicalData_parsed['dataItemFlag']=="G")&(historicalData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        actual['tradingItemId']=actual['tradingItemId'].replace('',np.NaN)
        guidance['tradingItemId']=guidance['tradingItemId'].replace('',np.NaN) 
        
        if (actual['filingDate']==pd.to_datetime(filingMetadata['metadata']['filingDate'])).any():
            merged_df=pd.merge(actual,guidance,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
        elif (guidance['filingDate']==pd.to_datetime(filingMetadata['metadata']['filingDate'])).any():
            merged_df=pd.merge(actual,guidance,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
       
        merged_df['date_diff']=((merged_df['filingDate_x'])-pd.to_datetime(merged_df['filingDate_y'])).astype('timedelta64[h]')

        diff=[]
        perc=[]
        peos=[]
        actualdate=[]
        guidancedate=[]
        
        for ind,row in merged_df.iterrows():

            if execute_operator(row['date_diff'],float(Threshold[0]),operator[0]):
                diff='NA'
                perc='NA'
                peos.append(row['peo'])
                actualdate.append(row['filingDate_x'])
                guidancedate.append(row['filingDate_y'])

        
        diff_df=pd.DataFrame({"diff":diff,"perc":perc,'peo':peos})

        temp1 = extractedData_parsed[(extractedData_parsed['peo'].isin(peos))]
        temp2 = historicalData_parsed[(historicalData_parsed['peo'].isin(peos) &  ((historicalData_parsed['filingDate'].isin(actualdate))|(historicalData_parsed['filingDate'].isin(guidancedate))))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Actual and Guidance data for the same PEO with in 48hrs"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():
             
            result = {"highlights": [], "error": "Actual and Guidance data for the same PEO with in 48hrs"}
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
def Duplicate_Actuals_or_Guidance_collected_with_the_same_value1(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)

    try:
        current=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
       

        history=historicalData_parsed[ ((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]


        #current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        #history['tradingItemId']=history['tradingItemId'].replace('',np.NaN) 
        
        merged_df=pd.merge(current,history,on=['dataItemId','peo','value','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        
        
        
        dataItemIds=[]
        diff=[]
        perc=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        prefilingdate=[]
        
        for ind,row in merged_df.iterrows():

                dataItemIds.append(row['dataItemId'])               
                prefilingdate.append(row['filingDate'])
                diff='NA'
                perc='NA'
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                

        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo':peos})

        temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(dataItemIds) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['filingDate'].isin(prefilingdate))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Duplicate Actuals or Guidance values collected for the same PEO + data item + trading item + AS + value + currency + units"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():
             
            result = {"highlights": [], "error": "Duplicate Actuals or Guidance values collected for the same PEO + data item + trading item + AS + value + currency + units"}
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
def Duplicate_Actuals_or_Guidance_collected_with_the_same_value(historicalData,filingMetadata,extractedData,parameters):

    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)    
    operator=get_dataItemIds_list('Operation', parameters)

    try:
        actual=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        actual['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])        

        guidance=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        guidance['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate']) 


        if len(actual)==0:
            actual=historicalData_parsed[ ((historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        if len(guidance)==0:
            guidance=historicalData_parsed[ ((historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                            &(historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]


        #actual['tradingItemId']=actual['tradingItemId'].replace('',np.NaN)
        #guidance['tradingItemId']=guidance['tradingItemId'].replace('',np.NaN) 
        
        
        merged_df=pd.merge(actual,guidance,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

        
        diff=[]
        perc=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        actualdataItemId=[]
        guidancedataItemId=[]
        actualdate=[]
        guidancedate=[]
        for ind,row in merged_df.iterrows():

            if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                diff='NA'
                perc='NA'
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                actualdataItemId.append(row['dataItemId_x'])
                guidancedataItemId.append(row['dataItemId_y'])
                actualdate.append(row['filingDate_x'])
                guidancedate.append(row['filingDate_y'])

        
        diff_df=pd.DataFrame({"diff":diff,"perc":perc,'peo':peos})

        temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(actualdataItemId)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((extractedData_parsed['dataItemId'].isin(guidancedataItemId)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
        temp2 = historicalData_parsed[(((historicalData_parsed['peo'].isin(peos)) & (historicalData_parsed['dataItemId'].isin(actualdataItemId))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((historicalData_parsed['dataItemId'].isin(guidancedataItemId))& (historicalData_parsed['filingDate'].isin(actualdate))|(historicalData_parsed['filingDate'].isin(guidancedate))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Same values for Acutal and Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():
             
            result = {"highlights": [], "error": "Same values for Acutal and Guidance"}
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
def Two_dataItems_are_captured_in_different_signs(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        
        try:
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]                          
            
                   
            if len(lhs_df)==0:
                lhs_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            if len(rhs_df)==0:
                rhs_df= historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                              &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]    
             
            if lhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                lhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~(historicalData_parsed['peo'].isin(lhs_df['peo'])))][["dataItemId","peo",'estimatePeriodId',"value_scaled"]]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)            

            if rhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                rhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~(historicalData_parsed['peo'].isin(rhs_df['peo'])))][["dataItemId","peo",'estimatePeriodId',"value_scaled"]]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
            

            lhs_df["valuesign"]=np.sign(lhs_df['value_scaled'])
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
            rhs_df["valuesign"]=np.sign(rhs_df['value_scaled'])
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
 
            
            #lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
            #rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN) 
            
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
            

            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in merged_df.iterrows():
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


            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((extractedData_parsed['dataItemId'].isin(right_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
           
                                                                                                                                                                                                                          
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                                           | ((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]

            for ind, row in temp1.iterrows():
                if row['value']!=0:
                    
                    result = {"highlights": [], "error": "Two DataItems are captured in different signs"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
 
            for ind, row in temp2.iterrows():
                if row['value']!=0:
                    result = {"highlights": [], "error": "Two DataItems are captured in different signs"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)                    
                                   
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors

@add_method(Validation)
def ROE_actual_collected_with_opposite_sign_of_EPS_flavors(historicalData,filingMetadata,extractedData,parameters):
# move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        try:
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]                          
            
            if lhs_df["dataItemId"].nunique()!=len(left_dataItemId_list):
                not_captured=[x for x in left_dataItemId_list if x not in set(lhs_df["dataItemId"])]
                lhs_df_not_cap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(not_captured)) &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                        &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
                
            extracted_dataItemId_peo_count=extractedData_parsed['peo'].nunique()
            lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')   
            
            if (lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                missed_peo_dataItemId=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                            &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~((historicalData_parsed['peo'].isin(collected_peo['peo']))))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
                
                                
            if rhs_df["dataItemId"].nunique()!=len(right_dataItemId_list):
                rhs_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
      
            rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')  
            
            if (rhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                rhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                            &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~((historicalData_parsed['peo'].isin(rhs_df['peo']))))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True) 

            lhs_df["valuesign"]=np.sign(lhs_df['value_scaled'])
            
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
            
            rhs_df["valuesign"]=np.sign(rhs_df['value_scaled'])
          
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
          

            # lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
            # rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN)  
            
     
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')

            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in merged_df.iterrows():
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

            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((extractedData_parsed['dataItemId'].isin(right_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
           
                                                                                                                                                                                                                          
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                                           | ((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
            
            for ind, row in temp1.iterrows():
                if row['value']!=0:
                    
                    result = {"highlights": [], "error": "ROE actual collected with opposite sign of EPS flavors"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
            for ind, row in temp2.iterrows():
                if row['value']!=0:
                    result = {"highlights": [], "error": "ROE actual collected with opposite sign of EPS flavors"}
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
def all_NI_flavors_in_Negative_but_ROE_is_in_positive_for_actual(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        try:
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]                          
            
            if lhs_df["dataItemId"].nunique()!=len(left_dataItemId_list):
                not_captured=[x for x in left_dataItemId_list if x not in set(lhs_df["dataItemId"])]
                lhs_df_not_cap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(not_captured)) &(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                        &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
                
            extracted_dataItemId_peo_count=extractedData_parsed['peo'].nunique()
            lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')   
            
            if (lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                missed_peo_dataItemId=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~((historicalData_parsed['peo'].isin(collected_peo['peo']))))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
                
                                
            if rhs_df["dataItemId"].nunique()!=len(right_dataItemId_list):
                rhs_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
      
            rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')  
            
            if (rhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                rhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))& ~((historicalData_parsed['peo'].isin(rhs_df['peo']))))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True) 

            lhs_df["valuesign"]=np.sign(lhs_df['value_scaled'])
            
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
            
            rhs_df["valuesign"]=np.sign(rhs_df['value_scaled'])
          
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
          

            # lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
            # rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN)  
            
      
            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')

            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in merged_df.iterrows():
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
            
            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((extractedData_parsed['dataItemId'].isin(right_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
           
                                                                                                                                                                                                                          
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                                           | ((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
            
            for ind, row in temp1.iterrows():
                if row['value']!=0:
                    
                    result = {"highlights": [], "error": "All NI flavors in Negative but ROE is in positive for actual"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
            
            for ind, row in temp2.iterrows():
                if row['value']!=0:
                    result = {"highlights": [], "error": "All NI flavors in Negative but ROE is in positive for actual"}
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
def NI_GAAP_and_NI_Normalized_Actual_are_positive_but_ROA_actual_is_negative(historicalData,filingMetadata,extractedData,parameters):
        # Move to testing
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        try:
            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            rhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]                          

            if lhs_df["dataItemId"].nunique()!=len(left_dataItemId_list):
                not_captured=[x for x in left_dataItemId_list if x not in set(lhs_df["dataItemId"])]
                lhs_df_not_cap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(not_captured)) 
                                                        & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                        &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df = pd.concat([lhs_df,lhs_df_not_cap])
           
            extracted_dataItemId_peo_count=extractedData_parsed['peo'].nunique()
            lhs_df_peo_count=lhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')        
        
            if (lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                missed_peo_dataItemId=lhs_df_peo_count[(lhs_df_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']
                collected_peo=lhs_df[lhs_df['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','peo']]
                lhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])) & ~((historicalData_parsed['peo'].isin(collected_peo['peo']))))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)
             
                                
            if rhs_df["dataItemId"].nunique()!=len(right_dataItemId_list):
                rhs_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
      
            rhs_df_peo_count=rhs_df.groupby(['dataItemId'])['peo'].nunique().reset_index(name='peocount')  
            
            if (rhs_df_peo_count['peocount']<extracted_dataItemId_peo_count).any():
                rhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))&(historicalData_parsed['value']!="")
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])) & ~((historicalData_parsed['peo'].isin(rhs_df['peo']))))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)  
            

            lhs_df["valuesign"]=np.sign(lhs_df['value_scaled'])
            
            lhs_df=lhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
            
            rhs_df["valuesign"]=np.sign(rhs_df['value_scaled'])
          
            rhs_df=rhs_df.groupby(['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])["valuesign"].sum().reset_index()
          

            # lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
            # rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN)  
            
      

            merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')


            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]

            for ind,row in merged_df.iterrows():
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


            temp1 = extractedData_parsed[(((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))) | ((extractedData_parsed['dataItemId'].isin(right_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc))))]
           
                                                                                                                                                                                                                          
            temp2 = historicalData_parsed[(((historicalData_parsed['dataItemId'].isin(left_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))) 
                                           | ((historicalData_parsed['dataItemId'].isin(right_dataItemId_list)) & (historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))))]
            
            for ind, row in temp1.iterrows():
                if row['value']!=0:
                    
                    result = {"highlights": [], "error": "NI GAAP and NI Normalized Actual are positive but ROA actual is negative"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
            
            for ind, row in temp2.iterrows():
                if row['value']!=0:
                    result = {"highlights": [], "error": "NI GAAP and NI Normalized Actual are positive but ROA actual is negative"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)            
                                   
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors

@add_method(Validation)
def  dataItemIds_which_have_units_variation(historicalData,filingMetadata,extractedData,parameters):

    #Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    try:
        temp0 = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        
        temp1=temp0.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])['scale'].nunique().reset_index(name='unitscount')

        merged_df=pd.merge(temp0,temp1[temp1['unitscount']>1],on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner') 

        dataItemIds=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind, row in merged_df.iterrows():
            if row['unitscount'] > 1:
                dataItemIds.append(row['dataItemId'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc})
        
        temp2=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp2.iterrows():
                        
            result = {"highlights": [], "error": "dataItemIds which have units variation"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks
@add_method(Validation)
def  dataItemIds_collected_in_wrong_units(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    scale_list=get_dataItemIds_list('scalelist', parameters)
    try:
        temp = extractedData_parsed[ ((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&~(extractedData_parsed['scale'].isin(scale_list)))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        dataItemIds=[]        
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]         
        for ind, row in temp.iterrows():
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'            
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})    

        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        for ind, row in temp1.iterrows():  
                                           
            result = {"highlights": [], "error": "dataItemIds collected in wrong units"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)  

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Check    
@add_method(Validation)
def  Units_captured_as_Myriad(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
    
    scale_list=get_dataItemIds_list('scalelist', parameters)
    countryCode=get_dataItemIds_list('COUNTRY_EXCLUDE',parameters)
    try:
        if filingMetadata['metadata']['country'] not in countryCode:

            temp = extractedData_parsed[ ((extractedData_parsed['value']!="")&(extractedData_parsed['scale'].isin(scale_list)))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]            
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
        
            temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            for ind, row in temp1.iterrows():
                               
                result = {"highlights": [], "error": "Units captured as Myriad"}
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
def dataItemIds_which_have_more_than_100_Variation_between_Quarters(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)#>
        Threshold=get_parameter_value(parameters,'Min_Threshold')#100
        
        try:
            companyid=filingMetadata['metadata']['companyId']
            currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]


            for ind,row in currentquarter.iterrows():
                if row['fiscalQuarter']==1:
                
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['value']!="")
                                                                
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']-1))&(historicalData_parsed['companyId']==companyid)
                                                                &(historicalData_parsed['fiscalQuarter']==4))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 
                else:
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['value']!="")
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']))&(historicalData_parsed['companyId']==companyid)
                                                                &(historicalData_parsed['fiscalQuarter'].isin(currentquarter['fiscalQuarter']-1)))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]                          

            if len(currentquarter) >0 and len(comparabletquarter)>0: 
                # currentquarter['tradingItemId']=currentquarter['tradingItemId'].replace('',np.NaN)
                # comparabletquarter['tradingItemId']=comparabletquarter['tradingItemId'].replace('',np.NaN) 
        
                merged_df=pd.merge(currentquarter,comparabletquarter,on=['dataItemId','parentFlag','tradingItemId','accountingStandardDesc','fiscalChainSeriesId','periodTypeId'],how='inner')
       
                base_currency=merged_df.currency_x.mode()[0]
                merged_df['value_scaled_y'] = merged_df.apply(lambda x: currency_converter(currency_from=x['currency_y'], currency_to=base_currency, value=x['value_scaled_y']), axis=1)                
                merged_df['variation']=((merged_df[['value_scaled_x','value_scaled_y']].max(axis=1)-merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))/merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))*100
                
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                dataItemIds=[]
                peos_x=[]
                peos_y=[]

                diff=[]
                perc=[]
                for ind,row in merged_df.iterrows():
                    if execute_operator(row['variation'],Threshold[0],operator[0]):
                        peos_x.append(row['peo_x'])
                        peos_y.append(row['peo_y'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        diff.append(difference)
                        perc.append(row['variation'])
                        dataItemIds.append(row['dataItemId'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        
                diff_df=pd.DataFrame({"peo_x":peos_x,'peo_y':peos_y,"diff":diff,"perc":perc,'dataItemId':dataItemIds})

            
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos_x))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peos_y))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                 
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
     
                for ind, row in temp2.iterrows():
                    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)                    
                                   
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors


#Estimates Error Checks 
@add_method(Validation)
def dataItemIds_which_have_more_than_100_Variation_between_Quarters_if_comparable_and_collected_value_are_in_same_sign(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        Threshold=get_parameter_value(parameters,'Min_Threshold')
        
        try:
            companyid=filingMetadata['metadata']['companyId']
            currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['periodTypeId']==2))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
            

            for ind,row in currentquarter.iterrows():
                if row['fiscalQuarter']==1:
                
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['value']!="")
                                                                
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']-1))&(historicalData_parsed['companyId']==companyid)
                                                                &(historicalData_parsed['fiscalQuarter']==4))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 
                else:
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['value']!="")
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']))&(historicalData_parsed['companyId']==companyid)
                                                                &(historicalData_parsed['fiscalQuarter'].isin(currentquarter['fiscalQuarter']-1)))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]                          
            
            if len(currentquarter) >0 and len(comparabletquarter)>0: 
                # currentquarter['tradingItemId']=currentquarter['tradingItemId'].replace('',np.NaN)
                # comparabletquarter['tradingItemId']=comparabletquarter['tradingItemId'].replace('',np.NaN) 
                
                currentquarter['valuesign']=np.sign(currentquarter['value_scaled'])
                comparabletquarter['valuesign']=np.sign(comparabletquarter['value_scaled'])
    
    
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
                for ind,row in merged_df.iterrows():
    
                    if row['valuesign_x']==row['valuesign_y']:
                        if execute_operator(row['variation'],Threshold[0],operator[0]):
                            peos_x.append(row['peo_x'])
                            peos_y.append(row['peo_y'])
                            tid.append(row['tradingItemId']) 
                            parentflag.append(row['parentFlag']) 
                            accounting.append(row['accountingStandardDesc']) 
                            fyc.append(row['fiscalChainSeriesId'])
                            difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                            diff.append(difference)
                            perc.append(row['variation'])
                            dataItemIds.append(row['dataItemId'])
                diff_df=pd.DataFrame({"peo_x":peos_x,'peo_y':peos_y,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
    
                
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos_x))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peos_y))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

                 
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters - Only if comparable and collected value are in same sign"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
     
                for ind, row in temp2.iterrows():
                    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters - Only if comparable and collected value are in same sign"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)                    
                                   
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors


#Estimates Error Checks 
@add_method(Validation)
def dataItemIds_which_have_more_than_100_Variation_between_Quarters_if_comparable_and_collected_value_are_in_absolutes(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        Threshold=get_parameter_value(parameters,'Min_Threshold')
        
        try:
            companyid=filingMetadata['metadata']['companyId']

            currentquarter = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['scale']=="ACTUAL"))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','scale']]

            for ind,row in currentquarter.iterrows():
                if row['fiscalQuarter']==1:
                
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['value']!="")
                                                                
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']-1))&(historicalData_parsed['companyId']==companyid)
                                                                &(historicalData_parsed['fiscalQuarter']==4)
                                                                &(historicalData_parsed['scale']=='ACTUAL'))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','scale']] 
                else:
                    comparabletquarter = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentquarter['dataItemId']))&(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                                                &(historicalData_parsed['fiscalYear'].isin(currentquarter['fiscalYear']))
                                                                &(historicalData_parsed['fiscalQuarter'].isin(currentquarter['fiscalQuarter']-1))&(historicalData_parsed['scale']=='ACTUAL'))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','scale']]                          
            

            if len(currentquarter) >0 and len(comparabletquarter)>0:

                # currentquarter['tradingItemId']=currentquarter['tradingItemId'].replace('',np.NaN)
                # comparabletquarter['tradingItemId']=comparabletquarter['tradingItemId'].replace('',np.NaN) 
                
    
                merged_df=pd.merge(currentquarter,comparabletquarter,on=['dataItemId','parentFlag','tradingItemId','accountingStandardDesc','fiscalChainSeriesId','periodTypeId'],how='inner')

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
                for ind,row in merged_df.iterrows():
    
                    if execute_operator(row['variation'],Threshold[0],operator[0]):
                        peos_x.append(row['peo_x'])
                        peos_y.append(row['peo_y'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        diff.append(difference)
                        perc.append(row['variation'])
                        dataItemIds.append(row['dataItemId'])
                diff_df=pd.DataFrame({"peo_x":peos_x,'peo_y':peos_y,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
    
                
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos_x))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peos_y))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

                 
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters - Only if comparable and collected value are in absolutes"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
     
                for ind, row in temp2.iterrows():
                    
                    result = {"highlights": [], "error": "dataItemIds which have more than 100% Variation between Quarters - Only if comparable and collected value are in absolutes"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)                    
                                       
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors

            
# Estimates Error Checks 
@add_method(Validation)
def dataItemId_having_vairation_when_compared_to_previous_document(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
        
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max_Threshold')
    try:
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['peo'].isin(current['peo']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
        

        if len (previous)>0:
            maxprevious=previous.groupby(['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

            previous=previous[previous['filingDate'].isin(maxprevious['filingDate'])]
        
        if (len(current)>0 and len(previous)>0):
            base_currency=current.currency.mode()[0]
            current["value_scaled"] = current.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            previous["value_scaled"] = previous.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    
            # current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
            # previous['tradingItemId']=previous['tradingItemId'].replace('',np.NaN)
            

            merged_df=pd.merge(current,previous,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
    
            merged_df['variation']=((merged_df[['value_scaled_x','value_scaled_y']].max(axis=1)-merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))/merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))*100        

    
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]
    
            for ind,row in merged_df.iterrows():
                if execute_operator(row['variation'],float(Threshold[0]),operator[0]):
    
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])  
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
               
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds})
    
           
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peos)) & (historicalData_parsed['filingDate'].isin(maxprevious['filingDate']))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
           
    
            for ind, row in temp1.iterrows():
    
                result = {"highlights": [], "error": "1000% variation between current and previous document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
           
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "1000% variation between current and previous document"}
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
def Data_Item_value_is_same_between_current_and_previous_but_units_currency_is_different(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
        
    operator = get_dataItemIds_list('Operation', parameters)
    
    try:

        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!=""))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId']))&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['peo'].isin(current['peo'])))][['dataItemId','peo','estimatePeriodId','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        maxprevious=previous.groupby(['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

        current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        maxprevious['tradingItemId']=maxprevious['tradingItemId'].replace('',np.NaN)
       
        merged_df=pd.merge(current,maxprevious,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

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

            if execute_operator(row['value_x'],row['value_y'],operator[0]):

                if execute_operator(row['scale_x'],row['scale_y'],operator[1]):
                    filingdate.append(row['filingDate'])
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])   
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'
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

       
        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
        temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peos)) & (historicalData_parsed['filingDate'].isin(filingdate))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
       

        for ind, row in temp1.iterrows():

            result = {"highlights": [], "error": "dataItemIds have same value but having different units compared with previous"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
       
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "dataItemIds have same value but having different units compared with previous"}
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
def Consensus_more_than_or_equal_to_100_to_below_500(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold1=get_parameter_value(parameters,'Min_Threshold')
    Threshold2=get_parameter_value(parameters,'Max_Threshold')
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency']]
        print(temp)
    
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            print(temp)
            base_currency=temp.currency.mode()[0]
            temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100
               
        peos=[]
        tid=[]
        dataItemIds=[]        
        parentflag=[]
        accounting=[]
        fyc=[]
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():
            print(row)
            if execute_operator(row['consensusvariation'],float(Threshold1[0]),operator[0]):
                if execute_operator(row['consensusvariation'],float(Threshold2[0]),operator[1]):
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])   
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                    diff.append(difference)
                    
                    perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)

        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})


        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Consensus more than or equal to 100% to below 500%"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Consensus more than or equal to 100% to below 500%"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def EBITDA_and_GM_Quartile_0_100(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator = get_dataItemIds_list('Operation', parameters)
        Threshold=get_parameter_value(parameters,'Max_Threshold')

        
        try:
            q1=pd.DataFrame()
            q2=pd.DataFrame()
            q3=pd.DataFrame()
            q4=pd.DataFrame()
            q5=pd.DataFrame()
            q6=pd.DataFrame()
            q7=pd.DataFrame()
            q8=pd.DataFrame()
            fy1=pd.DataFrame()
            fy2=pd.DataFrame()
            fy3=pd.DataFrame()
            fy4=pd.DataFrame()
            h1=pd.DataFrame()
            h2=pd.DataFrame()
            h3=pd.DataFrame()
            h4=pd.DataFrame()
            
            companyid=filingMetadata['metadata']['companyId']
            currentpeo = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
            #print(currentpeo[['dataItemId','peo','value_scaled','periodTypeId','fiscalQuarter']])

            if len(currentpeo)>0:
                currentpeo['consValue_scaled']=currentpeo.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            
                base_currency=currentpeo.currency.mode()[0]
                currentpeo["consValue_scaled"] = currentpeo.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                currentpeo['consensusvariation']=((currentpeo[['value_scaled','consValue_scaled']].max(axis=1)-currentpeo[['value_scaled','consValue_scaled']].min(axis=1))/currentpeo[['value_scaled','consValue_scaled']].min(axis=1))*100
                currentpeo['tradingItemId']=currentpeo['tradingItemId'].replace('',np.NaN)

            for ind,row in currentpeo.iterrows():
                if row['periodTypeId']==2:
                    if row['fiscalQuarter']==1:
                
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
                                                                    
                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==2:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==3:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==4:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                    if len(q1)>0:
                        q1['consValue_scaled']=q1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
        
                    
                        base_currency=q1.currency.mode()[0]
                        q1["consValue_scaled"] = q1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q1['consensusvariation']=((q1[['value_scaled','consValue_scaled']].max(axis=1)-q1[['value_scaled','consValue_scaled']].min(axis=1))/q1[['value_scaled','consValue_scaled']].min(axis=1))*100
              
                    
                    if len(q2)>0:
                        q2['consValue_scaled']=q2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q2.currency.mode()[0]
                        q2["consValue_scaled"] = q2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q2['consensusvariation']=((q2[['value_scaled','consValue_scaled']].max(axis=1)-q2[['value_scaled','consValue_scaled']].min(axis=1))/q2[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q3)>0:
                        q3['consValue_scaled']=q3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q3.currency.mode()[0]
                        q3["consValue_scaled"] = q3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q3['consensusvariation']=((q3[['value_scaled','consValue_scaled']].max(axis=1)-q3[['value_scaled','consValue_scaled']].min(axis=1))/q3[['value_scaled','consValue_scaled']].min(axis=1))*100
                     
                            
                    if len(q4)>0:
                        q4['consValue_scaled']=q4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q4.currency.mode()[0]
                        q4["consValue_scaled"] = q4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q4['consensusvariation']=((q4[['value_scaled','consValue_scaled']].max(axis=1)-q4[['value_scaled','consValue_scaled']].min(axis=1))/q4[['value_scaled','consValue_scaled']].min(axis=1))*100
                    
        
                    
                    if len(q5)>0:
                        q5['consValue_scaled']=q5.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q5.currency.mode()[0]
                        q5["consValue_scaled"] = q5.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q5['consensusvariation']=((q5[['value_scaled','consValue_scaled']].max(axis=1)-q5[['value_scaled','consValue_scaled']].min(axis=1))/q5[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q6)>0:
                        q6['consValue_scaled']=q6.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q6.currency.mode()[0]
                        q6["consValue_scaled"] = q6.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q6['consensusvariation']=((q6[['value_scaled','consValue_scaled']].max(axis=1)-q6[['value_scaled','consValue_scaled']].min(axis=1))/q6[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q7)>0:
                        q7['consValue_scaled']=q7.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q7.currency.mode()[0]
                        q7["consValue_scaled"] = q7.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q7['consensusvariation']=((q7[['value_scaled','consValue_scaled']].max(axis=1)-q7[['value_scaled','consValue_scaled']].min(axis=1))/q7[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q8)>0:
                        q8['consValue_scaled']=q8.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q8.currency.mode()[0]
                        q8["consValue_scaled"] = q8.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q8['consensusvariation']=((q8[['value_scaled','consValue_scaled']].max(axis=1)-q8[['value_scaled','consValue_scaled']].min(axis=1))/q8[['value_scaled','consValue_scaled']].min(axis=1))*100

                    quarters_list=[q1,q2,q3,q4,q5,q6,q7,q8]

                    

                    quartilerange=pd.concat(quarters_list,join='outer')
                    

                    quartilerange['tradingItemId']=quartilerange['tradingItemId'].replace('',np.NaN)
                    if len(quartilerange)>0:
                        quartervalue=quartilerange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'], dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])


                if row['periodTypeId']==10:
                    
                    if row['fiscalQuarter']==4:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
            
                                 
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                    else:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    if len(h1)>0:
                        h1['consValue_scaled']=h1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h1.currency.mode()[0]
                        h1["consValue_scaled"] = h1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h1['consensusvariation']=((h1[['value_scaled','consValue_scaled']].max(axis=1)-h1[['value_scaled','consValue_scaled']].min(axis=1))/h1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(h2)>0:
                        h2['consValue_scaled']=h2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h2.currency.mode()[0]
                        h2["consValue_scaled"] = h2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h2['consensusvariation']=((h2[['value_scaled','consValue_scaled']].max(axis=1)-h2[['value_scaled','consValue_scaled']].min(axis=1))/h2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(h3)>0:
                        h3['consValue_scaled']=h3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h3.currency.mode()[0]
                        h3["consValue_scaled"] = h3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h3['consensusvariation']=((h3[['value_scaled','consValue_scaled']].max(axis=1)-h3[['value_scaled','consValue_scaled']].min(axis=1))/h3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(h4)>0:
                        h4['consValue_scaled']=h4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h4.currency.mode()[0]
                        h4["consValue_scaled"] = h4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h4['consensusvariation']=((h4[['value_scaled','consValue_scaled']].max(axis=1)-h4[['value_scaled','consValue_scaled']].min(axis=1))/h4[['value_scaled','consValue_scaled']].min(axis=1))*100


                    half_list=[h1,h2,h3,h4]
        
                    halfrange=pd.concat(half_list,join='outer')

                    halfrange['tradingItemId']=halfrange['tradingItemId'].replace('',np.NaN)
                    if len(halfrange)>0:
                        halfvalue=halfrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])



                if row['periodTypeId']==1:


                    fy1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     

                    
                    fy2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))                                                        
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
        
                    
                    fy3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-3))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
          
                    fy4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-4))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
    
        
                    if len(fy1)>0:
                        fy1['consValue_scaled']=fy1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy1.currency.mode()[0]
                        fy1["consValue_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy1['consensusvariation']=((fy1[['value_scaled','consValue_scaled']].max(axis=1)-fy1[['value_scaled','consValue_scaled']].min(axis=1))/fy1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(fy2)>0:
                        fy2['consValue_scaled']=fy2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy2.currency.mode()[0]
                        fy2["consValue_scaled"] = fy2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy2['consensusvariation']=((fy2[['value_scaled','consValue_scaled']].max(axis=1)-fy2[['value_scaled','consValue_scaled']].min(axis=1))/fy2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(fy3)>0:
                        fy3['consValue_scaled']=fy3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy3.currency.mode()[0]
                        fy3["consValue_scaled"] = fy3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy3['consensusvariation']=((fy3[['value_scaled','consValue_scaled']].max(axis=1)-fy3[['value_scaled','consValue_scaled']].min(axis=1))/fy3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(fy4)>0:
                        fy4['consValue_scaled']=fy4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy4.currency.mode()[0]
                        fy4["consValue_scaled"] = fy4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy4['consensusvariation']=((fy4[['value_scaled','consValue_scaled']].max(axis=1)-fy4[['value_scaled','consValue_scaled']].min(axis=1))/fy4[['value_scaled','consValue_scaled']].min(axis=1))*100
                   
                    fy_list=[fy1,fy2,fy3,fy4]

                    fyrange=pd.concat(fy_list,join='outer')

                    #fyrange['tradingItemId']=fyrange['tradingItemId'].replace('',np.NaN)
                    #print(fyrange)
                    if len(fyrange)>0:
                        fyvalue=fyrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])


            merged_df=pd.DataFrame()
 
            for ind,row in currentpeo.iterrows():
                if row['periodTypeId']==2:
                    if len(quartilerange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,quartervalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                if row['periodTypeId']==1:
                    if len(fyrange)>0:

                        merged_df=merged_df.append(pd.merge(currentpeo,fyvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 

                if row['periodTypeId']==10:
                    if len(halfrange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,halfvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
            
                      
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            diff=[]
            perc=[]

            for ind,row in merged_df.iterrows():

                if execute_operator(row['quartilevalue'],row['consensusvariation'],operator[0]):

                    if execute_operator(row['consensusvariation'],float(Threshold[0]),operator[1]):
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

    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 

             
            for ind, row in temp1.iterrows():
                

                result = {"highlights": [], "error": " Quartile formula value is > Consensus variation % and consensus variation is <100% "}
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
def EE_dataItemId_not_in_current_document_compared_to_previous_document_DataItem(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
        
        
    try:
        companyid=filingMetadata['metadata']['companyId']
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!="") )][['dataItemId','dataItemFlag','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        

        previous = historicalData_parsed[((historicalData_parsed['dataItemFlag']=='G')&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['companyId']==companyid))][['dataItemId','peo','dataItemFlag','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]
        
        
        maxprevious=previous.groupby(['dataItemId','peo','dataItemFlag','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

        # current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        # maxprevious['tradingItemId']=maxprevious['tradingItemId'].replace('',np.NaN)
        

        temp=maxprevious[(~(maxprevious['dataItemId'].isin(current['dataItemId']))&(maxprevious['parentFlag'].isin(current['parentFlag']))
                          &(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))&(maxprevious['tradingItemId'].isin(current['tradingItemId'])))]

        temp1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(temp['dataItemId'])) & (historicalData_parsed['peo'].isin(temp['peo']))&(historicalData_parsed['filingDate'].isin(temp['filingDate']))&(historicalData_parsed['companyId']==companyid))] 

       
        for ind, row in temp1.iterrows():

            result = {"highlights": [], "error": "dataItemId not in current document compared to previous document"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']},"section": row['team'],"filingId": row['versionId']})
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
def EE_dataItemId_not_in_current_document_compared_to_previous_document_flavor(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
        
        
    try:
        companyid=filingMetadata['metadata']['companyId']
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!=""))][['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        previous = historicalData_parsed[((historicalData_parsed['dataItemFlag']=='G')&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['companyId']==companyid))][['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','filingDate']]

        maxprevious=previous.groupby(['parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])['filingDate'].max().reset_index()
                                                                                                             

        temp=maxprevious[(~(maxprevious['parentFlag'].isin(current['parentFlag']))|~(maxprevious['accountingStandardDesc'].isin(current['accountingStandardDesc']))|
                          ~(maxprevious['tradingItemId'].isin(current['tradingItemId']))|~(maxprevious['fiscalChainSeriesId'].isin(current['fiscalChainSeriesId'])))]
                          

       
        temp1 = historicalData_parsed[((historicalData_parsed['parentFlag'].isin(temp['parentFlag'])) & (historicalData_parsed['accountingStandardDesc'].isin(temp['accountingStandardDesc']))
                                        & (historicalData_parsed['tradingItemId'].isin(temp['tradingItemId']))& (historicalData_parsed['fiscalChainSeriesId'].isin(temp['fiscalChainSeriesId']))
                                        &(historicalData_parsed['filingDate'].isin(temp['filingDate']))&(historicalData_parsed['companyId']==companyid))] 
        
       
        for ind, row in temp1.iterrows():

            result = {"highlights": [], "error": "Flavors not captured in the current document but available in the previous document"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e) 
        return errors


@add_method(Validation)
def multipleValueAreNull(historicalData,filingMetadata,extractedData,parameters):
    errors = []
    dataItemId_list1=get_dataItemIds_list('LHSdataItemIds', parameters)# Non per share data items
    dataItemId_list2=get_dataItemIds_list('RHSdataItemIds', parameters)# per share data items
    try:
        temp1 = extractedData_parsed[ (extractedData_parsed['dataItemId'].isin(dataItemId_list1))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','fiscalChainSeriesId','tradingItemId']]
        temp2 = extractedData_parsed[ (extractedData_parsed['dataItemId'].isin(dataItemId_list2))][['dataItemId','peo','value','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        dataItemIds=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        diff=[]
        perc=[]
        for ind, row in temp1.iterrows():

            if((row['value'] !='') & ((row['currency']=='')|(row['scale']=='')|(row['parentFlag']=='')|(row['accountingStandardDesc']==''))):
                
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'

        for ind, row in temp2.iterrows():

            if((row['value'] !='') & ((row['currency']=='')|(row['scale']=='')|(row['parentFlag']=='')|(row['accountingStandardDesc']=='')|(row['tradingItemId']==''))):

                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'                
                
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})

        final=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
        for ind, row in final.iterrows():
            result = {"highlights": [], "error": "Units/Currency/Trading Item/Accounting standard/Parent flag  not specified for the Data Item,"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Check    
@add_method(Validation)
def Duplicate_Actual_Guidance_value_comparable_checks(historicalData,filingMetadata,extractedData,parameters):

    # Keep dataItemId name first "Max_Threshold" then parameters
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max_Threshold') #
    try:
        companyid=filingMetadata['metadata']['companyId']
        current=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        current['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])        
        
        
        history=historicalData_parsed[ ((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid))][['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]


        # current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        # history['tradingItemId']=history['tradingItemId'].replace('',np.NaN) 
        

        merged_df=pd.merge(current,history,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
        merged_df['date_diff']=(merged_df['filingDate_x'])-pd.to_datetime(merged_df['filingDate_y'])


        dataItemIds=[]
        diff=[]
        perc=[]
        peos=[]
        prefilingdate=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        
        
        for ind,row in merged_df.iterrows():

            if execute_operator(row['date_diff'].days,float(Threshold[0]),operator[0]):
                dataItemIds.append(row['dataItemId'])               
                prefilingdate.append(row['filingDate_y'])
                diff='NA'
                perc='NA'
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])

        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo':peos})

        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(dataItemIds) & (historicalData_parsed['peo'].isin(peos))& (historicalData_parsed['filingDate'].isin(prefilingdate))&(historicalData_parsed['companyId']==companyid)
                                       &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Duplicate Actual and Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():
             
            result = {"highlights": [], "error": "Duplicate Actual and Guidance"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
            
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


@add_method(Validation)
def All_Qs_in_one_sign_and_FY_in_another_Sign(historicalData,filingMetadata,extractedData,parameters): 
    # move to testing
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)

    try:
        companyid=filingMetadata['metadata']['companyId']
        FQ = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")  & ((extractedData_parsed["periodTypeId"] == 2)|(extractedData_parsed["periodTypeId"] == 10)))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 

        FQ_not_cap = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(FQ['dataItemId'])) &(historicalData_parsed['value']!="")& ((historicalData_parsed["periodTypeId"] == 2)|(historicalData_parsed["periodTypeId"] == 10)) &(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))&(historicalData_parsed['companyId']==companyid))] [['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        
        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) &(extractedData_parsed['value']!="")& (extractedData_parsed["periodTypeId"] == 1))][['dataItemId','peo','scale','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        if len(FY)==0:
            FY = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list)) &(historicalData_parsed['value']!="")  &(historicalData_parsed["periodTypeId"] ==1) & (historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))&(historicalData_parsed['companyId']==companyid))][['dataItemId','peo','value','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] 
        
        FQ=pd.concat([FQ,FQ_not_cap])

        FQ["valuesign"]=np.sign(FQ['value'])
        FY["valuesign"]=np.sign(FY['value'])

        merged_df=pd.merge(FQ,FY,on=['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId',],how='inner')

        merged_df_PEO_count= merged_df.groupby(['dataItemId','fiscalYear','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId_x']).agg(PEO_Count=('peo_x','count')).reset_index() 

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
            if (row['periodTypeId_x']==2 and row['PEO_Count']==4):

                if ((row['fiscalYear']==merged_df['fiscalYear'])&(merged_df['valuesign_x']!=merged_df['valuesign_y'])).all():

                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])

        for ind,row in merged_df_PEO_count.iterrows():
            if (row['periodTypeId_x']==10 and row['PEO_Count']==2):

                if ((row['fiscalYear']==merged_df['fiscalYear'])&(merged_df['valuesign_x']!=merged_df['valuesign_y'])).all():

                    peos.append(row['fiscalYear'])
                    dataItemIds.append(row['dataItemId'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])


        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"fiscalYear":peos,"diff":diff,"perc":perc})
        

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['fiscalYear'].isin(peos))
                                      &(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['fiscalYear'].isin(peos))
                                       &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]


        for ind, row in temp1.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "ll Qs in one sign and FY in another Sign"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)                    
        for ind, row in temp2.iterrows():
            if row['value']!=0:
                result = {"highlights": [], "error": "ll Qs in one sign and FY in another Sign"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"],  "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
                errors.append(result)
        print(errors) 
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Check 
@add_method(Validation)
def Guidance_data_not_captured_for_PEO_next_to_Actual_PEO(historicalData,filingMetadata,extractedData,parameters):
        # Move to testing
        # Finalized
        errors = []

        try:
            currentyear=filingMetadata['metadata']['latestPeriodYear']

            fy1 = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="") 
                                        &(extractedData_parsed['fiscalYear']==int(currentyear)+1)
                                        &(extractedData_parsed['periodTypeId']==1))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']] 
            

            fy1LaterPEO=extractedData_parsed[((extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!="") 
                                            &(extractedData_parsed['fiscalYear']>int(currentyear)+1)
                                            &(extractedData_parsed['periodTypeId']==1))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']]
            

            peos=[]
            dataItemIds=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            if len(fy1)==0 and len(fy1LaterPEO)>0:

                for ind,row in fy1LaterPEO.iterrows():
                
                    dataItemIds.append(row['dataItemId'])   
                    peos.append(row['peo'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    difference="NA"
                    diff.append(difference)
                    
                    perc="NA"
            

            diff_df=pd.DataFrame({"peo":peos,"dataItemId":dataItemIds,"diff":diff,"perc":perc})

            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                          &(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            
            
            for ind,row in temp1.iterrows():
                
                result = {"highlights": [], "error": "Guidance data not captured for PEO next to Actual PEO"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                errors.append(result)
                             
               
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors        

# Estimates Error Checks 
@add_method(Validation)
def dataItemId_having_variation_when_compared_to_previous_document(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
        
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Max_Threshold')
    try:
        current = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!="") )][['dataItemId','peo','estimatePeriodId','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear']]
        
        previous = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))&(historicalData_parsed['value']!="")
                                          &(historicalData_parsed['peo'].isin(current['peo']))&(historicalData_parsed['estimatePeriodId'].isin(current['estimatePeriodId'])))][['dataItemId','peo','estimatePeriodId','value_scaled','scale','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','filingDate']]

        if len (previous)>0:
            maxprevious=previous.groupby(['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'])['filingDate'].max().reset_index()

            previous=previous[previous['filingDate'].isin(maxprevious['filingDate'])]
        
        
        if (len(current)>0 and len(previous)>0):
            base_currency=current.currency.mode()[0]
            current["value_scaled"] = current.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            previous["value_scaled"] = previous.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
    
            # current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
            # previous['tradingItemId']=previous['tradingItemId'].replace('',np.NaN)
            
           
            merged_df=pd.merge(current,previous,on=['dataItemId','peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')
    
            merged_df['variation']=((merged_df[['value_scaled_x','value_scaled_y']].max(axis=1)-merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))/merged_df[['value_scaled_x','value_scaled_y']].min(axis=1))*100        
     

            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
    
            for ind,row in merged_df.iterrows():

                if execute_operator(abs(row['variation']),float(Threshold[0]),operator[0]):
    
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])               
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                    diff.append(difference)
                    perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
    
                        
               
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc,"dataItemId":dataItemIds})
    
           
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                          &(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            temp2 = historicalData_parsed[(historicalData_parsed['dataItemId'].isin(dataItemIds) & (historicalData_parsed['peo'].isin(peos)) & (historicalData_parsed['filingDate'].isin(maxprevious['filingDate']))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
           
    
            for ind, row in temp1.iterrows():
    
                result = {"highlights": [], "error": "dataItemIds have percendataItemIde variation compared with the Previous Document"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
           
            for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "dataItemIds have percendataItemIde variation compared with the Previous Document"}
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
def Min_PEO_actualized_date_is_greater_than_the_Max_PEO_actualized_date(historicalData,filingMetadata,extractedData,parameters):
    # Finalized
    errors = []
    operator = get_dataItemIds_list('Operation', parameters)
    try:
        max_peo=filingMetadata['metadata']['latestActualizedPeo']
        max_FY=filingMetadata['metadata']['latestPeriodYear']
        companyid=filingMetadata['metadata']['companyId']

        max_peo_filing= historicalData_parsed[((historicalData_parsed['periodEndDate']==max_peo)&(historicalData_parsed['companyId']==companyid))][['companyId','peo','periodTypeId','fiscalYear','fiscalQuarter','actualizedDate']]
        

        min_peo_filing= historicalData_parsed[((historicalData_parsed['companyId'].isin(max_peo_filing['companyId']))
                                                &(historicalData_parsed['fiscalYear']<int(max_FY)-1))][['companyId','peo','periodTypeId','fiscalYear','fiscalQuarter','actualizedDate']]

        
        merged_df=pd.merge(min_peo_filing,max_peo_filing,on=['companyId'])

        min_peos=[]
        max_peos=[]
        diff=[]
        perc=[]
        companyid=[]
        for ind,row in merged_df.iterrows():
           
            if execute_operator(row['actualizedDate_x'],row['actualizedDate_y'],operator[0]):
                companyid.append(row['companyId'])
                min_peos.append(row['peo_x'])  
                max_peos.append(row['peo_x']) 
                difference='NA'
                diff.append(difference)
                perc='NA'
      
       
        diff_df=pd.DataFrame({"peo_x":min_peos,"peo_y":max_peos,"diff":diff,"perc":perc})

        temp1=historicalData_parsed[(historicalData_parsed['peo'].isin(min_peos)&historicalData_parsed['companyId'].isin(companyid))]
        temp2=historicalData_parsed[(historicalData_parsed['peo'].isin(max_peos)&historicalData_parsed['companyId'].isin(companyid))]

        for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "EBT GAAP actual 100 times greater than EBT Normalized actual"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            
        for ind, row in temp2.iterrows():
                result = {"highlights": [], "error": "EBT GAAP actual 100 times greater than EBT Normalized actual"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)            
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def dataItemIds_have_same_value_for_different_data_items_for_the_same_PEO (historicalData,filingMetadata,extractedData,parameters):
    # move to testing
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    try:

        temp0 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

        merged_df=pd.merge(temp0,temp1,on=['parentFlag','peo','accountingStandardDesc','fiscalChainSeriesId','fiscalYear','periodTypeId','tradingItemId'],how='inner')
                                                        
 
        dataItemIds_x=[]
        dataItemIds_y=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff='NA'
        perc='NA'
        for ind, row in merged_df.iterrows():
           
            if (execute_operator(row['dataItemId_x'],row['dataItemId_y'],operator[0])&execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[1])):
                dataItemIds_x.append(row['dataItemId_x'])
                dataItemIds_y.append(row['dataItemId_y'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                
        diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,"dataItemId_y":dataItemIds_y,"diff":diff,"perc":perc,'peo':peos}) 

        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_x)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp3 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_y)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
             
        for ind, row in temp2.iterrows():        
            result = {"highlights": [], "error": "dataItemIds have same value for different data items for the same PEO"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
            
        for ind, row in temp3.iterrows():        
            result = {"highlights": [], "error": "dataItemIds have same value for different data items for the same PEO"}
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
def Value_mismatch_for_balance_sheet_dataitems_for_same_period_end_date(historicalData,filingMetadata,extractedData,parameters):
     
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #Data items
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
    # conditions for flavour check
        companyid=filingMetadata['metadata']['companyId']
        FY = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==1)&(extractedData_parsed['fiscalQuarter']==4)&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #REVENUE
        Q4 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['fiscalQuarter']==4)&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        H2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['periodTypeId']==10)&(extractedData_parsed['fiscalQuarter']==4)&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        

        if FY["dataItemId"].nunique()!=len(left_dataItemId_list):
            FY = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['periodTypeId']==1)
                                        &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                        &(historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX
        extracted_dataItemId_peo_count=extractedData_parsed['fiscalYear'].nunique()
        FY_peo_count=FY.groupby(['dataItemId'])['fiscalYear'].nunique().reset_index(name='peocount')    


        if (FY_peo_count['peocount']<extracted_dataItemId_peo_count).any():
            missed_peo_dataItemId=FY_peo_count[(FY_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']

            collected_peo=FY[FY['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','fiscalYear']]

            FY_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId))&(historicalData_parsed['periodTypeId']==1) 
                                                  &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                                  & (historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))&~(historicalData_parsed['fiscalYear'].isin(collected_peo['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

            FY=FY.append(FY_missing_data,ignore_index=True)

 
        
        if Q4["dataItemId"].nunique()!=len(left_dataItemId_list):

            Q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['periodTypeId']==2)
                                        &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                        &(historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX

        Q4_peo_count=Q4.groupby(['dataItemId'])['fiscalYear'].nunique().reset_index(name='peocount')    
        

        if (Q4_peo_count['peocount']<extracted_dataItemId_peo_count).any():
            missed_peo_dataItemId=Q4_peo_count[(Q4_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']

            collected_peo=Q4[Q4['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','fiscalYear']]

            Q4_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId))&(historicalData_parsed['periodTypeId']==2) 
                                                  &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                                  & (historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))&~(historicalData_parsed['fiscalYear'].isin(collected_peo['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

            Q4=Q4.append(Q4_missing_data,ignore_index=True)
            

        if H2["dataItemId"].nunique()!=len(left_dataItemId_list):
            H2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['periodTypeId']==10)
                                        &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                        &(historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']] #CAPEX

        H2_peo_count=H2.groupby(['dataItemId'])['fiscalYear'].nunique().reset_index(name='peocount')    
        

        if (H2_peo_count['peocount']<extracted_dataItemId_peo_count).any():
            missed_peo_dataItemId=H2_peo_count[(H2_peo_count['peocount']<extracted_dataItemId_peo_count)]['dataItemId']

            collected_peo=H2[H2['dataItemId'].isin(missed_peo_dataItemId)][['dataItemId','fiscalYear']]

            H2_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(missed_peo_dataItemId))&(historicalData_parsed['periodTypeId']==10) 
                                                  &(historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")
                                                  & (historicalData_parsed['fiscalQuarter']==4)&(historicalData_parsed['fiscalYear'].isin(extractedData_parsed['fiscalYear']))&~(historicalData_parsed['fiscalYear'].isin(collected_peo['fiscalYear'])))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]

            H2=H2.append(H2_missing_data,ignore_index=True)            
   
        


        if (len(FY)>0):
            base_currency=FY.currency.mode()[0]
            FY["value_scaled"] = FY.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            if (len(Q4)>0): 
                Q4["value_scaled"] = Q4.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
            
            if (len(H2)>0):           
                H2["value_scaled"] = H2.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
        

        # FY['tradingItemId']=FY['tradingItemId'].replace('', np.NaN)
        # Q4['tradingItemId']=Q4['tradingItemId'].replace('', np.NaN)
        # H2['tradingItemId']=H2['tradingItemId'].replace('', np.NaN)
        

        merged_df1=pd.merge(FY,Q4,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear','fiscalQuarter'],how='inner')
        merged_df2=pd.merge(FY,H2,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','fiscalYear','fiscalQuarter'],how='inner')
        

        dataItemIds=[]
        FYs=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind,row in merged_df1.iterrows():
            if execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                dataItemIds.append(row['dataItemId'])
                FYs.append(row['fiscalYear'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        
      
        for ind,row in merged_df2.iterrows():
            if execute_operator (row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                dataItemIds.append(row['dataItemId'])
                FYs.append(row['fiscalYear'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
        
        
        diff_df=pd.DataFrame({"fiscalYear":FYs,"diff":diff,"perc":perc,'dataItemId':dataItemIds})
     
        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['fiscalYear'].isin(FYs))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['fiscalYear'].isin(FYs))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Value mismatch for balance sheet dataitems for same period end date"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "Value mismatch for balance sheet dataitems for same period end date"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": row['versionId'],"companyid": row['companyId'], "feedFileId": row['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['diff'].iloc[0], "percent": diff_df[diff_df['fiscalYear']==row["fiscalYear"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

# Estimates Error Checks
@add_method(Validation)
def Different_unis_or_Currency_for_dataItemId_compare_with_other_dataItemIds_in_the_document(historicalData,filingMetadata,extractedData,parameters):
    
    errors = []
    left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters) #BVPS
    right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters) #Other Per share data items
    operator = get_dataItemIds_list('Operation', parameters) #["!="]
    try:
        temp=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]
        temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value','currency','scale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter']]


        merged_df=pd.merge(temp,temp1,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter'])

        
        dataItemIds=[]
        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]
        for ind,row in merged_df.iterrows():
            if (execute_operator(row['scale_x'],row['scale_y'],operator[0])|execute_operator(row['currency_x'],row['currency_y'],operator[0])):
                peos.append(row['peo'])
                dataItemIds.append(row['dataItemId_y'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"peo":peos,"diff":diff,"perc":perc})
        
        temp2 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
        temp3 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))] 
        
        for ind, row in temp2.iterrows():
            result = {"highlights": [], "error": "BVPS units or currency or Bad data collected in Actual or estimate can be detected"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp3.iterrows():
            result = {"highlights": [], "error": "BVPS units or currency or Bad data collected in Actual or estimate can be detected"}
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
def filingDate_greater_than_the_EER_date(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []

    operator = get_dataItemIds_list('Operation', parameters)
    
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!=""))][['dataItemId','peo']]
        temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
        temp['Expected_Earning_release_date']=pd.to_datetime(filingMetadata['metadata']['Expected_Earning_release_date'])
       

        peos=[]
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():
            if execute_operator(row['filingDate'],row['Expected_Earning_release_date'],operator[0]):
                peos.append(row['peo'])             
                difference='NA'
                diff.append(difference)
                perc='NA'
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
        temp1=extractedData_parsed[(extractedData_parsed['peo'].isin(peos))]
       
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Guidance filing date greater than the EER date"}
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
def Guidance_effective_date_is_greater_than_same_actual(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []

    operator = get_dataItemIds_list('Operation', parameters)
    
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemFlag']=='G')&(extractedData_parsed['value']!=""))][['dataItemId','peo','actualizedDate']]
        temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate'])
        temp['actualizedDate']=pd.to_datetime(temp['actualizedDate'])
       
        peos=[]
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():
            if execute_operator(row['filingDate'],row['actualizedDate'],operator[0]):
                peos.append(row['peo'])             
                difference='NA'
                diff.append(difference)
                perc='NA'
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
        temp1=extractedData_parsed[(extractedData_parsed['peo'].isin(peos))]
       
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Guidacne data collected for Actualized PEO"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Check    
@add_method(Validation)
def  Semi_annual_reporting_cycle_for_US_and_Canada_companies(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []

    countryCode=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
    try:
        if filingMetadata['metadata']['country']  in countryCode:
            companyid=filingMetadata['metadata']['companyId']
            temp0 = extractedData_parsed[ ((extractedData_parsed['periodTypeId']==10)&(extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId']]

            history = historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['periodTypeId']==10))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId']]
            
            final = temp0[~(temp0['periodTypeId'].isin(history['periodTypeId']))]
            
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]            
            for ind, row in final.iterrows():
            
                dataItemIds.append(row['dataItemId'])
                peos.append(row['peo'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                diff='NA'
                perc='NA'            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})  
        
            temp1=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds))&(extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            for ind, row in temp1.iterrows():
                               
                result = {"highlights": [], "error": "Semi-annual for US and Canada companies which are not having semi reporting cycle"}
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
def Quartile_HPC_and_More_than_500(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator = get_dataItemIds_list('Operation', parameters)
        Threshold=get_parameter_value(parameters,'Max_Threshold')

        
        try:
            q1=pd.DataFrame()
            q2=pd.DataFrame()
            q3=pd.DataFrame()
            q4=pd.DataFrame()
            q5=pd.DataFrame()
            q6=pd.DataFrame()
            q7=pd.DataFrame()
            q8=pd.DataFrame()
            fy1=pd.DataFrame()
            fy2=pd.DataFrame()
            fy3=pd.DataFrame()
            fy4=pd.DataFrame()
            h1=pd.DataFrame()
            h2=pd.DataFrame()
            h3=pd.DataFrame()
            h4=pd.DataFrame()
            
            if filingMetadata['metadata']['tier']==100:
                companyid=filingMetadata['metadata']['companyId']

                currentpeo = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency','audit_type_id']]

                if len(currentpeo)>0:
                    currentpeo['consValue_scaled']=currentpeo.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                
                    base_currency=currentpeo.currency.mode()[0]
                    currentpeo["consValue_scaled"] = currentpeo.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                    currentpeo['consensusvariation']=((currentpeo[['value_scaled','consValue_scaled']].max(axis=1)-currentpeo[['value_scaled','consValue_scaled']].min(axis=1))/currentpeo[['value_scaled','consValue_scaled']].min(axis=1))*100
                    currentpeo['tradingItemId']=currentpeo['tradingItemId'].replace('',np.NaN)

                for ind,row in currentpeo.iterrows():

                    if row['periodTypeId']==2:
                        if row['fiscalQuarter']==1:
                    
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
                                                                        
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==2:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==3:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==4:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
        
                        if len(q1)>0:
                            q1['consValue_scaled']=q1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            
                        
                            base_currency=q1.currency.mode()[0]
                            q1["consValue_scaled"] = q1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q1['consensusvariation']=((q1[['value_scaled','consValue_scaled']].max(axis=1)-q1[['value_scaled','consValue_scaled']].min(axis=1))/q1[['value_scaled','consValue_scaled']].min(axis=1))*100
                  
                        
                        if len(q2)>0:
                            q2['consValue_scaled']=q2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q2.currency.mode()[0]
                            q2["consValue_scaled"] = q2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q2['consensusvariation']=((q2[['value_scaled','consValue_scaled']].max(axis=1)-q2[['value_scaled','consValue_scaled']].min(axis=1))/q2[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q3)>0:
                            q3['consValue_scaled']=q3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q3.currency.mode()[0]
                            q3["consValue_scaled"] = q3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q3['consensusvariation']=((q3[['value_scaled','consValue_scaled']].max(axis=1)-q3[['value_scaled','consValue_scaled']].min(axis=1))/q3[['value_scaled','consValue_scaled']].min(axis=1))*100
                         
                                
                        if len(q4)>0:
                            q4['consValue_scaled']=q4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q4.currency.mode()[0]
                            q4["consValue_scaled"] = q4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q4['consensusvariation']=((q4[['value_scaled','consValue_scaled']].max(axis=1)-q4[['value_scaled','consValue_scaled']].min(axis=1))/q4[['value_scaled','consValue_scaled']].min(axis=1))*100
                        
            
                        
                        if len(q5)>0:
                            q5['consValue_scaled']=q5.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q5.currency.mode()[0]
                            q5["consValue_scaled"] = q5.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q5['consensusvariation']=((q5[['value_scaled','consValue_scaled']].max(axis=1)-q5[['value_scaled','consValue_scaled']].min(axis=1))/q5[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q6)>0:
                            q6['consValue_scaled']=q6.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q6.currency.mode()[0]
                            q6["consValue_scaled"] = q6.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q6['consensusvariation']=((q6[['value_scaled','consValue_scaled']].max(axis=1)-q6[['value_scaled','consValue_scaled']].min(axis=1))/q6[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q7)>0:
                            q7['consValue_scaled']=q7.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q7.currency.mode()[0]
                            q7["consValue_scaled"] = q7.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q7['consensusvariation']=((q7[['value_scaled','consValue_scaled']].max(axis=1)-q7[['value_scaled','consValue_scaled']].min(axis=1))/q7[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q8)>0:
                            q8['consValue_scaled']=q8.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q8.currency.mode()[0]
                            q8["consValue_scaled"] = q8.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q8['consensusvariation']=((q8[['value_scaled','consValue_scaled']].max(axis=1)-q8[['value_scaled','consValue_scaled']].min(axis=1))/q8[['value_scaled','consValue_scaled']].min(axis=1))*100
    
                        quarters_list=[q1,q2,q3,q4,q5,q6,q7,q8]
    
    
                        quartilerange=pd.concat(quarters_list,join='outer')

    
                        quartilerange['tradingItemId']=quartilerange['tradingItemId'].replace('',np.NaN)
                        if len(quartilerange)>0:
                            quartervalue=quartilerange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'], dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
    

                    if row['periodTypeId']==10:
                        
                        if row['fiscalQuarter']==4:
                            
                            h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            
                            h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                           
                            
                            h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                
                                     
                            h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                           
                        else:
                            
                            h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                            h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        if len(h1)>0:
                            h1['consValue_scaled']=h1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h1.currency.mode()[0]
                            h1["consValue_scaled"] = h1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h1['consensusvariation']=((h1[['value_scaled','consValue_scaled']].max(axis=1)-h1[['value_scaled','consValue_scaled']].min(axis=1))/h1[['value_scaled','consValue_scaled']].min(axis=1))*100           
            
                        if len(h2)>0:
                            h2['consValue_scaled']=h2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h2.currency.mode()[0]
                            h2["consValue_scaled"] = h2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h2['consensusvariation']=((h2[['value_scaled','consValue_scaled']].max(axis=1)-h2[['value_scaled','consValue_scaled']].min(axis=1))/h2[['value_scaled','consValue_scaled']].min(axis=1))*100
            
                        if len(h3)>0:
                            h3['consValue_scaled']=h3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h3.currency.mode()[0]
                            h3["consValue_scaled"] = h3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h3['consensusvariation']=((h3[['value_scaled','consValue_scaled']].max(axis=1)-h3[['value_scaled','consValue_scaled']].min(axis=1))/h3[['value_scaled','consValue_scaled']].min(axis=1))*100
             
                        if len(h4)>0:
                            h4['consValue_scaled']=h4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h4.currency.mode()[0]
                            h4["consValue_scaled"] = h4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h4['consensusvariation']=((h4[['value_scaled','consValue_scaled']].max(axis=1)-h4[['value_scaled','consValue_scaled']].min(axis=1))/h4[['value_scaled','consValue_scaled']].min(axis=1))*100
    
    
                        half_list=[h1,h2,h3,h4]
            

                        halfrange=pd.concat(half_list,join='outer')

                        halfrange['tradingItemId']=halfrange['tradingItemId'].replace('',np.NaN)
                        if len(halfrange)>0:
                            halfvalue=halfrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
   
    
    
                    if row['periodTypeId']==1:
    
                        fy1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
    
                        
                        fy2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))                                                        
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
            
                        
                        fy3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-3))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
              
                        fy4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-4))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
        
            
                        if len(fy1)>0:
                            fy1['consValue_scaled']=fy1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy1.currency.mode()[0]
                            fy1["consValue_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy1['consensusvariation']=((fy1[['value_scaled','consValue_scaled']].max(axis=1)-fy1[['value_scaled','consValue_scaled']].min(axis=1))/fy1[['value_scaled','consValue_scaled']].min(axis=1))*100           
            
                        if len(fy2)>0:
                            fy2['consValue_scaled']=fy2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy2.currency.mode()[0]
                            fy2["consValue_scaled"] = fy2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy2['consensusvariation']=((fy2[['value_scaled','consValue_scaled']].max(axis=1)-fy2[['value_scaled','consValue_scaled']].min(axis=1))/fy2[['value_scaled','consValue_scaled']].min(axis=1))*100
            
                        if len(fy3)>0:
                            fy3['consValue_scaled']=fy3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy3.currency.mode()[0]
                            fy3["consValue_scaled"] = fy3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy3['consensusvariation']=((fy3[['value_scaled','consValue_scaled']].max(axis=1)-fy3[['value_scaled','consValue_scaled']].min(axis=1))/fy3[['value_scaled','consValue_scaled']].min(axis=1))*100
             
                        if len(fy4)>0:
                            fy4['consValue_scaled']=fy4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy4.currency.mode()[0]
                            fy4["consValue_scaled"] = fy4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy4['consensusvariation']=((fy4[['value_scaled','consValue_scaled']].max(axis=1)-fy4[['value_scaled','consValue_scaled']].min(axis=1))/fy4[['value_scaled','consValue_scaled']].min(axis=1))*100
                       
                        fy_list=[fy1,fy2,fy3,fy4]
                        
                        fyrange=pd.concat(fy_list,join='outer')
    
                        fyrange['tradingItemId']=fyrange['tradingItemId'].replace('',np.NaN)
                        #print(fyrange)
                        if len(fyrange)>0:
                            fyvalue=fyrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
    
    
                merged_df=pd.DataFrame()
     
                for ind,row in currentpeo.iterrows():

                    if row['periodTypeId']==2:
                        if len(quartilerange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,quartervalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                    if row['periodTypeId']==1:
                        if len(fyrange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,fyvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                    if row['periodTypeId']==10:
                        if len(halfrange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,halfvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                
                                        
                dataItemIds=[]
                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[]
    
                for ind,row in merged_df.iterrows():

                    if row['audit_type_id']==2057:
                        if execute_operator(row['consensusvariation'],row['quartilevalue'],operator[0]):
                            if execute_operator(row['consensusvariation'],float(Threshold[0]),operator[1]):
                                dataItemIds.append(row['dataItemId'])
                                tid.append(row['tradingItemId']) 
                                parentflag.append(row['parentFlag']) 
                                accounting.append(row['accountingStandardDesc']) 
                                fyc.append(row['fiscalChainSeriesId'])
                                peos.append(row['peo'])               
                                difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                                diff.append(difference)
                                perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
    
                    else:
                        if execute_operator(row['consensusvariation'],row['quartilevalue'],operator[0]):
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
    
        
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                             &(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                 
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": " Quartile HPC and More than 500%"}
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
def Current_Vs_Quartile_greater_than_50_of_0_100(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator = get_dataItemIds_list('Operation', parameters)
        Threshold1=get_parameter_value(parameters,'Min_Threshold')
        Threshold2=get_parameter_value(parameters,'Max_Threshold')

        
        try:
            q1=pd.DataFrame()
            q2=pd.DataFrame()
            q3=pd.DataFrame()
            q4=pd.DataFrame()
            q5=pd.DataFrame()
            q6=pd.DataFrame()
            q7=pd.DataFrame()
            q8=pd.DataFrame()
            fy1=pd.DataFrame()
            fy2=pd.DataFrame()
            fy3=pd.DataFrame()
            fy4=pd.DataFrame()
            h1=pd.DataFrame()
            h2=pd.DataFrame()
            h3=pd.DataFrame()
            h4=pd.DataFrame()
            

            companyid=filingMetadata['metadata']['companyId']

            currentpeo = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency','auditTypeId']]

            if len(currentpeo)>0:
                currentpeo['consValue_scaled']=currentpeo.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            
                base_currency=currentpeo.currency.mode()[0]
                currentpeo["consValue_scaled"] = currentpeo.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                currentpeo['consensusvariation']=((currentpeo[['value_scaled','consValue_scaled']].max(axis=1)-currentpeo[['value_scaled','consValue_scaled']].min(axis=1))/currentpeo[['value_scaled','consValue_scaled']].min(axis=1))*100
                currentpeo['tradingItemId']=currentpeo['tradingItemId'].replace('',np.NaN)

            for ind,row in currentpeo.iterrows():
                #print(row)

                if row['periodTypeId']==2:
                    if row['fiscalQuarter']==1:
                
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
                                                                    
                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==2:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==3:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==4:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                    if len(q1)>0:
                        q1['consValue_scaled']=q1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
        
                    
                        base_currency=q1.currency.mode()[0]
                        q1["consValue_scaled"] = q1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q1['consensusvariation']=((q1[['value_scaled','consValue_scaled']].max(axis=1)-q1[['value_scaled','consValue_scaled']].min(axis=1))/q1[['value_scaled','consValue_scaled']].min(axis=1))*100
              
                    
                    if len(q2)>0:
                        q2['consValue_scaled']=q2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q2.currency.mode()[0]
                        q2["consValue_scaled"] = q2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q2['consensusvariation']=((q2[['value_scaled','consValue_scaled']].max(axis=1)-q2[['value_scaled','consValue_scaled']].min(axis=1))/q2[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q3)>0:
                        q3['consValue_scaled']=q3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q3.currency.mode()[0]
                        q3["consValue_scaled"] = q3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q3['consensusvariation']=((q3[['value_scaled','consValue_scaled']].max(axis=1)-q3[['value_scaled','consValue_scaled']].min(axis=1))/q3[['value_scaled','consValue_scaled']].min(axis=1))*100
                     
                            
                    if len(q4)>0:
                        q4['consValue_scaled']=q4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q4.currency.mode()[0]
                        q4["consValue_scaled"] = q4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q4['consensusvariation']=((q4[['value_scaled','consValue_scaled']].max(axis=1)-q4[['value_scaled','consValue_scaled']].min(axis=1))/q4[['value_scaled','consValue_scaled']].min(axis=1))*100
                    
        
                    
                    if len(q5)>0:
                        q5['consValue_scaled']=q5.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q5.currency.mode()[0]
                        q5["consValue_scaled"] = q5.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q5['consensusvariation']=((q5[['value_scaled','consValue_scaled']].max(axis=1)-q5[['value_scaled','consValue_scaled']].min(axis=1))/q5[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q6)>0:
                        q6['consValue_scaled']=q6.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q6.currency.mode()[0]
                        q6["consValue_scaled"] = q6.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q6['consensusvariation']=((q6[['value_scaled','consValue_scaled']].max(axis=1)-q6[['value_scaled','consValue_scaled']].min(axis=1))/q6[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q7)>0:
                        q7['consValue_scaled']=q7.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q7.currency.mode()[0]
                        q7["consValue_scaled"] = q7.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q7['consensusvariation']=((q7[['value_scaled','consValue_scaled']].max(axis=1)-q7[['value_scaled','consValue_scaled']].min(axis=1))/q7[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q8)>0:
                        q8['consValue_scaled']=q8.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q8.currency.mode()[0]
                        q8["consValue_scaled"] = q8.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q8['consensusvariation']=((q8[['value_scaled','consValue_scaled']].max(axis=1)-q8[['value_scaled','consValue_scaled']].min(axis=1))/q8[['value_scaled','consValue_scaled']].min(axis=1))*100

                    quarters_list=[q1,q2,q3,q4,q5,q6,q7,q8]
                    #print(quarters_list)

                    quartilerange=pd.concat(quarters_list,join='outer')


                    quartilerange['tradingItemId']=quartilerange['tradingItemId'].replace('',np.NaN)
                    if len(quartilerange)>0:

                        quartervalue=quartilerange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'], dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])


                if row['periodTypeId']==10:
                    
                    if row['fiscalQuarter']==4:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
            
                                 
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                    else:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    if len(h1)>0:
                        h1['consValue_scaled']=h1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h1.currency.mode()[0]
                        h1["consValue_scaled"] = h1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h1['consensusvariation']=((h1[['value_scaled','consValue_scaled']].max(axis=1)-h1[['value_scaled','consValue_scaled']].min(axis=1))/h1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(h2)>0:
                        h2['consValue_scaled']=h2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h2.currency.mode()[0]
                        h2["consValue_scaled"] = h2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h2['consensusvariation']=((h2[['value_scaled','consValue_scaled']].max(axis=1)-h2[['value_scaled','consValue_scaled']].min(axis=1))/h2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(h3)>0:
                        h3['consValue_scaled']=h3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h3.currency.mode()[0]
                        h3["consValue_scaled"] = h3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h3['consensusvariation']=((h3[['value_scaled','consValue_scaled']].max(axis=1)-h3[['value_scaled','consValue_scaled']].min(axis=1))/h3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(h4)>0:
                        h4['consValue_scaled']=h4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h4.currency.mode()[0]
                        h4["consValue_scaled"] = h4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h4['consensusvariation']=((h4[['value_scaled','consValue_scaled']].max(axis=1)-h4[['value_scaled','consValue_scaled']].min(axis=1))/h4[['value_scaled','consValue_scaled']].min(axis=1))*100


                    half_list=[h1,h2,h3,h4]


                    halfrange=pd.concat(half_list,join='outer')

                    halfrange['tradingItemId']=halfrange['tradingItemId'].replace('',np.NaN)
                    if len(halfrange)>0:
                        halfvalue=halfrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
   


                if row['periodTypeId']==1:

                    fy1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     

                    
                    fy2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))                                                        
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
        
                    
                    fy3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-3))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
          
                    fy4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-4))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
    
        
                    if len(fy1)>0:
                        fy1['consValue_scaled']=fy1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy1.currency.mode()[0]
                        fy1["consValue_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy1['consensusvariation']=((fy1[['value_scaled','consValue_scaled']].max(axis=1)-fy1[['value_scaled','consValue_scaled']].min(axis=1))/fy1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(fy2)>0:
                        fy2['consValue_scaled']=fy2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy2.currency.mode()[0]
                        fy2["consValue_scaled"] = fy2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy2['consensusvariation']=((fy2[['value_scaled','consValue_scaled']].max(axis=1)-fy2[['value_scaled','consValue_scaled']].min(axis=1))/fy2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(fy3)>0:
                        fy3['consValue_scaled']=fy3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy3.currency.mode()[0]
                        fy3["consValue_scaled"] = fy3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy3['consensusvariation']=((fy3[['value_scaled','consValue_scaled']].max(axis=1)-fy3[['value_scaled','consValue_scaled']].min(axis=1))/fy3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(fy4)>0:
                        fy4['consValue_scaled']=fy4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy4.currency.mode()[0]
                        fy4["consValue_scaled"] = fy4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy4['consensusvariation']=((fy4[['value_scaled','consValue_scaled']].max(axis=1)-fy4[['value_scaled','consValue_scaled']].min(axis=1))/fy4[['value_scaled','consValue_scaled']].min(axis=1))*100
                   
                    fy_list=[fy1,fy2,fy3,fy4]

                    fyrange=pd.concat(fy_list,join='outer')

                    fyrange['tradingItemId']=fyrange['tradingItemId'].replace('',np.NaN)
                    #print(fyrange)
                    if len(fyrange)>0:

                        fyvalue=fyrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])

                    # print(fyvalue)
            merged_df=pd.DataFrame()
 
            for ind,row in currentpeo.iterrows():

                if row['periodTypeId']==2:
                    if len(quartilerange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,quartervalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 

                if row['periodTypeId']==1:
                    if len(fyrange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,fyvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                if row['periodTypeId']==10:
                    if len(halfrange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,halfvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 

            if len(merged_df)>0:
                merged_df['Quartilediff']=merged_df[['consensusvariation','quartilevalue']].max(axis=1)-merged_df[['consensusvariation','quartilevalue']].min(axis=1)
           
                                    
                dataItemIds=[]
                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[]
    
                for ind,row in merged_df.iterrows():

                    if execute_operator(row['consensusvariation'],float(Threshold2[0]),operator[0]):
                
                        if execute_operator(row['Quartilediff'],float(Threshold1[0]),operator[1]):
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
    
        
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                             &(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]                 
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": " Current PEO consensus variation is <100% and Difference between current peo consensus variation and quartile formula variation is >50%"}
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
def Quartile_Review_NHL_Data_items_with_consensus_variation_is_100_500(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator = get_dataItemIds_list('Operation', parameters)
        Threshold1=get_parameter_value(parameters,'Min_Threshold')
        Threshold2=get_parameter_value(parameters,'Max_Threshold')

        
        try:
            q1=pd.DataFrame()
            q2=pd.DataFrame()
            q3=pd.DataFrame()
            q4=pd.DataFrame()
            q5=pd.DataFrame()
            q6=pd.DataFrame()
            q7=pd.DataFrame()
            q8=pd.DataFrame()
            fy1=pd.DataFrame()
            fy2=pd.DataFrame()
            fy3=pd.DataFrame()
            fy4=pd.DataFrame()
            h1=pd.DataFrame()
            h2=pd.DataFrame()
            h3=pd.DataFrame()
            h4=pd.DataFrame()
            

            companyid=filingMetadata['metadata']['companyId']

            currentpeo = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency','auditTypeId']]

            if len(currentpeo)>0:
                currentpeo['consValue_scaled']=currentpeo.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            
                base_currency=currentpeo.currency.mode()[0]
                currentpeo["consValue_scaled"] = currentpeo.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                currentpeo['consensusvariation']=((currentpeo[['value_scaled','consValue_scaled']].max(axis=1)-currentpeo[['value_scaled','consValue_scaled']].min(axis=1))/currentpeo[['value_scaled','consValue_scaled']].min(axis=1))*100
                currentpeo['tradingItemId']=currentpeo['tradingItemId'].replace('',np.NaN)

            for ind,row in currentpeo.iterrows():
                #print(row)

                if row['periodTypeId']==2:
                    if row['fiscalQuarter']==1:
                
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
                                                                    
                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==2:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==3:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]


                    elif row['fiscalQuarter']==4:
                        
                        q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==3)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==1)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]

                        q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                    if len(q1)>0:
                        q1['consValue_scaled']=q1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
        
                    
                        base_currency=q1.currency.mode()[0]
                        q1["consValue_scaled"] = q1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q1['consensusvariation']=((q1[['value_scaled','consValue_scaled']].max(axis=1)-q1[['value_scaled','consValue_scaled']].min(axis=1))/q1[['value_scaled','consValue_scaled']].min(axis=1))*100
              
                    
                    if len(q2)>0:
                        q2['consValue_scaled']=q2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q2.currency.mode()[0]
                        q2["consValue_scaled"] = q2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q2['consensusvariation']=((q2[['value_scaled','consValue_scaled']].max(axis=1)-q2[['value_scaled','consValue_scaled']].min(axis=1))/q2[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q3)>0:
                        q3['consValue_scaled']=q3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q3.currency.mode()[0]
                        q3["consValue_scaled"] = q3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q3['consensusvariation']=((q3[['value_scaled','consValue_scaled']].max(axis=1)-q3[['value_scaled','consValue_scaled']].min(axis=1))/q3[['value_scaled','consValue_scaled']].min(axis=1))*100
                     
                            
                    if len(q4)>0:
                        q4['consValue_scaled']=q4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q4.currency.mode()[0]
                        q4["consValue_scaled"] = q4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q4['consensusvariation']=((q4[['value_scaled','consValue_scaled']].max(axis=1)-q4[['value_scaled','consValue_scaled']].min(axis=1))/q4[['value_scaled','consValue_scaled']].min(axis=1))*100
                    
        
                    
                    if len(q5)>0:
                        q5['consValue_scaled']=q5.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q5.currency.mode()[0]
                        q5["consValue_scaled"] = q5.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q5['consensusvariation']=((q5[['value_scaled','consValue_scaled']].max(axis=1)-q5[['value_scaled','consValue_scaled']].min(axis=1))/q5[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q6)>0:
                        q6['consValue_scaled']=q6.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q6.currency.mode()[0]
                        q6["consValue_scaled"] = q6.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q6['consensusvariation']=((q6[['value_scaled','consValue_scaled']].max(axis=1)-q6[['value_scaled','consValue_scaled']].min(axis=1))/q6[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q7)>0:
                        q7['consValue_scaled']=q7.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q7.currency.mode()[0]
                        q7["consValue_scaled"] = q7.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q7['consensusvariation']=((q7[['value_scaled','consValue_scaled']].max(axis=1)-q7[['value_scaled','consValue_scaled']].min(axis=1))/q7[['value_scaled','consValue_scaled']].min(axis=1))*100
                            
                    if len(q8)>0:
                        q8['consValue_scaled']=q8.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=q8.currency.mode()[0]
                        q8["consValue_scaled"] = q8.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        q8['consensusvariation']=((q8[['value_scaled','consValue_scaled']].max(axis=1)-q8[['value_scaled','consValue_scaled']].min(axis=1))/q8[['value_scaled','consValue_scaled']].min(axis=1))*100

                    quarters_list=[q1,q2,q3,q4,q5,q6,q7,q8]
                    #print(quarters_list)

                    quartilerange=pd.concat(quarters_list,join='outer')


                    quartilerange['tradingItemId']=quartilerange['tradingItemId'].replace('',np.NaN)
                    if len(quartilerange)>0:

                        quartervalue=quartilerange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'], dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])


                if row['periodTypeId']==10:
                    
                    if row['fiscalQuarter']==4:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
            
                                 
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                       
                    else:
                        
                        h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                        h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==4)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    
                        h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                    &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                    &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                    &(historicalData_parsed['fiscalQuarter']==2)
                                                                    &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                    if len(h1)>0:
                        h1['consValue_scaled']=h1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h1.currency.mode()[0]
                        h1["consValue_scaled"] = h1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h1['consensusvariation']=((h1[['value_scaled','consValue_scaled']].max(axis=1)-h1[['value_scaled','consValue_scaled']].min(axis=1))/h1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(h2)>0:
                        h2['consValue_scaled']=h2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h2.currency.mode()[0]
                        h2["consValue_scaled"] = h2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h2['consensusvariation']=((h2[['value_scaled','consValue_scaled']].max(axis=1)-h2[['value_scaled','consValue_scaled']].min(axis=1))/h2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(h3)>0:
                        h3['consValue_scaled']=h3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h3.currency.mode()[0]
                        h3["consValue_scaled"] = h3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h3['consensusvariation']=((h3[['value_scaled','consValue_scaled']].max(axis=1)-h3[['value_scaled','consValue_scaled']].min(axis=1))/h3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(h4)>0:
                        h4['consValue_scaled']=h4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=h4.currency.mode()[0]
                        h4["consValue_scaled"] = h4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        h4['consensusvariation']=((h4[['value_scaled','consValue_scaled']].max(axis=1)-h4[['value_scaled','consValue_scaled']].min(axis=1))/h4[['value_scaled','consValue_scaled']].min(axis=1))*100


                    half_list=[h1,h2,h3,h4]


                    halfrange=pd.concat(half_list,join='outer')

                    halfrange['tradingItemId']=halfrange['tradingItemId'].replace('',np.NaN)
                    if len(halfrange)>0:
                        halfvalue=halfrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
   


                if row['periodTypeId']==1:

                    fy1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     

                    
                    fy2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))                                                        
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
        
                    
                    fy3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-3))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
          
                    fy4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                  &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                  &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                  &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-4))
                                                  &(historicalData_parsed['fiscalQuarter']==4)
                                                  &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
    
        
                    if len(fy1)>0:
                        fy1['consValue_scaled']=fy1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy1.currency.mode()[0]
                        fy1["consValue_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy1['consensusvariation']=((fy1[['value_scaled','consValue_scaled']].max(axis=1)-fy1[['value_scaled','consValue_scaled']].min(axis=1))/fy1[['value_scaled','consValue_scaled']].min(axis=1))*100           
        
                    if len(fy2)>0:
                        fy2['consValue_scaled']=fy2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy2.currency.mode()[0]
                        fy2["consValue_scaled"] = fy2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy2['consensusvariation']=((fy2[['value_scaled','consValue_scaled']].max(axis=1)-fy2[['value_scaled','consValue_scaled']].min(axis=1))/fy2[['value_scaled','consValue_scaled']].min(axis=1))*100
        
                    if len(fy3)>0:
                        fy3['consValue_scaled']=fy3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy3.currency.mode()[0]
                        fy3["consValue_scaled"] = fy3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy3['consensusvariation']=((fy3[['value_scaled','consValue_scaled']].max(axis=1)-fy3[['value_scaled','consValue_scaled']].min(axis=1))/fy3[['value_scaled','consValue_scaled']].min(axis=1))*100
         
                    if len(fy4)>0:
                        fy4['consValue_scaled']=fy4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                    
                        base_currency=fy4.currency.mode()[0]
                        fy4["consValue_scaled"] = fy4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                        fy4['consensusvariation']=((fy4[['value_scaled','consValue_scaled']].max(axis=1)-fy4[['value_scaled','consValue_scaled']].min(axis=1))/fy4[['value_scaled','consValue_scaled']].min(axis=1))*100
                   
                    fy_list=[fy1,fy2,fy3,fy4]

                    fyrange=pd.concat(fy_list,join='outer')

                    fyrange['tradingItemId']=fyrange['tradingItemId'].replace('',np.NaN)
                    #print(fyrange)
                    if len(fyrange)>0:

                        fyvalue=fyrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])

                    # print(fyvalue)
            merged_df=pd.DataFrame()
 
            for ind,row in currentpeo.iterrows():

                if row['periodTypeId']==2:
                    if len(quartilerange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,quartervalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 

                if row['periodTypeId']==1:
                    if len(fyrange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,fyvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                if row['periodTypeId']==10:
                    if len(halfrange)>0:
                        merged_df=merged_df.append(pd.merge(currentpeo,halfvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 

            # if len(merged_df)>0:
            #     merged_df['Quartilediff']=merged_df[['consensusvariation','quartilevalue']].max(axis=1)-merged_df[['consensusvariation','quartilevalue']].min(axis=1)
           
                                    
            dataItemIds=[]
            peos=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]

            for ind,row in merged_df.iterrows():

                if execute_operator(row['consensusvariation'],float(Threshold1[0]),operator[0]):
                    if execute_operator(row['consensusvariation'],float(Threshold2[0]),operator[1]):
                        if execute_operator(row['consensusvariation'],row['quartilevalue'],operator[0]):
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

    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                         &(extractedData_parsed['tradingItemId'].isin(tid))
                                                  &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]                 
             
            for ind, row in temp1.iterrows():
                

                result = {"highlights": [], "error": " NHL Data items with consensus variation is 100-500% and Quartile variation is more than consensus variation"}
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
def Quartile_Review_Non_HPC(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        operator = get_dataItemIds_list('Operation', parameters)
        Threshold1=get_parameter_value(parameters,'Min_Threshold')
        Threshold2=get_parameter_value(parameters,'Max_Threshold')

        
        try:
            q1=pd.DataFrame()
            q2=pd.DataFrame()
            q3=pd.DataFrame()
            q4=pd.DataFrame()
            q5=pd.DataFrame()
            q6=pd.DataFrame()
            q7=pd.DataFrame()
            q8=pd.DataFrame()
            fy1=pd.DataFrame()
            fy2=pd.DataFrame()
            fy3=pd.DataFrame()
            fy4=pd.DataFrame()
            h1=pd.DataFrame()
            h2=pd.DataFrame()
            h3=pd.DataFrame()
            h4=pd.DataFrame()
            
            if filingMetadata['metadata']['tier']!=100:
                companyid=filingMetadata['metadata']['companyId']

                currentpeo = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency','auditTypeId']]

                if len(currentpeo)>0:
                    currentpeo['consValue_scaled']=currentpeo.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                
                    base_currency=currentpeo.currency.mode()[0]
                    currentpeo["consValue_scaled"] = currentpeo.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                    currentpeo['consensusvariation']=((currentpeo[['value_scaled','consValue_scaled']].max(axis=1)-currentpeo[['value_scaled','consValue_scaled']].min(axis=1))/currentpeo[['value_scaled','consValue_scaled']].min(axis=1))*100
                    currentpeo['tradingItemId']=currentpeo['tradingItemId'].replace('',np.NaN)

                for ind,row in currentpeo.iterrows():

                    if row['periodTypeId']==2:
                        if row['fiscalQuarter']==1:
                    
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
                                                                        
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==2:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==3:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
    
                        elif row['fiscalQuarter']==4:
                            
                            q1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q5 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==3)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q6 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q7 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==1)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
    
                            q8 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==2))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]
        
                        if len(q1)>0:
                            q1['consValue_scaled']=q1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            
                        
                            base_currency=q1.currency.mode()[0]
                            q1["consValue_scaled"] = q1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q1['consensusvariation']=((q1[['value_scaled','consValue_scaled']].max(axis=1)-q1[['value_scaled','consValue_scaled']].min(axis=1))/q1[['value_scaled','consValue_scaled']].min(axis=1))*100
                  
                        
                        if len(q2)>0:
                            q2['consValue_scaled']=q2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q2.currency.mode()[0]
                            q2["consValue_scaled"] = q2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q2['consensusvariation']=((q2[['value_scaled','consValue_scaled']].max(axis=1)-q2[['value_scaled','consValue_scaled']].min(axis=1))/q2[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q3)>0:
                            q3['consValue_scaled']=q3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q3.currency.mode()[0]
                            q3["consValue_scaled"] = q3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q3['consensusvariation']=((q3[['value_scaled','consValue_scaled']].max(axis=1)-q3[['value_scaled','consValue_scaled']].min(axis=1))/q3[['value_scaled','consValue_scaled']].min(axis=1))*100
                         
                                
                        if len(q4)>0:
                            q4['consValue_scaled']=q4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q4.currency.mode()[0]
                            q4["consValue_scaled"] = q4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q4['consensusvariation']=((q4[['value_scaled','consValue_scaled']].max(axis=1)-q4[['value_scaled','consValue_scaled']].min(axis=1))/q4[['value_scaled','consValue_scaled']].min(axis=1))*100
                        
            
                        
                        if len(q5)>0:
                            q5['consValue_scaled']=q5.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q5.currency.mode()[0]
                            q5["consValue_scaled"] = q5.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q5['consensusvariation']=((q5[['value_scaled','consValue_scaled']].max(axis=1)-q5[['value_scaled','consValue_scaled']].min(axis=1))/q5[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q6)>0:
                            q6['consValue_scaled']=q6.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q6.currency.mode()[0]
                            q6["consValue_scaled"] = q6.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q6['consensusvariation']=((q6[['value_scaled','consValue_scaled']].max(axis=1)-q6[['value_scaled','consValue_scaled']].min(axis=1))/q6[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q7)>0:
                            q7['consValue_scaled']=q7.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q7.currency.mode()[0]
                            q7["consValue_scaled"] = q7.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q7['consensusvariation']=((q7[['value_scaled','consValue_scaled']].max(axis=1)-q7[['value_scaled','consValue_scaled']].min(axis=1))/q7[['value_scaled','consValue_scaled']].min(axis=1))*100
                                
                        if len(q8)>0:
                            q8['consValue_scaled']=q8.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=q8.currency.mode()[0]
                            q8["consValue_scaled"] = q8.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            q8['consensusvariation']=((q8[['value_scaled','consValue_scaled']].max(axis=1)-q8[['value_scaled','consValue_scaled']].min(axis=1))/q8[['value_scaled','consValue_scaled']].min(axis=1))*100
    
                        quarters_list=[q1,q2,q3,q4,q5,q6,q7,q8]
    
    
                        quartilerange=pd.concat(quarters_list,join='outer')

    
                        quartilerange['tradingItemId']=quartilerange['tradingItemId'].replace('',np.NaN)
                        if len(quartilerange)>0:
                            quartervalue=quartilerange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'], dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
    

                    if row['periodTypeId']==10:
                        
                        if row['fiscalQuarter']==4:
                            
                            h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            
                            h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                           
                            
                            h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                
                                     
                            h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                           
                        else:
                            
                            h1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            h2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                            
                            h3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==4)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        
                            h4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                                        &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                                        &(historicalData_parsed['fiscalQuarter']==2)
                                                                        &(historicalData_parsed['periodTypeId']==10))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
                        if len(h1)>0:
                            h1['consValue_scaled']=h1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h1.currency.mode()[0]
                            h1["consValue_scaled"] = h1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h1['consensusvariation']=((h1[['value_scaled','consValue_scaled']].max(axis=1)-h1[['value_scaled','consValue_scaled']].min(axis=1))/h1[['value_scaled','consValue_scaled']].min(axis=1))*100           
            
                        if len(h2)>0:
                            h2['consValue_scaled']=h2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h2.currency.mode()[0]
                            h2["consValue_scaled"] = h2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h2['consensusvariation']=((h2[['value_scaled','consValue_scaled']].max(axis=1)-h2[['value_scaled','consValue_scaled']].min(axis=1))/h2[['value_scaled','consValue_scaled']].min(axis=1))*100
            
                        if len(h3)>0:
                            h3['consValue_scaled']=h3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h3.currency.mode()[0]
                            h3["consValue_scaled"] = h3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h3['consensusvariation']=((h3[['value_scaled','consValue_scaled']].max(axis=1)-h3[['value_scaled','consValue_scaled']].min(axis=1))/h3[['value_scaled','consValue_scaled']].min(axis=1))*100
             
                        if len(h4)>0:
                            h4['consValue_scaled']=h4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=h4.currency.mode()[0]
                            h4["consValue_scaled"] = h4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            h4['consensusvariation']=((h4[['value_scaled','consValue_scaled']].max(axis=1)-h4[['value_scaled','consValue_scaled']].min(axis=1))/h4[['value_scaled','consValue_scaled']].min(axis=1))*100
    
    
                        half_list=[h1,h2,h3,h4]
            

                        halfrange=pd.concat(half_list,join='outer')

                        halfrange['tradingItemId']=halfrange['tradingItemId'].replace('',np.NaN)
                        if len(halfrange)>0:
                            halfvalue=halfrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
   
    
    
                    if row['periodTypeId']==1:
    
                        fy1 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-1))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
    
                        
                        fy2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))                                                        
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-2))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
            
                        
                        fy3 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-3))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
              
                        fy4 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(currentpeo['dataItemId']))&(historicalData_parsed['companyId']==companyid)
                                                      &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                                      &(historicalData_parsed['periodTypeId'].isin(currentpeo['periodTypeId']))               
                                                      &(historicalData_parsed['fiscalYear'].isin(currentpeo['fiscalYear']-4))
                                                      &(historicalData_parsed['fiscalQuarter']==4)
                                                      &(historicalData_parsed['periodTypeId']==1))]  [['dataItemId','peo','value_scaled','currency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','fiscalQuarter','consValue','consScale','consCurrency']]                     
        
            
                        if len(fy1)>0:
                            fy1['consValue_scaled']=fy1.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy1.currency.mode()[0]
                            fy1["consValue_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy1['consensusvariation']=((fy1[['value_scaled','consValue_scaled']].max(axis=1)-fy1[['value_scaled','consValue_scaled']].min(axis=1))/fy1[['value_scaled','consValue_scaled']].min(axis=1))*100           
            
                        if len(fy2)>0:
                            fy2['consValue_scaled']=fy2.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy2.currency.mode()[0]
                            fy2["consValue_scaled"] = fy2.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy2['consensusvariation']=((fy2[['value_scaled','consValue_scaled']].max(axis=1)-fy2[['value_scaled','consValue_scaled']].min(axis=1))/fy2[['value_scaled','consValue_scaled']].min(axis=1))*100
            
                        if len(fy3)>0:
                            fy3['consValue_scaled']=fy3.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy3.currency.mode()[0]
                            fy3["consValue_scaled"] = fy3.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy3['consensusvariation']=((fy3[['value_scaled','consValue_scaled']].max(axis=1)-fy3[['value_scaled','consValue_scaled']].min(axis=1))/fy3[['value_scaled','consValue_scaled']].min(axis=1))*100
             
                        if len(fy4)>0:
                            fy4['consValue_scaled']=fy4.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
                        
                            base_currency=fy4.currency.mode()[0]
                            fy4["consValue_scaled"] = fy4.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                            fy4['consensusvariation']=((fy4[['value_scaled','consValue_scaled']].max(axis=1)-fy4[['value_scaled','consValue_scaled']].min(axis=1))/fy4[['value_scaled','consValue_scaled']].min(axis=1))*100
                       
                        fy_list=[fy1,fy2,fy3,fy4]
                        
                        fyrange=pd.concat(fy_list,join='outer')
    
                        fyrange['tradingItemId']=fyrange['tradingItemId'].replace('',np.NaN)
                        #print(fyrange)
                        if len(fyrange)>0:
                            fyvalue=fyrange.groupby(['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],dropna=False)['consensusvariation'].agg([('quartilevalue', lambda x: x.quantile(0.75))])
    
    
                merged_df=pd.DataFrame()
     
                for ind,row in currentpeo.iterrows():
                    
                    if row['periodTypeId']==2:
                        if len(quartilerange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,quartervalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                    if row['periodTypeId']==1:
                        if len(fyrange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,fyvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                    if row['periodTypeId']==10:
                        if len(halfrange)>0:
                            merged_df=merged_df.append(pd.merge(currentpeo,halfvalue,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'])) 
                
                                        
                dataItemIds=[]
                peos=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]        
                diff=[]
                perc=[]
    
                for ind,row in merged_df.iterrows():

                    if execute_operator(row['consensusvariation'],float(Threshold1[0]),operator[0]):
                        if execute_operator(row['consensusvariation'],float(Threshold2[0]),operator[1]):
                            if execute_operator(row['consensusvariation'],row['quartilevalue'],operator[0]):
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
        
            
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peos))
                                             &(extractedData_parsed['tradingItemId'].isin(tid))
                                                      &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]                 
                     
                for ind, row in temp1.iterrows():
                    
    
                    result = {"highlights": [], "error": "Non HPC HL Data items with consensus variation is 100-500% and Quartile variation is more than consensus variation"}
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
def Duplicate_guidance_captured_high_value_equal_mid_or_mid_equal_to_low(historicalData,filingMetadata,extractedData,parameters):
        # move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        
        try:

            lhs_df = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(left_dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]
            rhs_df = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                            &(historicalData_parsed['estimatePeriodId'].isin(lhs_df['estimatePeriodId']))&(historicalData_parsed['value']!=""))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]                          

            
            # lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
            # rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN) 
            


            merged_df=pd.merge(lhs_df,rhs_df,on=['parentFlag','peo','accountingStandardDesc','fiscalChainSeriesId','fiscalYear','periodTypeId','tradingItemId'])#,'accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear'],how='inner')

            peos=[]
            dataItemIds_x=[]
            dataItemIds_y=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            diff=[]
            perc=[]
            for ind,row in merged_df.iterrows():
                if execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]):
                    dataItemIds_x.append(row['dataItemId_x'])
                    dataItemIds_y.append(row['dataItemId_y'])
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    diff='NA'
                    perc='NA'
                    peos.append(row['peo'])
            diff_df=pd.DataFrame({"dataItemId_x":dataItemIds_x,'dataItemId_y':dataItemIds_y,"diff":diff,"perc":perc,'peo':peos})

            
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_x)) & (extractedData_parsed['peo'].isin(peos))
                                          &(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
            temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds_y)) &( historicalData_parsed['peo'].isin(peos))&(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
             
            for ind, row in temp1.iterrows():
                if row['value']!=0:
                    
                    result = {"highlights": [], "error": "Duplicate guidance captured high value equal mid or mid equal to low"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
 
            for ind, row in temp2.iterrows():
                if row['value']!=0:
                    result = {"highlights": [], "error": "Duplicate guidance captured high value equal mid or mid equal to low"}
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
def ROA_Actual_value_is_less_than_5_times_to_ROA_consensus(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','consValue','consScale','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
                
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

        temp['multipled_value']=temp['value_scaled']*factor[0]

        peos=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():

            if execute_operator(row['multipled_value'],row['consValue_scaled'],operator[0]):
                peos.append(row['peo']) 
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])
                difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                diff.append(difference)
                
                perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)

        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        # print(diff_df)

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))
                                          &(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "ROA Actual value is less than 5 times to ROA consensus"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "ROA Actual value is less than 5 times to ROA consensus"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def Flavor_collected_for_Banks_and_FIGs(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    industrylist = get_dataItemIds_list('INDUSTRY_INCLUDE', parameters)
    try:
        temp=[]
        if filingMetadata['metadata']['industry'] in industrylist:
            
            temp = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemId_list)&(extractedData_parsed['value']!=""))]
        temp1=pd.DataFrame(temp)    
        peos=[]
        diff=[]
        industry=[]

        for ind,row in temp1.iterrows():
           
            peos.append(row['peo'])               
            difference='NA'
            diff.append(difference)
            
            industry=filingMetadata['metadata']['industry']
      
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"industry":industry})


        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "wrongly collecting some of the data items for Banking and FIG sectors which should not be collected"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "industry": diff_df[diff_df['peo']==row["peo"]]['industry'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "industry": diff_df[diff_df['peo']==row["peo"]]['industry'].iloc[0]}]
            errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def ROE_Actual_value_is_less_than_5_times_to_ROE_consensus(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','consValue','consScale']]
                
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

        temp['multipled_value']=temp['value_scaled']*factor[0]

        peos=[]
        parentflag=[]
        accounting=[]
        fyc=[]        
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():

            if execute_operator(row['multipled_value'],row['consValue_scaled'],operator[0]):
                peos.append(row['peo'])               
                difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                diff.append(difference)
                perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId'])

        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        # print(diff_df)

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))
                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]



        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "collected ROE Actual value is less than 5 times to ROE consensus,"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "collected ROE Actual value is less than 5 times to ROE consensus,"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def comparision_between_two_reporting_Periods(historicalData,filingMetadata,extractedData,parameters):
    # move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    Threshold1=get_parameter_value(parameters,'Max_Threshold')
    try:
      
        companyid=filingMetadata['metadata']['companyId']
        current = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]

        if len(current)>0:
            current['consValue_scaled'] = current.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

            base_currency=current.currency.mode()[0]
            current["consValue_scaled"] = current.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            current['consensusvariation']=((current[['value_scaled','consValue_scaled']].max(axis=1)-current[['value_scaled','consValue_scaled']].min(axis=1))/current[['value_scaled','consValue_scaled']].min(axis=1))*100
            
            
            peos=[]
            diff=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]        
            perc=[]
    
            for ind,row in current.iterrows():
    
                if execute_operator(row['consensusvariation'],float(Threshold[0]),operator[0]):
                    peos.append(row['peo'])               
                    difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                    diff.append(difference)
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId'])
                    perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
    
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})

            
            comparableyear=current['fiscalYear']-1
    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['periodTypeId']==2)
                                          &(extractedData_parsed['tradingItemId'].isin(tid))&(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]


            comparable = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(current['dataItemId']))
                                                &(historicalData_parsed['companyId']==companyid)
                                              &(historicalData_parsed['value']!="")&(historicalData_parsed['consValue']!="")
                                              &(historicalData_parsed['fiscalYear'].isin(comparableyear))
                                              &(historicalData_parsed['periodTypeId']==2)
                                              &(historicalData_parsed['fiscalQuarter'].isin(current['fiscalQuarter'])))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','periodTypeId','fiscalYear','fiscalQuarter','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
            
    
            if len(comparable)>0:
                comparable['consValue_scaled'] = comparable.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
    
                base_currency=comparable.currency.mode()[0]
                comparable["consValue_scaled"] = comparable.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
                comparable['consensusvariation']=((comparable[['value_scaled','consValue_scaled']].max(axis=1)-comparable[['value_scaled','consValue_scaled']].min(axis=1))/comparable[['value_scaled','consValue_scaled']].min(axis=1))*100
               
    
    
                peos1=[]
                
                diff1=[]
                perc1=[]
                
                for ind,row in comparable.iterrows():

                    if execute_operator(row['consensusvariation'],float(Threshold1[0]),operator[1]):
                        peos1.append(row['peo'])               
                        difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                        diff1.append(difference)
                        
                        perc1.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
                
                
                diff_df1=pd.DataFrame({"peo":peos1,"diff":diff1,"perc":perc1})

                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemId_list)) & (historicalData_parsed['peo'].isin(peos1))&(historicalData_parsed['periodTypeId']==2)
                                              &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        
        
                if (len(temp1)>0 and len(temp2)>0):
                    for ind, row in temp1.iterrows():
                        result = {"highlights": [], "error": "EBITDA & EBIT surprise FQ is greater than 200% and FQ-1 is less than 10%"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                        result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result)
                    for ind, row in temp2.iterrows():
                        result = {"highlights": [], "error": "EBITDA & EBIT surprise FQ is greater than 200% and FQ-1 is less than 10%"}
                        result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
                        result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df1[diff_df1['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['peo']==row["peo"]]['perc'].iloc[0]}
                        result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df1[diff_df1['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df1[diff_df1['peo']==row["peo"]]['perc'].iloc[0]}]
                        errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors  

#Estimates Error Checks 
@add_method(Validation)
def Cash_EPS_actual_10_times_greater_than_Mean_for_latest_quarter(historicalData,filingMetadata,extractedData,parameters):
# Where is the mean getting calculated? here mean value means consensus value
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]
        
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
            

            base_currency=temp.currency.mode()[0]
            temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            temp['multipled_value']=temp['consValue_scaled']*factor[0]
            
        
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
            
            for ind,row in temp.iterrows():

                if execute_operator(row['value_scaled'],row['multipled_value'],operator[0]):
                    peos.append(row['peo'])   
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId']) 
                    difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                    diff.append(difference)
                    
                    perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
    
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
    
            temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list)) & (extractedData_parsed['peo'].isin(peos))&(extractedData_parsed['tradingItemId'].isin(tid))
                                          &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
    
    
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "Cash EPS actual 10 times greater than Mean for latest quarter"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "Cash EPS actual 10 times greater than Mean for latest quarter"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def Depreciation_Amortization_actual_for_FQ_10_times_less_than_DA_mean_for_FQ(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    factor=get_parameter_value(parameters,'MultiplicationFactor')
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]
                
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

            base_currency=temp.currency.mode()[0]
            temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            temp['multipled_value']=temp['value_scaled']*factor[0]
    
    
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
    
            for ind,row in temp.iterrows():

                if execute_operator(row['multipled_value'],row['consValue_scaled'],operator[0]):
                    peos.append(row['peo'])   
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId']) 
                    difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                    diff.append(difference)
                    
                    perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
    
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
    
            temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemId_list) & extractedData_parsed['peo'].isin(peos)&extractedData_parsed['tradingItemId'].isin(tid)
                                          &extractedData_parsed['parentFlag'].isin(parentflag)&extractedData_parsed['accountingStandardDesc'].isin(accounting)&extractedData_parsed['fiscalChainSeriesId'].isin(fyc))]
    
    
            for ind, row in temp1.iterrows():
                
                result = {"highlights": [], "error": "Depreciation & Amortization actual for FQ 10 times less than DA mean for FQ"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "Depreciation & Amortization actual for FQ 10 times less than DA mean for FQ"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def A_G_Document_attached_to_More_than_one_Company(historicalData,filingMetadata,extractedData,parameters):
# why the checkGeneratedForList is commented
    # Finalized
    errors = []
    operator = get_dataItemIds_list('Operation', parameters)
    #Threshold = get_dataItemIds_list('Min_Threshold', parameters)
    try:
        currentdocument = filingMetadata['metadata']['versionId']

        history=historicalData_parsed[(historicalData_parsed['versionId']==currentdocument)&(historicalData_parsed['companyId']!=filingMetadata['metadata']['companyId'])][['versionId','companyId']]
        history['company_count']=history['companyId'].nunique()
       
        history.drop_duplicates(inplace=True)

        
        versionid=[]
        companyid=[]
        companiescount=[]        

        for ind,row in history.iterrows():
            
            if execute_operator(row['company_count'],1,operator[0]):
                versionid.append(row['versionId'])
                companyid.append(row['companyId'])
                companiescount.append(row['company_count'])
           
        temp=history[(history['versionId'].isin(versionid)&history['companyId'].isin(companyid))]


        for ind, row in temp.iterrows():
            result = {"highlights": [], "error": "A & G Document attached to More than one Company"}
            result["checkGeneratedFor"]={"statement": "",  "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId']}
            #result["checkGeneratedForList"]={"versionId": row['versionId'], "companyId": row["companyId"], "company_count": row["company_count"]}
            errors.append(result)
            
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def Data_Collected_in_Inactive_trading_item(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    
    try:
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['tradingItemStatus']=='In-Active')&(extractedData_parsed['value']!=""))][['dataItemId','description','securityName','exchangeSymbol','lastTradedDate','tradingItemStatus','tradingItemId','tradingItemName']]
        temp['filingDate']=pd.to_datetime(filingMetadata['metadata']['filingDate']).date()
        
        
        dataItemIds=[]
        tradingitemid=[]
        tradingitem=[]

        for ind,row in temp.iterrows():

            
            if execute_operator(row['filingDate'],pd.to_datetime(row['lastTradedDate']).date(),operator[0]):
                dataItemIds.append(row['dataItemId'])               
                tradingitemid.append(row['tradingItemId'])
                tradingitem.append(row['tradingItemName'])
                
  
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"tradingItemId":tradingitemid,"tradingItemName":tradingitem})

        temp1=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds) &(extractedData_parsed['tradingItemId'].isin(tradingitemid)))]
        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Data Captured in Inactive trading item"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"],  "trading_item_name": diff_df[diff_df['dataItemId']==row["dataItemId"]]['trading_item_name'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"],  "trading_item_name": diff_df[diff_df['dataItemId']==row["dataItemId"]]['trading_item_name'].iloc[0]}]
            errors.append(result)

        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors


#Estimates Error Checks 
@add_method(Validation)
def Actualizeddate_Less_than_or_Equals_to_Periodenddate(historicalData,filingMetadata,extractedData,parameters):
    # Finalized
    errors = []
    operator = get_dataItemIds_list('Operation', parameters)
    try:
        temp = extractedData_parsed[extractedData_parsed['actualizedDate'].notnull()][['peo','actualizedDate','periodEndDate']]
        temp['actualizedDate']=pd.to_datetime(temp['actualizedDate']).dt.normalize()   
        temp['periodEndDate']=pd.to_datetime(temp['periodEndDate']).dt.normalize()                          
        

       
        peos=[]
        diff=[]
        perc=[]

        for ind,row in temp.iterrows():
           
            if execute_operator(row['periodEndDate'],row['actualizedDate'],operator[0]):
                peos.append(row['peo'].strftime('%Y/%m/%d'))            
                difference='NA'
                diff.append(difference)
                perc='NA'
      
       
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
        temp1=extractedData_parsed[(extractedData_parsed['peo'].isin(peos))]

        for ind, row in temp1.iterrows():
            result = {"highlights": [], "error": "Actualizeddate Less than or Equals to Periodenddate"}
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
def Guidance_Initiation(historicalData,filingMetadata,extractedData,parameters):
    #move to testing
    # Finalized
    errors = []
    
    try:
        companyid=filingMetadata['metadata']['companyId']
        temp0 = extractedData_parsed[((extractedData_parsed['dataItemFlag']=="G")&(extractedData_parsed['value']!=""))]
 
        if len(temp0)>0:
            temp1=historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!="")&(historicalData_parsed['dataItemFlag']=="G"))]

            final=temp0[~(temp0['dataItemFlag'].isin(temp1['dataItemFlag']))]
              
                                                                                                                                                                                                     
            peos=[]
            diff=[]
            perc=[]
    
            for ind,row in final.iterrows():
               
                peos.append(row['peo'])               
                difference='NA'
                diff.append(difference)
                perc='NA'
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})        
              
            
            for ind, row in final.iterrows():
                result = {"highlights": [], "error": "Guidance Initiation - Company level"}
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
def DPS_Surprise_percendataItemIde_is_greater_than_500_for_quarter(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold=get_parameter_value(parameters,'Min_Threshold')
    try:
        
        temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]
  
        if len(temp)>0:
            temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)

            base_currency=temp.currency.mode()[0]
            temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
            temp['consensusvariation']=((temp[['value_scaled','consValue_scaled']].max(axis=1)-temp[['value_scaled','consValue_scaled']].min(axis=1))/temp[['value_scaled','consValue_scaled']].min(axis=1))*100
            
            dataItemIds=[]
            peos=[]
            diff=[]
            perc=[]
            tid=[]
            parentflag=[]
            accounting=[]
            fyc=[]
    
            for ind,row in temp.iterrows():

                if execute_operator(row['consensusvariation'],float(Threshold[0]),operator[0]):
                    dataItemIds.append(row['dataItemId'])
                    peos.append(row['peo'])  
                    tid.append(row['tradingItemId']) 
                    parentflag.append(row['parentFlag']) 
                    accounting.append(row['accountingStandardDesc']) 
                    fyc.append(row['fiscalChainSeriesId']) 
                    
                    difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                    diff.append(difference)
                    
                    perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
    
            
            diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
    
    
            temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemId_list) & extractedData_parsed['peo'].isin(peos)&extractedData_parsed['tradingItemId'].isin(tid)
                                          &extractedData_parsed['parentFlag'].isin(parentflag)&extractedData_parsed['accountingStandardDesc'].isin(accounting)&extractedData_parsed['fiscalChainSeriesId'].isin(fyc))]
    
    
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "DPS Surprise percendataItemIde is greater than 500% for quarter"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                errors.append(result)
            for ind, row in temp1.iterrows():
                result = {"highlights": [], "error": "DPS Surprise percendataItemIde is greater than 500% for quarter"}
                result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def DPS_surprise_over_100_for_Special_dividend_declared_companies(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    country=get_dataItemIds_list('COUNTRY_INCLUDE',parameters)
    operator = get_dataItemIds_list('Operation', parameters)
    Threshold = get_parameter_value(parameters,'Min_Threshold')
    try:

        if filingMetadata['metadata']['country'] in country:
            temp = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['periodTypeId']==2)&(extractedData_parsed['value']!="")&(extractedData_parsed['consValue']!=""))][['dataItemId','peo','value_scaled','currency','consValue','consScale','consCurrency','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','estimatePeriodId']]            #consensus_df= extractedData_parsed[extractedData_parsed['dataItemId'].isin(dataItemId_list)&(extractedData_parsed['peo'].isin(actual_df['peo']))][["dataItemId","peo","consValue",'consScale']]
   
            if len(temp)>0:
                temp['consValue_scaled'] = temp.apply(lambda row: get_scaled_value(row['consValue'], row['consScale']), axis=1)
    
                base_currency=temp.currency.mode()[0]
                temp["consValue_scaled"] = temp.apply(lambda x: currency_converter(currency_from=x['consCurrency'], currency_to=base_currency, value=x['consValue_scaled']), axis=1)
  
                      
                dataItemIds=[]
                peos=[]
                diff=[]
                perc=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]
        
                for ind,row in temp.iterrows():
                    if (row['value_scaled']==0 or row['consValue_scaled']==0):
                        row['consensusvariation']=0
                    else:
                        row['consensusvariation']=((row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min())/row[['value_scaled','consValue_scaled']].min())*100
   
                    if execute_operator(row['consensusvariation'],float(Threshold[0]),operator[0]):
                        dataItemIds.append(row['dataItemId'])
                        peos.append(row['peo'])  
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId']) 
              
                        difference=row[['value_scaled','consValue_scaled']].max()-row[['value_scaled','consValue_scaled']].min()
                        diff.append(difference)
                        
                        perc.append((difference/(row[['value_scaled','consValue_scaled']].min()))*100)
        
                
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
        
        
                temp1 = extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemId_list) & extractedData_parsed['peo'].isin(peos)&extractedData_parsed['tradingItemId'].isin(tid)
                                              &extractedData_parsed['parentFlag'].isin(parentflag)&extractedData_parsed['accountingStandardDesc'].isin(accounting)&extractedData_parsed['fiscalChainSeriesId'].isin(fyc))]        
                for ind, row in temp1.iterrows():
                    result = {"highlights": [], "error": "DPS surprise over 100% for Special dividend declared companies"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                for ind, row in temp1.iterrows():
                    result = {"highlights": [], "error": "DPS surprise over 100% for Special dividend declared companies"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "consScale": row['consScale'], "consValue": row['consValue'], "consCurrency": row['consCurrency']}})
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
def guidance_values_are_matching_with_different_periods_for_same_filingDate_documents(historicalData,filingMetadata,extractedData,parameters):
# move to testing
    # Finalized
    errors = []
    dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
    operator=get_dataItemIds_list('Operation', parameters)
    try:
        companyid=filingMetadata['metadata']['companyId']
        current=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))&(extractedData_parsed['value']!=""))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        history=historicalData_parsed[ ((historicalData_parsed['dataItemId'].isin(current['dataItemId']))
                                        &(historicalData_parsed['value']!="")&(historicalData_parsed['companyId']==companyid)
                                        &(historicalData_parsed['filingDate']==filingMetadata['metadata']['filingDate']))][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId']]
        

        #current['tradingItemId']=current['tradingItemId'].replace('',np.NaN)
        #history['tradingItemId']=history['tradingItemId'].replace('',np.NaN) 
        
        merged_df=pd.merge(current,history,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')

        dataItemIds=[]
        diff=[]
        perc=[]
        peosx=[]
        peosy=[]
        tid=[]
        parentflag=[]
        accounting=[]
        fyc=[]
        
        for ind,row in merged_df.iterrows():

            if (execute_operator(row['value_scaled_x'],row['value_scaled_y'],operator[0]) and execute_operator(row['peo_x'],row['peo_y'],operator[1])):
                dataItemIds.append(row['dataItemId'])               
                difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                diff.append(difference)
                perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
                peosx.append(row['peo_x'])
                peosy.append(row['peo_y'])
                tid.append(row['tradingItemId']) 
                parentflag.append(row['parentFlag']) 
                accounting.append(row['accountingStandardDesc']) 
                fyc.append(row['fiscalChainSeriesId']) 
                
        
        diff_df=pd.DataFrame({"dataItemId":dataItemIds,"diff":diff,"perc":perc,'peo_x':peosx,'peo_y':peosy})

   

        temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds)) & (extractedData_parsed['peo'].isin(peosx))&(extractedData_parsed['tradingItemId'].isin(tid))
                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))&(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
        temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['peo'].isin(peosy))&(historicalData_parsed['tradingItemId'].isin(tid))
                                              &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]

        for ind, row in temp1.iterrows():

             
            result = {"highlights": [], "error": "Guidance values are matching with different periods for same filing date documents"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_x']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_x']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)

        for ind, row in temp2.iterrows():

             
            result = {"highlights": [], "error": "Guidance values are matching with different periods for same filing date documents"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo_y']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo_y']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
            
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Checks 
@add_method(Validation)
def currency_initiation(historicalData,filingMetadata,extractedData,parameters):
#move to testing
    # Finalized
    errors = []
    
    try:
        companyid=filingMetadata['metadata']['companyId']
        temp = extractedData_parsed[(extractedData_parsed['value']!="")]
        print(temp[['dataItemId','currency']])
        if len(temp)>0:
            temp1=historicalData_parsed[((historicalData_parsed['companyId']==companyid)&(historicalData_parsed['value']!=""))]
            print(temp1[['dataItemId','currency']])
            final=temp[~(temp['currency'].isin(temp1['currency']))]
        

        dataItemIds=[]
        peos=[]
        diff=[]
        perc=[]
        currency=[]

        for ind,row in final.iterrows():
            currency.append(row['currency'])
            dataItemIds.append(row['dataItemId'])
            peos.append(row['peo'])               
            difference='NA'
            diff.append(difference)
            perc='NA'
        
        diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})        
        
        final1=extractedData_parsed[(extractedData_parsed['dataItemId'].isin(dataItemIds)&extractedData_parsed['peo'].isin(peos)&extractedData_parsed['currency'].isin(currency))]
        
        for ind, row in final1.iterrows():
            #print(row)

            result = {"highlights": [], "error": "Currency initiation @ company level"}
            result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
            result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
            result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
            errors.append(result)
        print(errors)
        return errors
    except Exception as e:
        print(e)
        return errors

#Estimates Error Check         
@add_method(Validation)
def dataItemId_has_lower_value_than_related_dataItemId_with_multiples(historicalData,filingMetadata,extractedData,parameters):
# Move to testing
        # Finalized
        errors = []
        left_dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        right_dataItemId_list=get_dataItemIds_list('RHSdataItemIds', parameters)
        operator=get_dataItemIds_list('Operation', parameters)
        factor=get_parameter_value(parameters,'MultiplicationFactor')
        
        try:
            lhs_df = extractedData_parsed[extractedData_parsed['dataItemId'].isin(left_dataItemId_list)&(extractedData_parsed['value']!="")][['currency','dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
            
            if len(lhs_df)==0:
                lhs_df=historicalData_parsed[(historicalData_parsed['dataItemId'].isin(left_dataItemId_list)) & (historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                              &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))
                                              &(historicalData_parsed['value']!="")][['dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
            
            rhs_df=extractedData_parsed[((extractedData_parsed['dataItemId'].isin(right_dataItemId_list))&(extractedData_parsed['value']!=""))][['currency','dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
           
            if len(rhs_df)==0:
                rhs_df=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo']))
                                              &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))
                                              &(historicalData_parsed['value']!=""))][['currency','dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
                                             

            if lhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                lhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(left_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) 
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))&(historicalData_parsed['value']!="")
                                                          & ~(historicalData_parsed['peo'].isin(lhs_df['peo'])))][['currency','dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
                lhs_df=lhs_df.append(lhs_df_missing_data,ignore_index=True)            


        
            if rhs_df['peo'].nunique()<extractedData_parsed['peo'].nunique():
                rhs_df_missing_data=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(right_dataItemId_list))&(historicalData_parsed['peo'].isin(extractedData_parsed['peo'])) 
                                                          &(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId']))&(historicalData_parsed['value']!="")
                                                          & ~(historicalData_parsed['peo'].isin(rhs_df['peo'])))][['currency','dataItemId','peo','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','estimatePeriodId']]
                rhs_df=rhs_df.append(rhs_df_missing_data,ignore_index=True)
           
            if (len(lhs_df)>0 and len(rhs_df)>0):
                base_currency=lhs_df.currency.mode()[0]
                lhs_df["value_scaled"] = lhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                rhs_df["value_scaled"] = rhs_df.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
     
        
                # lhs_df['tradingItemId']=lhs_df['tradingItemId'].replace('',np.NaN)
                # rhs_df['tradingItemId']=rhs_df['tradingItemId'].replace('',np.NaN)
                
                
                merged_df=pd.merge(lhs_df,rhs_df,on=['peo','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
              
                merged_df['multipled_value']=merged_df['value_scaled_y']*factor[0]
                

                
                peos=[]
                dataItemIds_x=[]
                dataItemIds_y=[]
                tid=[]
                parentflag=[]
                accounting=[]
                fyc=[]
                diff=[]
                perc=[]
                
                for ind,row in merged_df.iterrows():
                    if execute_operator(row['value_scaled_x'],row['multipled_value'],operator[0]):
                        peos.append(row['peo']) 
                        dataItemIds_x.append(row['dataItemId_x'])
                        dataItemIds_y.append(row['dataItemId_y'])
                        tid.append(row['tradingItemId']) 
                        parentflag.append(row['parentFlag']) 
                        accounting.append(row['accountingStandardDesc']) 
                        fyc.append(row['fiscalChainSeriesId'])                        
                        difference=row[['value_scaled_x','value_scaled_y']].max()-row[['value_scaled_x','value_scaled_y']].min()
                        diff.append(difference)
                        
                        perc.append((difference/(row[['value_scaled_x','value_scaled_y']].min()))*100)
    
            
                diff_df=pd.DataFrame({"peo":peos,"diff":diff,"perc":perc})
                
                temp1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemIds_x)) & (extractedData_parsed['peo'].isin(peos))
                                              &(extractedData_parsed['tradingItemId'].isin(tid))
                                              &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))
                                              &(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))
                                              | ((extractedData_parsed['dataItemId'].isin(dataItemIds_y)) & (extractedData_parsed['peo'].isin(peos))
                                                &(extractedData_parsed['parentFlag'].isin(parentflag))&(extractedData_parsed['accountingStandardDesc'].isin(accounting))
                                                &(extractedData_parsed['fiscalChainSeriesId'].isin(fyc)))]
                
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds_x)) & (historicalData_parsed['peo'].isin(peos))
                                              &(historicalData_parsed['tradingItemId'].isin(tid))
                                              &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))
                                              &(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))
                                              | ((historicalData_parsed['dataItemId'].isin(dataItemIds_y)) & (historicalData_parsed['peo'].isin(peos))
                                                &(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))
                                                &(historicalData_parsed['fiscalChainSeriesId'].isin(fyc))&(historicalData_parsed['estimatePeriodId'].isin(extractedData_parsed['estimatePeriodId'])))]

                
                for ind, row in temp1.iterrows():
                    result = {"highlights": [], "error": "EBT GAAP actual 100 times greater than EBT Normalized actual"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['peo']==row["peo"]]['diff'].iloc[0], "percent": diff_df[diff_df['peo']==row["peo"]]['perc'].iloc[0]}]
                    errors.append(result)
                for ind, row in temp2.iterrows():
                    result = {"highlights": [], "error": "EBT GAAP actual 100 times greater than EBT Normalized actual"}
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
def dataItemId_has_greter_value_compare_with_current_and_next_year(historicalData,filingMetadata,extractedData,parameters):
        # Move to testing
        # Finalized
        errors = []
        dataItemId_list=get_dataItemIds_list('LHSdataItemIds', parameters)
        factor=get_parameter_value(parameters,'MultiplicationFactor')
        operator = get_dataItemIds_list('Operation', parameters) 
        try:
            companyid=filingMetadata['metadata']['companyId']
            currentyear=filingMetadata['metadata']['latestPeriodYear']
            fy1 = extractedData_parsed[((extractedData_parsed['dataItemId'].isin(dataItemId_list))
                                        &(extractedData_parsed['fiscalYear']==int(currentyear)+1)
                                        &(extractedData_parsed['periodTypeId']==1)
                                        &(extractedData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']] 
          
            fy0=historicalData_parsed[((historicalData_parsed['dataItemId'].isin(fy1['dataItemId']))
                                      &(historicalData_parsed['companyId']==companyid)
                                            &(historicalData_parsed['fiscalYear']==currentyear)
                                            &(historicalData_parsed['periodTypeId']==1)
                                            &(historicalData_parsed['value']!=""))][['dataItemId','peo','scale','value_scaled','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId','periodTypeId','fiscalYear','currency']]
            
            if (len(fy1)>0 and len(fy0)>0):
                base_currency=fy0.currency.mode()[0]
                fy0["value_scaled"] = fy0.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                
                fy1["value_scaled"] = fy1.apply(lambda x: currency_converter(currency_from=x['currency'], currency_to=base_currency, value=x['value_scaled']), axis=1)
                
                # fy0['tradingItemId']=fy0['tradingItemId'].replace('',np.NaN)
                # fy1['tradingItemId']=fy1['tradingItemId'].replace('',np.NaN) 
                
                merged_df=pd.merge(fy0,fy1,on=['dataItemId','parentFlag','accountingStandardDesc','tradingItemId','fiscalChainSeriesId'],how='inner')
               
                merged_df['multipled_value']=merged_df['value_scaled_x']*factor[0]
    
    
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
                temp2 = historicalData_parsed[((historicalData_parsed['dataItemId'].isin(dataItemIds)) & (historicalData_parsed['fiscalYear']==currentyear)&(historicalData_parsed['periodTypeId']==1)&(historicalData_parsed['companyId']==companyid)
                                              &(historicalData_parsed['tradingItemId'].isin(tid))&(historicalData_parsed['parentFlag'].isin(parentflag))&(historicalData_parsed['accountingStandardDesc'].isin(accounting))&(historicalData_parsed['fiscalChainSeriesId'].isin(fyc)))]    
                
    
                
                for ind,row in temp1.iterrows():
                    
                    result = {"highlights": [], "error": "Revenue High guidance for FY+1 10 times greater than FY+0 guidance"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                    errors.append(result)
                                 
                for ind, row in temp2.iterrows():
                    result = {"highlights": [], "error": "Revenue High guidance for FY+1 10 times greater than FY+0 guidance"}
                    result["highlights"].append({"row": {"name": row['dataItemId'], "id": row["dataItemId"]}, "cell": {"peo": row['peo'], "scale": row['scale'], "value": row['value'], "currency": row['currency']}})
                    result["checkGeneratedFor"]={"statement": "", "dataItemId": row['dataItemId'], "description": row["description"], "versionId": filingMetadata['metadata']['versionId'],"companyid": filingMetadata['metadata']['companyId'], "feedFileId": filingMetadata['metadata']['feedFileId'], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}
                    result["checkGeneratedForList"]=[{"dataItemId": row['dataItemId'], "description": row["description"], "tradingItemId": row["tradingItemId"],"fiscalYear": row["fiscalYear"], "fiscalQuarter":row["fiscalQuarter"], "peo": row["peo"], "diff": diff_df[diff_df['dataItemId']==row["dataItemId"]]['diff'].iloc[0], "percent": diff_df[diff_df['dataItemId']==row["dataItemId"]]['perc'].iloc[0]}]
                    errors.append(result)                
               
            print(errors) 
            return errors                                                                   
        except Exception as e:
            print(e)
            return errors
