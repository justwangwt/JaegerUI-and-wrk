
import numpy as np 
import json
import pandas as pd
import openpyxl
from pathlib import Path


with open ('jaeger-and-wrk/traces-1700057624052.json','r',encoding='utf-8') as traces_json:
    data=json.load(traces_json)

    length=len(data['data'][0]['spans'])
    
    child_ID_data=[None]*length
    excl_data=[None]*length
    keys=[None]*length
    values=[[0]* 5 for i in range(length)]
    
    for  i in range(length):
        
        keys[i]=data['data'][0]['spans'][i]['spanID']
        values[i][0]=data['data'][0]['spans'][i]['spanID']
        values[i][1]=data['data'][0]['spans'][i]['operationName']
        values[i][2]=data['data'][0]['spans'][i]['duration']
        values[i][3]=data['data'][0]['spans'][i]['hasChildren']
        
        if data['data'][0]['spans'][i]['hasChildren'] == True:
            child_ID_data[i]=data['data'][0]['spans'][i]['childSpanIds']
        
        
    span_id=keys
    span_dict=dict(zip(keys,values))
    
    for  i in range(length):
        excl_data[i]=span_dict[span_id[i]]
        self_time=span_dict[data['data'][0]['spans'][i]['spanID']][2]
        if data['data'][0]['spans'][i]['hasChildren'] == False:
            span_dict[data['data'][0]['spans'][i]['spanID']][4]=span_dict[data['data'][0]['spans'][i]['spanID']][2]
        else:
            for j in range(len(child_ID_data[i])):
                duration2=self_time
                duration1=duration2-span_dict[child_ID_data[i][j]][2]
                self_time=duration1
            self_time=abs(self_time)
            span_dict[data['data'][0]['spans'][i]['spanID']][4]=self_time

    #print(excl_data)
    trace_id=data['data'][0]['traceID']
    
    #pd.DataFrame(excl_data, columns=["span_ID","operationName","duration","hasChildren","self_time"]).to_excel("C:/Users/31778/Desktop/self_time.xlsx",index=False)
    in_file=Path('C:/Users/31778/Desktop/self_time.xlsx')
    insert='_'+trace_id
    
    out_file=in_file.parent/(in_file.stem+insert+in_file.suffix)
    pd.DataFrame(excl_data, columns=["span_ID","operationName","duration","hasChildren","self_time"]).to_excel(out_file,index=False)
