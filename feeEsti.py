# -*- coding: utf-8 -*-

import pandas as pd
import os
import re

def ZCEDataProcess(excelFile):
    df_tradeData=pd.read_excel(excelFile+os.path.sep+'FutureDataDaily.xls',skiprows=[0])
    df_tradeData=df_tradeData[(df_tradeData[u'品种月份'] != u'小计') & (df_tradeData[u'品种月份'] != u'总计')]
    df_tradeData.set_index(keys=u'品种月份',inplace=True)
    df_tradeData[[u'成交量(手)',u'增减量']]=df_tradeData[[u'成交量(手)',u'增减量']].applymap(lambda x: int(x.replace(',','')))
    df_tradeData[u'开仓']=(df_tradeData[u'成交量(手)']+df_tradeData[u'增减量'])/2
    df_tradeData[u'平仓']=(df_tradeData[u'成交量(手)']-df_tradeData[u'增减量'])/2
    df_volume=df_tradeData[[u'开仓',u'平仓',u'成交量(手)']]
    
    df_clearParams=pd.read_excel(excelFile+os.path.sep+'FutureDataClearParams.xls',skiprows=[0])
    df_clearParams.set_index(keys=u'合约代码',inplace=True)
    df_clearParams=df_clearParams[[u'交易手续费',u'平今仓手续费']]
    df_clearParams[u'估计平仓费率']=(df_clearParams[u'交易手续费']+df_clearParams[u'平今仓手续费'])/2
    
    df_volume.loc[:,'estiFee']=df_volume.apply(lambda x: x.loc[u'开仓']*df_clearParams.loc[x.name,u'交易手续费']+x.loc[u'平仓']*df_clearParams.loc[x.name,u'估计平仓费率'],axis=1)
    
    return df_volume


def DCEDataProcess(excelFile):
    df_tradeData=pd.read_excel(excelFile+os.path.sep+'20190703_Daily.xls')
    df_tradeData=df_tradeData[(df_tradeData[u'商品名称'].map(lambda x:x[-2:]!=u'小计')) & (df_tradeData[u'商品名称'].map(lambda x:x[-2:]!=u'总计'))]
    df_tradeData[u'交割月份']=df_tradeData[u'交割月份'].map(lambda x:str(int(x)))
    df_tradeData.set_index(keys=[u'商品名称',u'交割月份'],inplace=True)
    df_tradeData[[u'成交量',u'持仓量变化']]=df_tradeData[[u'成交量',u'持仓量变化']].applymap(lambda x: int(x.replace(',','')))
    df_tradeData[u'成交额']=df_tradeData[u'成交额'].map(lambda x:float(x.replace(',','')))
    df_tradeData[u'开仓']=(df_tradeData[u'成交量']+df_tradeData[u'持仓量变化'])/2
    df_tradeData[u'平仓']=(df_tradeData[u'成交量']-df_tradeData[u'持仓量变化'])/2
    df_tradeData[u'平仓比例']=df_tradeData.apply(lambda x:0 if x[u'平仓']+x[u'开仓']==0 else x[u'平仓']/(x[u'平仓']+x[u'开仓']),axis=1)
    df_tradeData=df_tradeData[[u'开仓',u'平仓',u'成交量',u'成交额',u'平仓比例']]
    
    df_clearParams=pd.read_excel(excelFile+os.path.sep+'ClearParams_20190703.xls')
    df_clearParams[u'月份']=df_clearParams[u'合约代码'].map(lambda x:str(x)[-4:])
    df_clearParams.set_index(keys=[u'品种',u'月份'],inplace=True)
    df_clearParams[u'平今仓手续费']=df_clearParams[u'短线平仓手续费'].map(float)*2-df_clearParams[u'平仓手续费'].map(float)
    
    df_clearParams=df_clearParams[[u'开仓手续费',u'平仓手续费',u'平今仓手续费',u'手续费收取方式']]
    df_clearParams[[u'开仓手续费',u'平仓手续费',u'平今仓手续费']]=df_clearParams[[u'开仓手续费',u'平仓手续费',u'平今仓手续费']].applymap(float)
    df_clearParams[u'估计平仓费率']=(df_clearParams[u'平仓手续费']+df_clearParams[u'平今仓手续费'])/2
    
    df_tradeData.loc[:,'estiFee']=df_tradeData.apply(lambda x: x.loc[u'开仓']*df_clearParams.loc[x.name,u'开仓手续费']+x.loc[u'平仓']*df_clearParams.loc[x.name,u'估计平仓费率'] 
                                                     if df_clearParams.loc[x.name,u'手续费收取方式']==u'绝对值' 
                                                     else (x.loc[u'成交额']*10000*x.loc[u'平仓比例']*df_clearParams.loc[x.name,u'估计平仓费率']+x.loc[u'成交额']*10000*(1-x.loc[u'平仓比例'])*df_clearParams.loc[x.name,u'开仓手续费'])/10000,axis=1)
    
    df_tradeData.rename(columns={u'成交额':u'成交额（万元）'},inplace=True)
    return df_tradeData


def SHFEDataProcess(csvFile):
    
    dic={'cu': [u'铜',5],
         'al': [u'铝',5],
         'zn': [u'锌',5],
         'pb': [u'铅',5],
         'ni': [u'镍',1],
         'sn': [u'锡',1],
         'sp': [u'纸浆',10],
         'au': [u'黄金',1000],
         'ag': [u'白银',15],
         'rb': [u'螺纹钢',10],
         'wr': [u'线材',10],
         'hc': [u'热轧卷板',10],
         'sc': [u'原油',1000],
         'fu': [u'燃料油',10],
         'bu': [u'石油沥青',10],
         'ru': [u'天然橡胶',10]
         }
    
    file=csvFile+os.path.sep+r'20190703_Daily.csv'
    l_df=[]
    with open(file) as f:
        l=[]
        for line in f:
            if line == '\n':
                continue
            if u'交割月份' in line:
                columns=re.split('[,\\n/]',line)[:-2]
                continue
            if u'商品名称' in line:
                product=re.split('[:,]',line)[1].strip()
                continue
            if u'小计' in line:
                df_tmp=pd.DataFrame(l)
                df_tmp.columns=columns
                df_tmp1=df_tmp[[u'结算参考价',u'成交手',u'变化']].applymap(float)
                df_tmp1[u'交割月份']=df_tmp[u'交割月份'].map(str)
                df_tmp1[u'商品名称']=[product]*len(df_tmp1)
                l_df.append(df_tmp1)
                l=[]
                continue
            if u'总计' in line:
                break
            else:
                row=re.split('[,]',line)[:-1]
                l.append(row)
    
    df_tradeData=pd.concat(l_df)
    
    dic1={x[0]:x[1] for x in dic.values()}
    df_tradeData[u'乘数']=df_tradeData[u'商品名称'].map(lambda x:dic1[x] )
    df_tradeData.set_index(keys=[u'商品名称',u'交割月份'],inplace=True)
    
    df_tradeData[u'成交额']=df_tradeData[u'成交手']*df_tradeData[u'结算参考价']*df_tradeData[u'乘数']
    df_tradeData[u'开仓']=(df_tradeData[u'成交手']+df_tradeData[u'变化'])/2
    df_tradeData[u'平仓']=(df_tradeData[u'成交手']-df_tradeData[u'变化'])/2
    df_tradeData[u'平仓比例']=df_tradeData.apply(lambda x:0 if x[u'平仓']+x[u'开仓']==0 else x[u'平仓']/(x[u'平仓']+x[u'开仓']),axis=1)
    df_tradeData=df_tradeData[[u'开仓',u'平仓',u'成交手',u'成交额',u'平仓比例']]
    
    df_clearParams=pd.read_csv(csvFile+os.path.sep+'ClearParams.csv',skiprows=[0,1,2],encoding='gb2312')
    unnamed_columns=[x for x in df_clearParams.columns if 'Unnamed' in x]
    df_clearParams.drop(columns=unnamed_columns,inplace=True)
    df_clearParams.columns=[x.strip() for x in df_clearParams.columns]
    df_clearParams[u'月份']=df_clearParams[u'合约代码'].map(lambda x:str(x)[-4:])
    df_clearParams[u'商品名称']=df_clearParams[u'合约代码'].map(lambda x: dic[x[:-4]][0])
    df_clearParams.set_index(keys=[u'商品名称',u'月份'],inplace=True)
    
    df_tradeData.loc[:,'estiFee']=df_tradeData.apply(lambda x:x.loc[u'成交额']*(1-x.loc[u'平仓比例'])*df_clearParams.loc[x.name,u'交易手续费率(‰)']/1000 
                                                              + x.loc[u'成交额']*x.loc[u'平仓比例']*(1+df_clearParams.loc[x.name,u'平今折扣率(%)']/100)/2*df_clearParams.loc[x.name,u'交易手续费率(‰)']/1000
                                                              + x.loc[u'开仓']*df_clearParams.loc[x.name,u'交易手续费额(元/手)']
                                                              + x.loc[u'平仓']*(1+df_clearParams.loc[x.name,u'平今折扣率(%)']/100)/2*df_clearParams.loc[x.name,u'交易手续费额(元/手)'],axis=1)
    
    return df_tradeData

if __name__=='__main__':
    shfe_csvFile=r'shfe_path'
    dce_csvFile=r'dce_path'
    zce_csvFile=r'zce_path'
    shfe=SHFEDataProcess(shfe_csvFile)
    dce=DCEDataProcess(dce_csvFile)
    zce=ZCEDataProcess(csvFile)
