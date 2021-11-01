import os,sys
import pymssql
import pandas as pd
import re
import numpy as np
import numpy as np
import datetime
import xlrd
import xlwt
import warnings
warnings.filterwarnings("ignore")
from scipy import stats
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import pylab

if __name__ == "__main__":
    connect = pymssql.connect(host='172.16.1.25', user='b2bstat', password='Xsb2b2020', database='B2B',charset='utf8')  #建立连接
    if connect:
        print("连接成功!")


    cursor = connect.cursor()
    
sql='''
WITH 基础数据 AS (
SELECT * FROM B2B.dbo.LHH_HistoricalBenchmarkPrices)

SELECT 
基础数据.机型,C.组合项 新组合项,
 SUM(中拍价格)/CAST(SUM(当前基准价) AS FLOAT) 系数
 
FROM 基础数据
LEFT JOIN
B2B.[dbo].[Temporary_20211027_PhoneModelMappingTypeNew] B
ON 基础数据.机型=B.机型
LEFT JOIN
[B2B].[dbo].[Temporary_20211027_Level_D_Up_Report_Type] AS C
ON 基础数据.质检报告ID=C.质检报告ID
WHERE 价格标签 IS NOT NULL
AND C.组合项 IS NOT NULL
GROUP BY 基础数据.机型,C.组合项
ORDER BY 新组合项 DESC
'''

data=pd.read_sql(sql,con=connect)
data_1=pd.read_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\系数大小比较\10.23阶段性等级细分.xls',sheet_name='组合项')
data_2=data_1[['等级','组合项','大小']]
data_2.drop_duplicates(inplace=True)
data_2.reset_index(drop=True,inplace=True)
data_2.rename(columns={'组合项':'新组合项'},inplace=True)
data_3=pd.merge(data,data_2,how='left',on='新组合项')
order=['等级','新组合项','系数','大小']
data_3.drop_duplicates(inplace=True)

data_4=data_3
data_4['条件']=data_4['机型']+data_4['等级']+data_4['新组合项']
data_5=data_4[['条件']]


data_mean=pd.DataFrame(data_3['系数'].groupby([data_3['机型'],data_3['等级'],data_3['新组合项']]).mean())
data_mean.reset_index(drop=False,inplace=True)
data_mean['条件']=data_mean['机型']+data_mean['等级']+data_mean['新组合项']

datadx_mean=pd.DataFrame(data_3['大小'].groupby([data_3['机型'],data_3['等级'],data_3['新组合项']]).mean())
datadx_mean.reset_index(drop=False,inplace=True)
datadx_mean['条件']=datadx_mean['机型']+datadx_mean['等级']+datadx_mean['新组合项']

data_test=pd.merge(data_mean,datadx_mean,how='left',on='条件')

data_use=data_test[['机型_x','等级_x','新组合项_x','系数','大小']]
data_use.rename(columns={'机型_x':'机型','等级_x':'等级','新组合项_x':'新组合项'},inplace=True)
data_use['条件']=data_use['机型']+data_use['等级']+data_use['新组合项']

data_use=pd.merge(data_use,data_5,how='left',on='条件')
data_use=data_use.sort_values(by=['新组合项','等级'],ascending=False)
data_use.drop_duplicates(inplace=True)



data_used=pd.DataFrame(data_use['机型'])
data_used.drop_duplicates(inplace=True)
data_used.reset_index(drop=True,inplace=True)
i=0 

l_1=pd.DataFrame({'机型':pd.Series(np.random.rand(3)),'等级':pd.Series(np.random.rand(3)),'新组合项':pd.Series(np.random.rand(3)),'系数':pd.Series(np.random.rand(3)),'大小':pd.Series(np.random.rand(3))})
while i < len(data_used) :
    k=pd.DataFrame(data_used.loc[i])
    k=k.T
    k_1=k['机型']
    k_1.replace(' ','_',inplace=True)
    k_1['次数']=1
    m=str(k['机型'].values)        #找对机型对应标签
    m_1=m.strip("['") # k是转化标签名为str格式，删除['  ，  '] 便于下面输出excel时对sheet进行命名
    m_2=m_1.strip("']") ####   
    m_2
    l=pd.merge(k_1,data_use,how='left',on='机型')
    l.dropna(axis=0,inplace=True)
    j=0
    l['对比结果']=['']*len(l)
    while j < abs(len(l)-1):
        if l['等级'][j]!=l['等级'][j+1]:
            pass
        else :
            if l['大小'][j+1]<=l['大小'][j] :
                if l['系数'][j+1]<=l['系数'][j]:
                    l['对比结果'][j+1]='小于等于'
                else :
                    l['对比结果'][j+1]='结果异常'
        j+=1
        #l.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\系数大小比较\对比结果\')
    
    i+=1
    l_1=l_1.append(l)
l_1=l_1[3:]
#l_1['条件']=l_1['机型']+l_1['等级']
#l_1=l_1[['机型','等级','新组合项','系数','']]
l_1.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需.xlsx')   
l_1['数字']=1

l_2=pd.pivot_table(l_1,values='数字',index='新组合项',columns='等级',aggfunc='count')
#l_2.replace(np.nan,0,inplace=True)
l_2.reset_index(drop=False,inplace=True)
#l_2=l_2.T
l_2melt=pd.melt(l_2,id_vars='新组合项',var_name='等级',value_name='元素量')
l_2melt.dropna(axis=0,inplace=True)
l_1=l_1[['机型','等级','新组合项','系数','大小','对比结果']]
with pd.ExcelWriter(r'C:\Users\liuyunyun\Desktop\相关性需求\系数大小比较\对比结果\系数比较.xlsx') as writer:
    l_1.to_excel(writer,sheet_name='系数比较',index=False)
    l_2melt.to_excel(writer,sheet_name='元素容纳量',index=False)
#l_2melt.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需_1.xlsx')   



#data_median=pd.DataFrame(data_3['系数'].groupby([data_3['机型'],data_3['等级'],data_3['新组合项']]).median())
#data_median.reset_index(drop=False,inplace=True)




#data_median=pd.DataFrame(data_3['系数'].groupby([data_3['机型'],data_3['等级'],data_3['新组合项']]).median())
#data_median.reset_index(drop=False,inplace=True)
































    
    
    
    
    
















