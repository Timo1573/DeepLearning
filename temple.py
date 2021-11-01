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


###//...全标签项
sql='''
WITH 基础数据 AS (
SELECT * FROM B2B.dbo.LHH_HistoricalBenchmarkPrices)

SELECT 
 B.全组合项,C.组合项 新组合项,
 SUM(中拍价格)/CAST(SUM(当前基准价) AS FLOAT) 系数
 
FROM 基础数据
LEFT JOIN
B2B.[dbo].[Temporary_20211027_PhoneModelMappingTypeNew] B
ON 基础数据.机型=B.机型
LEFT JOIN
[B2B].[dbo].[Temporary_20211027_Level_D_Up_Report_Type] AS C
ON 基础数据.质检报告ID=C.质检报告ID
WHERE 全组合项 IS NOT NULL
AND C.组合项 IS NOT NULL
GROUP BY B.全组合项,C.组合项
ORDER BY 全组合项 DESC

'''


###//...价格标签
sql1='''
WITH 基础数据 AS (
SELECT * FROM B2B.dbo.LHH_HistoricalBenchmarkPrices)

SELECT 
 B.价格标签,C.组合项 新组合项,
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
GROUP BY B.价格标签,C.组合项
ORDER BY 价格标签 DESC
'''


###//...品牌价位段
sql2='''
WITH 基础数据 AS (
SELECT * FROM B2B.dbo.LHH_HistoricalBenchmarkPrices)

SELECT 
 B.品牌价位段,C.组合项 新组合项,
 SUM(中拍价格)/CAST(SUM(当前基准价) AS FLOAT) 系数
 
FROM 基础数据
LEFT JOIN
B2B.[dbo].[Temporary_20211027_PhoneModelMappingTypeNew] B
ON 基础数据.机型=B.机型
LEFT JOIN
[B2B].[dbo].[Temporary_20211027_Level_D_Up_Report_Type] AS C
ON 基础数据.质检报告ID=C.质检报告ID
WHERE 品牌价位段 IS NOT NULL
AND C.组合项 IS NOT NULL
GROUP BY B.品牌价位段,C.组合项
ORDER BY 品牌价位段 DESC
'''


baojiabilv=pd.read_sql(sql,con=connect)

baojiabilv_rem=pd.read_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\源文件\剩余标签.xls')
baojiabilv_res=pd.merge(baojiabilv_rem,baojiabilv,how='left',on='全组合项')
baojiabilv_res.dropna(axis=0,inplace=True)
baojiabilv_res.reset_index(drop=True,inplace=True)

baojiabilv_mean=pd.pivot_table(baojiabilv_res,values='系数',index='全组合项',columns='新组合项',aggfunc='mean')
baojiabilv_mean=baojiabilv_mean.T
data_1=baojiabilv_mean.corr(method='pearson')
data_1.reset_index(drop=False,inplace=True)
data_2=pd.melt(data_1,id_vars=['全组合项'],var_name='属性',value_name='值')
data_2.replace(np.nan,'',inplace=True)
data_3=data_2[data_2['值']!='']
data_4=data_3[data_3['值']>=0.7]
#data=data[data['值']!=1]
data_5=pd.DataFrame(data_4['全组合项'].groupby(data_4['全组合项']).count())
data_5.rename(columns={'全组合项':'相关量'},inplace=True)
data_5.reset_index(drop=False,inplace=True)
data_6=pd.merge(data_4,data_5,how='left',on='全组合项')
data_6.drop_duplicates(inplace=True)

#data=pd.read_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\源文件\相关性10.18.xlsx',sheet_name='总数据')


data_7=data_6[['全组合项','相关量']]
data_7.drop_duplicates(inplace=True)
data_7.reset_index(inplace=True,drop=True)
data_8=data_6[['全组合项','属性','值']]

correlation_meltcount_Y=pd.DataFrame({'全组合项':pd.Series(np.random.rand(3)),'次元素同类量':pd.Series(np.random.rand(3)),'占总量比':pd.Series(np.random.rand(3))})  #设置初始值，便于下面循环判断时应用
correlation_meltcount_Y1=pd.DataFrame({'全组合项':pd.Series(np.random.rand(3)),'次元素同类量':pd.Series(np.random.rand(3)),'占总量比':pd.Series(np.random.rand(3))}) #设置初始值，便于下面循环判断时应用
n=0
while n<len(data_7) :
    
    m=pd.DataFrame(data_7.loc[n])   # 找到机型对应标签以及其相关量
    m=m.T
    m=m.reset_index(drop=True)

    if m['全组合项'].values in correlation_meltcount_Y1['全组合项'].values: #如果该标签已在前面的分组中，即已成一类，则不仅下以下步骤，如果不在同类中，则进入下列筛选机制
        pass
    else :
        
        k=str(m['全组合项'].values)        #找对机型对应标签
        k_1=k.strip("['") # k是转化标签名为str格式，删除['  ，  '] 便于下面输出excel时对sheet进行命名
        k_2=k_1.strip("']") ####
        m_1=pd.merge(m,data_8,how='left',on='全组合项')  #结合找到某类主元素即主元素所对应的次元素

        m_2=pd.merge(m_1,baojiabilv_res,how='left',on=['全组合项']) #结合找到主元素的报价比率
        m_3=m_2[['属性','值']]  #
        m_3.rename(columns=({'属性':'全组合项'}),inplace=True)  # 对次元素的表名进行重命名
                
#        correlation_meltcount_Y1_test=correlation_meltcount_Y1[['机型对应标签','次元素同类量']]
#        m_3=pd.merge(m_3,correlation_meltcount_Y1_test,how='left',on='机型对应标签')
#        m_3['次元素是否已存在在类中']=['']*len(m_3)
#        m_3.drop_duplicates(inplace=True)
#        m_3.replace(np.nan,0,inplace=True)
#        for i in range(len(m_3)) :
#            if m_3['次元素同类量'][i]==0 :
#                m_3['次元素是否已存在在类中']='不存在'
#            else :
#                m_3['次元素是否已存在在类中']='存在'
        
#        m_3=m_3[m_3['次元素是否已存在在类中']=='不存在']
#        m_3=m_3[['机型对应标签','值']]
#        if m_3 is np.empty:
#            pass
#        else :
#            print('该类不在以上相同类中，放开执行下列内容')
        m_4=pd.merge(m_3,baojiabilv,how='left',on='全组合项') #结合找到次元素的报价比率
        m_5=m_2[['全组合项','新组合项','系数']]
        m_6=m_4[['全组合项','新组合项','系数']]
        m_7=m_5.append(m_6)
        m_8=pd.pivot_table(m_7,values='系数',index='全组合项',columns='新组合项',aggfunc='mean') #常规操作，便于下面计算相关性
        m_8=m_8.T    
        correlation=m_8.corr(method='pearson') #计算相关性
        correlation.reset_index(inplace=True)
        correlation_melt=correlation.melt(id_vars=['全组合项'],var_name='属性',value_name='相关性系数')   #逆透视
        
        correlation_melt['是否同类']=['']*len(correlation_melt) #筛选相关性，并标注
        
        for i in range(len(correlation_melt)) :##
            if correlation_melt['相关性系数'][i]>=0.7:
                correlation_melt['是否同类'][i]='同类'
            else :
                correlation_melt['是否同类'][i]='不同类'
        #correlation_melt.drop_duplicates(inplace=True)
        correlation_meltcount=pd.DataFrame(correlation_melt[correlation_melt['是否同类']=='同类']['是否同类'].groupby(correlation_melt['全组合项']).count()) #找出每个类中对应次元素的量
        correlation_meltcount['占总量比']=correlation_meltcount['是否同类']/(len(m_1))
        correlation_meltcount['占总量比'].replace(np.inf,1,inplace=True)
        correlation_meltcount.reset_index(inplace=True)
        #correlation_meltcount[correlation_meltcount['占总量比']<0.5]['机型对应标签']
        
        correlation_meltcount_Y=correlation_meltcount[correlation_meltcount['占总量比']>=0.5]  #占总量比超过50%，取为该类真实元素
        correlation_meltcount_Y.rename(columns={'是否同类':'次元素同类量'},inplace=True)
        correlation_meltcount_Y.reset_index(drop=True,inplace=True)
        for i in range(len(correlation_meltcount_Y)) :            #次元素量高于最高8时，归类为多元素类，否则是低元素类
            correlation_meltcount_Y.drop_duplicates(inplace=True)
            if correlation_meltcount_Y['次元素同类量'][i]>=5:
                test=pd.DataFrame([k_2]*len(correlation_meltcount_Y))                
                correlation_meltcount_Y.index=test[0]
                correlation_meltcount_Y.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\单文件\%s'%k+'.xlsx',sheet_name='%s'%k_2)
            else :
                test_1=pd.DataFrame([k_2]*len(correlation_meltcount_Y))
                correlation_meltcount_Y.index=test_1[0]
                correlation_meltcount_Y.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\低相关联\%s'%k+'.xlsx',sheet_name='%s'%k_2)
            
            correlation_meltcount_N=correlation_meltcount[correlation_meltcount['占总量比']<0.5]   
                
                
                
        print('正在写入相关性，目前进度为：'+str(n)+'/'+str(len(data_7)))
        
    n+=1
    correlation_meltcount_Y1=correlation_meltcount_Y1.append(correlation_meltcount_Y)    #将所有相关类中的元素集合起来，便于筛选出不属于该类的再做循环，有类的将不会进行到此循环里。
    

path=r'C:\Users\liuyunyun\Desktop\相关性需求\单文件'
df6 = []
for i in os.listdir(path):
    k=0
    name = os.path.join(path,i)#路径+单个文件夹名 为str类型
    
 
    single1 = pd.read_excel(name)
    df6.append(single1)    
    k=+1
 
df6 = pd.concat(df6)
df6=df6.astype(str)

df6.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\合并文件\多相关.xlsx')


path1=r'C:\Users\liuyunyun\Desktop\相关性需求\低相关联'
df7 = []
for i in os.listdir(path1):
    k=0
    name1 = os.path.join(path1,i)#路径+单个文件夹名 为str类型
    
 
    single2 = pd.read_excel(name1)
    df7.append(single2)    
    k=+1
 
df7 = pd.concat(df7)
df7=df7.astype(str)
df7.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\合并文件\低相关.xlsx')

path2=r'C:\Users\liuyunyun\Desktop\相关性需求\合并文件'
df8 = []
for i in os.listdir(path2):
    k=0
    name2 = os.path.join(path2,i)#路径+单个文件夹名 为str类型
    
 
    single3 = pd.read_excel(name2)
    df8.append(single3)    
    k=+1
 
df8 = pd.concat(df8)
df8=df8.astype(str)
df8.rename(columns={'Unnamed: 0.1':'总类标签'},inplace=True)
df8=df8[['总类标签','全组合项','次元素同类量','占总量比']]
df8.drop_duplicates(inplace=True)
df9=pd.DataFrame(df8['总类标签'].groupby(df8['全组合项']).count())
df9.rename(columns={'总类标签':'机型对应标签在不同类中出现次数'},inplace=True)
df9.reset_index(drop=False,inplace=True)
df10=pd.merge(df8,df9,how='left',on='全组合项')
df10_1=pd.DataFrame(df10['总类标签'].groupby(df10['总类标签']).count())
df10_1.rename(columns={'总类标签':'总类含元素量'},inplace=True)
df10_1.reset_index(inplace=True,drop=False)
df10_1.sort_values(by='总类含元素量',ascending=False,inplace=True)
df10=pd.merge(df10_1,df10,how='left',on='总类标签')

df11=pd.DataFrame(df10['总类标签'])
df11.drop_duplicates(inplace=True)
df11=df11.reset_index(drop=True)

###//...对主标签进行再分类，找到主类
r=0
ssr=pd.DataFrame({'全组合项':pd.Series(np.random.rand(3)),'次元素同类量':pd.Series(np.random.rand(3)),'占总量比':pd.Series(np.random.rand(3))}) 
sr=pd.DataFrame({'全组合项':pd.Series(np.random.rand(3)),'次元素同类量':pd.Series(np.random.rand(3)),'占总量比':pd.Series(np.random.rand(3))}) 
t=1
while r<len(df11) :
    s=pd.DataFrame(df11.loc[r])  #找到每次循环的主标签
    s=s.T
    s.reset_index(drop=True)
    
    #brand=baojiabilv[['全组合项','全组合项']] #再分类情况二：品牌价位段分类
    #brand.rename(columns={'全组合项':'总类标签'},inplace=True)
    
    ss=pd.merge(s,df10,how='left',on='总类标签') #再分类情况一：全组合标签分类
    #ss=pd.merge(s,brand,how='left',on='总类标签')
    #ss.drop_duplicates(inplace=True)
    #ss_u=pd.merge(ss,df10,how='left',on='总类标签')
    
    if len(ss) < 5:
        pass
    else :
        sss=ss[['总类标签','全组合项','次元素同类量','总类含元素量','机型对应标签在不同类中出现次数']]
    r+=1
    ssr=ssr.append(sss)   
    sr=sr.append(ss)   
ssr.replace(np.nan,'',inplace=True)     
sssr=ssr[ssr['总类标签']!='']
order=['总类标签','全组合项','总类含元素量','次元素同类量','机型对应标签在不同类中出现次数']
ur=sssr[order]
ur.reset_index(drop=True,inplace=True)
ur.drop_duplicates(inplace=True)
df10.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\相关性.xlsx',index=False,merge_cells=True)
ur.to_excel(r'C:\Users\liuyunyun\Desktop\相关性需求\剩余标签全组合项同类标签相关性_0.70.xlsx',index=False,merge_cells=True)




































