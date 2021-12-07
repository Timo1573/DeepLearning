import pandas as pd
import numpy as np
import os
import datetime
import pymssql
import shutil



data=pd.read_excel(r'C:\Users\liuyunyun\Desktop\系数计算\数据验证表.xlsx')
ReportCode=data
ReportCode=ReportCode.dropna(axis=0)


if __name__ == "__main__":
    connect = pymssql.connect(host='172.16.1.25', user='b2bstat', password='Xsb2b2020', database='B2B',charset='utf8')  #建立连接
    if connect:
        print("连接成功!")
        
    cursor=connect.cursor()



sql='''
SELECT A.*,B.当前基准价 FROM B2B.dbo.LHH_XSJSS A
LEFT JOIN (SELECT CAST(A.修改时间 AS DATE) 修改时间,A.机型名称,A.SKUID ,B.当前基准价
				FROM (
					SELECT DISTINCT 修改时间,机型名称,SKUID,ROW_NUMBER()OVER(PARTITION BY 机型名称+SKUID ORDER BY 修改时间) 序号
						FROM (
							SELECT DISTINCT 修改时间,机型名称,CAST(SKUID AS NVARCHAR) SKUID
								FROM
									GumaBusiness.dbo.PaixpSkuLReferencePriceChgLog
							WHERE CAST(修改时间 AS DATE)>='2021-09-01'
							) A
						) A
			LEFT JOIN (SELECT 修改时间,机型名称,CAST(SKUID AS NVARCHAR) SKUID,当前基准价
							FROM 
								GumaBusiness.dbo.PaixpSkuLReferencePriceChgLog
						WHERE CAST(修改时间 AS DATE)>='2021-09-01'
						) B
			ON A.修改时间=B.修改时间 AND A.机型名称=B.机型名称 AND A.SKUID=B.SKUID
			WHERE 序号=1			
			) B
ON A.SKU编号=B.SKUID AND A.机型名称=B.机型名称
WHERE 修改时间<=CAST(场次创建时间 AS DATE)
AND A.机型名称='华为 P40 Pro（5G版）'
'''

PriceForBidding=pd.read_sql(sql,con=connect)
PriceForBidding.dropna(axis=0,inplace=True)
cols= [x for x in PriceForBidding.columns if x!='质检等级']
usedata=pd.merge(ReportCode,PriceForBidding[cols],how='left',on='质检报告ID')
#usedata.drop_duplicates(inplace=True)

BiddingData=usedata[['机型名称','SKU编号','质检报告ID','非完美项&系数','大小系数','非完美项','分类2','质检等级','系数3','物品参考价','物品起拍价','中拍金额','当前基准价','有效出价数']][usedata['中拍金额']!=0]
BiddingData['是否保留'] = ['']*len(BiddingData)
BiddingData.reset_index(drop=True,inplace=True)
for i in range(len(BiddingData)) :
    if BiddingData['物品起拍价'][i]-BiddingData['物品参考价'][i]>150 :
        if BiddingData['中拍金额'][i]-BiddingData['物品参考价'][i]>200 :
            BiddingData['是否保留'][i]=0
        else :
            BiddingData['是否保留'][i]=1
    else :
        if BiddingData['有效出价数'][i]<3 :
            if BiddingData['中拍金额'][i]-BiddingData['物品参考价'][i]>200 :
                BiddingData['是否保留'][i]=0
            else :
                BiddingData['是否保留'][i]=1
        else :
            BiddingData['是否保留'][i]=1
        
BiddingData_use=BiddingData[BiddingData['是否保留']==1]
BiddingData_use.reset_index(drop=True,inplace=True)  #数据源1
BiddingData_use['溢价']=BiddingData_use['中拍金额']-BiddingData_use['物品参考价']
BiddingData_use['溢价率']=round(BiddingData_use['溢价']/BiddingData_use['物品参考价'],3)
BiddingData_use.dropna(axis=0,inplace=True)
BiddingData_use.reset_index(drop=True,inplace=True)


print(BiddingData_use.columns)

for i in range(len(BiddingData_use)) :
    if BiddingData_use['中拍金额'][i]-BiddingData_use['物品参考价'][i]>=0:
        if BiddingData_use['质检等级'][i] in ('A','A-','A+','B++','B+','B','B-') :
            if BiddingData_use['物品参考价'][i]<=1500:
                if BiddingData_use['溢价率'][i]>0.1 :
                    BiddingData_use['溢价率'][i]=0.1
                elif BiddingData_use['溢价'][i]>100 :
                    BiddingData_use['溢价'][i]=100
            elif 1500<=BiddingData_use['物品参考价'][i]<2500 :
                if BiddingData_use['溢价率'][i]>0.08:
                    BiddingData_use['溢价率'][i]=0.08
                elif BiddingData_use['溢价'][i]>100 :
                    BiddingData_use['溢价'][i]=100
            elif 2500<=BiddingData_use['物品参考价'][i]<5000 :
                if BiddingData_use['溢价率'][i]>0.05:
                    BiddingData_use['溢价率'][i]=0.05
                elif BiddingData_use['溢价'][i]>200 :
                    BiddingData_use['溢价'][i]=200
            else :
                if BiddingData_use['溢价率'][i]>0.04:
                    BiddingData_use['溢价率'][i]=0.04
                elif BiddingData_use['溢价'][i]>250 :
                    BiddingData_use['溢价'][i]=250
        else :
            if BiddingData_use['物品参考价'][i]<=1500:
                if BiddingData_use['溢价率'][i]>0.12 :
                    BiddingData_use['溢价率'][i]=0.12
                elif BiddingData_use['溢价'][i]>150 :
                    BiddingData_use['溢价'][i]=150
            elif 1500<=BiddingData_use['物品参考价'][i]<2500 :
                if BiddingData_use['溢价率'][i]>0.1:
                    BiddingData_use['溢价率'][i]=0.1
                elif BiddingData_use['溢价'][i]>200 :
                    BiddingData_use['溢价'][i]=200
            elif 2500<=BiddingData_use['物品参考价'][i]<5000 :
                if BiddingData_use['溢价率'][i]>0.06:
                    BiddingData_use['溢价率'][i]=0.06
                elif BiddingData_use['溢价'][i]>250 :
                    BiddingData_use['溢价'][i]=250
            else :
                if BiddingData_use['溢价率'][i]>0.05:
                    BiddingData_use['溢价率'][i]=0.05
                elif BiddingData_use['溢价'][i]>300 :
                    BiddingData_use['溢价'][i]=300            
    else :
        if BiddingData_use['物品参考价'][i]<=1500:
            if abs(BiddingData_use['溢价率'][i])>0.1 :
                BiddingData_use['溢价率'][i]=-0.1
            elif abs(BiddingData_use['溢价'][i])>100 :
                BiddingData_use['溢价'][i]=-100
        elif 1500<=BiddingData_use['物品参考价'][i]<2500 :
            if abs(BiddingData_use['溢价率'][i])>0.08:
                BiddingData_use['溢价率'][i]=-0.08
            elif abs(BiddingData_use['溢价'][i])>100 :
                BiddingData_use['溢价'][i]=-100
        elif 2500<=BiddingData_use['物品参考价'][i]<5000 :
            if abs(BiddingData_use['溢价率'][i])>0.05:
                BiddingData_use['溢价率'][i]=-0.05
            elif abs(BiddingData_use['溢价'][i])>200 :
                BiddingData_use['溢价'][i]=-200
        else :
            if abs(BiddingData_use['溢价率'][i])>0.04:
                BiddingData_use['溢价率'][i]=0.04
            elif abs(BiddingData_use['溢价'][i])>250 :
                BiddingData_use['溢价'][i]=250

BiddingData_use
BiddingData_use['溢价反推']=(BiddingData_use['物品参考价']+BiddingData_use['溢价']).astype(int)
BiddingData_use['溢价率反推']=(BiddingData_use['物品参考价']+BiddingData_use['物品参考价']*BiddingData_use['溢价率']).astype(int)

for i in range(len(BiddingData_use)) :
    if BiddingData_use['溢价反推'][i]<=BiddingData_use['溢价反推'][i]:
        BiddingData_use['中拍金额'][i]=BiddingData_use['溢价反推'][i]
    else :
        BiddingData_use['中拍金额'][i]=BiddingData_use['溢价率反推'][i]

cols=[i for i in BiddingData_use.columns if i not in ['溢价','溢价率','溢价反推','溢价率反推','是否保留']]
BiddingData_used=BiddingData_use[cols]

BiddingData_used['报价比率']=round(BiddingData_used['中拍金额']/BiddingData_used['当前基准价'],3)
BiddingData_used.columns

#BiddingData_used['条件']=BiddingData_used['SKUID']=BiddingData_used['质检报告ID']



#纯系数3
#Mean_data_all=BiddingData_used[['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False)
#Mean_data_all_1=BiddingData_used[BiddingData_used['非完美项']==1]['报价比率'].groupby(BiddingData_used['系数3']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_1'})
#Mean_data_all_2=BiddingData_used[BiddingData_used['非完美项']==2]['报价比率'].groupby(BiddingData_used['系数3']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_2'})
#Mean_data_all_3=BiddingData_used[BiddingData_used['非完美项']==3]['报价比率'].groupby(BiddingData_used['系数3']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_3'})
#Mean_data_all_4=BiddingData_used[BiddingData_used['非完美项']==4]['报价比率'].groupby(BiddingData_used['系数3']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_4'})



#系数3+质检等级
#Mean_data_all=BiddingData_used[['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False)
#Mean_data_all_1=BiddingData_used[BiddingData_used['非完美项']==1][['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_1'})
#Mean_data_all_2=BiddingData_used[BiddingData_used['非完美项']==2][['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_2'})
#Mean_data_all_3=BiddingData_used[BiddingData_used['非完美项']==3][['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_3'})
#Mean_data_all_4=BiddingData_used[BiddingData_used['非完美项']==4][['系数3','质检等级','报价比率']].groupby(['系数3','质检等级']).mean().reset_index(drop=False).rename(columns={'报价比率':'报价比率_4'})



#BiddingData_used_all=pd.merge(BiddingData_used[cols],Mean_data_all,how='left',on=['系数3','质检等级'])
#BiddingData_used_all_1=pd.merge(BiddingData_used[BiddingData_used['非完美项']==1][cols],Mean_data_all_1,how='left',on=['系数3','质检等级'])
#BiddingData_used_all_2=pd.merge(BiddingData_used[BiddingData_used['非完美项']==2][cols],Mean_data_all_2,how='left',on=['系数3','质检等级'])
#BiddingData_used_all_3=pd.merge(BiddingData_used[BiddingData_used['非完美项']==3][cols],Mean_data_all_3,how='left',on=['系数3','质检等级'])
#BiddingData_used_all_4=pd.merge(BiddingData_used[BiddingData_used['非完美项']==4][cols],Mean_data_all_4,how='left',on=['系数3','质检等级'])


#BiddingData_used_all_4.drop_duplicates(inplace=True)

ModelData=pd.read_excel(r'C:\Users\liuyunyun\Desktop\系数计算\新进度.xlsx',sheet_name='操作业')

FinalData=pd.merge(BiddingData_used,ModelData,how='left',on='机型名称')
FinalData.dropna(axis=0,inplace=True)
cols = [x for x in FinalData.columns if x in ['机型品牌','机型名称','组合项','新标签','质检等级','非完美项','系数3']]

Mean_data_all=FinalData[['新标签','系数3','质检等级','报价比率']].groupby(['系数3','新标签','质检等级']).mean().reset_index(drop=False)
AllFordata_all=pd.merge(FinalData[cols],Mean_data_all,how='right',on=['系数3','新标签','质检等级'])
AllFordata_all.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等+标_all.xlsx',index=False)



Mean_data_1=FinalData[FinalData['非完美项']==1][['新标签','系数3','质检等级','报价比率']].groupby(['系数3','新标签','质检等级']).mean().reset_index(drop=False)
AllFordata_1=pd.merge(FinalData[cols],Mean_data_1,how='right',on=['系数3','新标签','质检等级'])
AllFordata_1.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等+标_1.xlsx',index=False)



Mean_data_2=FinalData[FinalData['非完美项']==2][['新标签','系数3','质检等级','报价比率']].groupby(['系数3','新标签','质检等级']).mean().reset_index(drop=False)
AllFordata_2=pd.merge(FinalData[cols],Mean_data_2,how='right',on=['系数3','新标签','质检等级'])
AllFordata_2.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等+标_2.xlsx',index=False)




Mean_data_3=FinalData[FinalData['非完美项']==3][['新标签','系数3','质检等级','报价比率']].groupby(['系数3','新标签','质检等级']).mean().reset_index(drop=False)
AllFordata_3=pd.merge(FinalData[cols],Mean_data_3,how='right',on=['系数3','新标签','质检等级'])
AllFordata_3.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等+标_3.xlsx',index=False)


Mean_data_4=FinalData[FinalData['非完美项']==4][['新标签','系数3','质检等级','报价比率']].groupby(['系数3','新标签','质检等级']).mean().reset_index(drop=False)
AllFordata_4=pd.merge(FinalData[cols],Mean_data_4,how='right',on=['系数3','新标签','质检等级'])
AllFordata_4.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等+标_4.xlsx',index=False)


#BiddingData_used_all.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等_all.xlsx')
#BiddingData_used_all_1.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等_1.xlsx')
#BiddingData_used_all_2.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等_2.xlsx')
#BiddingData_used_all_3.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等_3.xlsx')
#BiddingData_used_all_4.to_excel(r'C:\Users\liuyunyun\Desktop\新进度结果_系+等_4.xlsx')


verify=pd.DataFrame(data['系数3'].drop_duplicates().reset_index(drop=True))

value=pd.DataFrame([x for x in verify['系数3'].values if x not in AllFordata_all['系数3'].values]).rename(columns={0:'系数3'})
midata=pd.merge(value,data,how='left',on='系数3').drop_duplicates().reset_index(drop=True)
midata_=pd.merge(midata,ModelData,how='left',on='机型名称')
midata_1=midata[['系数3','非完美项','质检等级']]
midata_2=AllFordata_all.append(midata_1).sort_values(by=['非完美项','质检等级'],ascending=True).reset_index(drop=True)

#if pd.isna(midata_2['报价比率'][10])==True:
#    print('True')
#else:
#    print('False')


def early(data,col) :
    '''定义头部位置'''
    m=[]
    n=[]
    i=0
    j=0
    while i < len(data)-1 :
#        if pd.isna(data[col][0]) == False :            
        if pd.isna(data[col][i])==False:
            if pd.isna(data[col][i+1])==True :
                
                m.append(i+1)
                n.append(data[col][i])
            else :
                pass
        else :
            pass
#        else :
#            m.append(1)
#            n.append(0.1)
#            if pd.isna(data[col][i])==False:
#                if pd.isna(data[col][i+1])==True :
                    
#                    m.append(i+1)
#                    n.append(data[col][i])
#                else :
#                    pass
#            else :
#                pass
        i+=1


    return m,n
col='报价比率'


def late(data,col) :
    '''定义尾部位置'''
    s=[]
    t=[]
    j=0
    while j < len(data)-1 :
        if pd.isna(data[col][j])==True:
            if pd.isna(data[col][j+1])==False:
                
                s.append(j)
                t.append(data[col][j+1])
            else :
                pass
        else :
            pass
        j+=1    
    return s,t

if __name__ == '__main__':
    earlier=early(midata_2,col)
    later=late(midata_2,col)


head=pd.DataFrame(earlier).T.rename(columns={0:'头部编码',1:'头部值'})
foot=pd.DataFrame(later).T.rename(columns={0:'尾部编码',1:'尾部值'})

if len(head)>len(foot) :
    head.drop(index=25,inplace=True)
else :
    pass
    
    

math_data=pd.merge(head,foot,how='left',on=head.index).drop(columns='key_0')


series =[]
series1 = []
i=0
while i < len(math_data)-1:
    series1=pd.DataFrame(np.linspace(np.array(math_data['头部值'][i]),np.array(math_data['尾部值'][i]),num=int((math_data['尾部编码'][i]-math_data['头部编码'][i])))).rename(columns={0:'报价比率'})
    series1.index=range(int(math_data['头部编码'][i]),int(math_data['尾部编码'][i]))
    print('正在进行中............'+'///现在是第'+str(i)+'条')
    series1.to_excel(r'C:\Users\liuyunyun\Desktop\系数计算\单个系数\%s'%i+'.xlsx')
    i+=1
    series.append(series1)
    
        
path=r'C:\Users\liuyunyun\Desktop\系数计算\单个系数'
df6 = []
for i in os.listdir(path):
    k=0
    name = os.path.join(path,i)#路径+单个文件夹名 为str类型
    
 
    single1 = pd.read_excel(name)
    df6.append(single1)    
    k=+1
 
df6 = pd.concat(df6)
#df6=df6.astype(str)

df6.to_excel(r'C:\Users\liuyunyun\Desktop\系数计算\总文件.xlsx')

df6.rename(columns={'Unnamed: 0':'序号'},inplace=True)

midata_3=midata_2.reset_index(drop=False).rename(columns={'index':'序号'})
midata_4=midata_3[['序号','系数3']]
midata_5=pd.merge(df6,midata_4,how='left',on='序号')

all_=midata_2.reset_index(drop=False).rename(columns={'index':'序号'})
nondata=pd.DataFrame([x for x in midata_2.index if x not in midata_5['序号'].values]).rename(columns={0:'序号'})
nondata_all=pd.merge(nondata,all_[['序号','系数3','报价比率']],how='left',on='序号')
alldata_=midata_5.append(nondata_all).sort_values(by='序号',ascending=True).reset_index(drop=True).rename(columns={'报价比率':'报价比率_等差'})

allfordata_=pd.merge(all_,alldata_,how='left',on=['序号','系数3'])


allfordata_.to_excel(r'C:\Users\liuyunyun\Desktop\系数计算\系数补充_等差.xlsx',index=False)


shutil.rmtree(r'C:\Users\liuyunyun\Desktop\系数计算\单个系数')
os.mkdir(r'C:\Users\liuyunyun\Desktop\系数计算\单个系数')


























































