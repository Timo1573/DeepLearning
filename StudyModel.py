'''
开始构造KSU质检价算法模型
问题描述：
    根据现有时间衰减，SKU对应历史成交价,质检等级,报价人数,报价MAX,MIN值拟合初适合预测价格的模型

'''
import statsmodels as smd
import pylab as pl
import pandas as pd
import numpy as np
import pymssql
import datetime
from matplotlib import pyplot as plt
start_date=datetime.datetime.now()
import math
import sys

if __name__ == "__main__":
    connect = pymssql.connect(host='172.16.1.25', user='b2bstat', password='Xsb2b2020', database='B2B',charset='utf8')  #建立连接
    if connect:
        print("连接成功!")
        
    cursor=connect.cursor()

####//...数据准备，日期，机型，sku，质检报告，等级，出价数，最高最低报价，最大上牌次数，上拍次数均值，中标价
sql='''
WITH SKU中拍价格 AS (SELECT DISTINCT CAST(场次创建时间 AS DATE) 场次创建时间,机型名称,SKU编号,质检报告ID,质检等级,中拍金额,CASE WHEN 物品参考价<=1000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.1 THEN 1
																																WHEN 物品参考价>1000 AND 物品参考价<=3000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.07 THEN 1
																																WHEN 物品参考价>3000 AND 物品参考价<=5000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.05 THEN 1
																																WHEN 物品参考价>5000 AND 物品参考价<=10000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.03 THEN 1
																																WHEN 物品参考价>10000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.02 THEN 1
																																ELSE 0 END 是否保留
						FROM 
							GumaBusiness.dbo.PaixpBiddingGoods
WHERE CAST(场次创建时间 AS DATE)>=CAST(GETDATE()-60 AS DATE)
AND 场次类型='普通场'
AND 品类='手机'
AND 中拍状态='已中拍'
GROUP BY CAST(场次创建时间 AS DATE),机型名称,SKU编号,质检报告ID,质检等级,中拍金额,CASE WHEN 物品参考价<=1000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.1 THEN 1
																																WHEN 物品参考价>1000 AND 物品参考价<=3000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.07 THEN 1
																																WHEN 物品参考价>3000 AND 物品参考价<=5000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.05 THEN 1
																																WHEN 物品参考价>5000 AND 物品参考价<=10000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.03 THEN 1
																																WHEN 物品参考价>10000 AND ABS(中拍金额-物品参考价)*1.0/物品参考价<0.02 THEN 1
																																ELSE 0 END 
),
	报价人数 AS (SELECT 发布日期,机型,SKU编号,质检报告ID,质检等级,COUNT(DISTINCT 商户出价) 出价用户数
					FROM
						GumaBusiness.dbo.PaixpItemBuyerOfferDetailDaily
				 WHERE CAST(发布日期 AS DATE)>=CAST(GETDATE()-60 AS DATE)
				 AND 上拍类型='普通'
				 AND 品类='手机'
				 GROUP BY 发布日期,机型,SKU编号,质检报告ID,质检等级
),
	报价 AS (SELECT 发布日期,机型,SKU编号,质检报告ID,质检等级,MAX(商户出价) 最高报价,MIN(商户出价) 最低报价,AVG(商户出价) 平均报价
				FROM
					GumaBusiness.dbo.PaixpItemBuyerOfferDetailDaily
			 WHERE CAST(发布日期 AS DATE)>=CAST(GETDATE()-60 AS DATE)
			 AND 上拍类型='普通'
			 AND 品类='手机'
			 GROUP BY 发布日期,机型,SKU编号,质检报告ID,质检等级
),
	上拍次数 AS (SELECT 发布日期,机型,SKU编号,质检报告ID,质检等级,MAX(普通场上拍次数) 最大上拍次数,AVG(普通场上拍次数) 平均上拍次数
					FROM
						GumaBusiness.dbo.PaixpItemBuyerOfferDetailDaily
				 WHERE CAST(发布日期 AS DATE)>=CAST(GETDATE()-60 AS DATE)
				AND 上拍类型='普通'
				AND 品类='手机'
				GROUP BY 发布日期,机型,SKU编号,质检报告ID,质检等级
)
SELECT DISTINCT A.场次创建时间,A.机型名称,A.SKU编号,A.质检报告ID,A.质检等级,B.出价用户数,C.最低报价,C.最高报价,D.最大上拍次数,D.平均上拍次数,平均报价,A.中拍金额,平均基准价 FROM SKU中拍价格 A
LEFT JOIN 报价人数 B
ON A.场次创建时间=B.发布日期 AND A.机型名称=B.机型 AND A.SKU编号=B.SKU编号 AND A.质检报告ID=B.质检报告ID
LEFT JOIN 报价 C
ON A.场次创建时间=C.发布日期 AND A.机型名称=C.机型 AND A.SKU编号=C.SKU编号 AND A.质检报告ID=C.质检报告ID
LEFT JOIN 上拍次数 D
ON A.场次创建时间=D.发布日期 AND A.机型名称=D.机型 AND A.SKU编号=D.SKU编号 AND A.质检报告ID=D.质检报告ID
LEFT JOIN (SELECT 机型名称,SKUid,AVG(基准价格) 平均基准价
				FROM
				B2B.dbo.LHH_TableD
				GROUP BY 机型名称,SKUid) E
ON A.SKU编号=E.SKUid AND A.机型名称=E.机型名称
WHERE A.是否保留=1

'''

ModelData=pd.read_sql(sql,con=connect)
ModelData.dropna(axis=0,inplace=True)
end_date=datetime.datetime.now()
timedelta=end_date-start_date
print('数据读取完成，共计用时：',timedelta)

#####//...数据归一化

Transitiondata=ModelData[['SKU编号','质检等级','出价用户数','最低报价','最高报价','最大上拍次数','平均上拍次数','平均报价','中拍金额','平均基准价']]
Transitiondata['条件']=Transitiondata['SKU编号'].astype(str)+Transitiondata['质检等级']
Testdata=Transitiondata[['条件','出价用户数','最低报价','最高报价','最大上拍次数','平均上拍次数','平均报价','中拍金额','平均基准价']]

print(Testdata.head())

print(Testdata.describe().head())

###//...调参运用神经网络进行工作，神经网络分为三层，第一层为输入层，第二层为隐藏层，第三层为输出层,输出层到隐藏层需要先归类，归类之后计算预测知道输出层


#step1：对数据进行标准化处理

Testdata['标准化出价用户数']=(Testdata['出价用户数']-np.mean(Testdata['出价用户数']))/(max(Testdata['出价用户数'])-min(Testdata['出价用户数']))
Testdata['标准化平均报价']=(Testdata['平均报价']-np.mean(Testdata['平均报价']))/(max(Testdata['平均报价'])-min(Testdata['平均报价']))
Testdata['标准化平均基准价']=(Testdata['平均基准价']-np.mean(Testdata['平均基准价']))/(max(Testdata['平均基准价'])-min(Testdata['平均基准价']))
Testdata['标准化平均上拍次数']=(Testdata['平均上拍次数']-np.mean(Testdata['平均上拍次数']))/(max(Testdata['平均上拍次数'])-min(Testdata['平均上拍次数']))

data=Testdata[['条件','出价用户数','平均报价','平均基准价','平均上拍次数','中拍金额']]


#step2：定义第一层传递二层，第二层传递三层函数，标准化后的数据进行训练

#定义sigmoid函数进行归类计算
def sigmoid(x):
    #第一层到第二层的激活函数
    return 1/(1+np.exp(-x))

def deriv_sigmoid(x):
    #第一层到第二层的求导函数
    
    fx = sigmoid(x)
    return fx*(1-fx)

def mse_loss(y_true,y_pred):
    #使用方差作为损失函数
    return ((y_true-y_pred)**2).mean()

class OurNeuralNetwork() :
    
    def __init__(self):
        #第一层到第二层的函数
        self.w11 = np.random.normal()
        self.w12 = np.random.normal()
        self.w13 = np.random.normal()
        self.w14 = np.random.normal()
        self.w21 = np.random.normal()
        self.w22 = np.random.normal()
        self.w22 = np.random.normal()
        self.w23 = np.random.normal()
        self.w24 = np.random.normal()
        #第二层传到第三层的函数
        self.w1 = np.random.normal()
        self.w2 = np.random.normal()
        #截距项, Biases
        self.b1 = np.random.normal()
        self.b2 = np.random.normal()
        self.b3 = np.random.normal()
        
    def feedforward(self,x) :
        #前向传播学习
        h1=sigmoid(self.w11*x[0]+self.w12*x[1]+self.w13*x[2]+self.w14*x[3]+self.b1)
        h2=sigmoid(self.w21*x[0]+self.w22*x[1]+self.w23*x[2]+self.w24*x[3]+self.b1)
        o1=self.w1*h1+self.w2*h2+self.b3
        return o1
    
    #训练函数
    def train(self,data,all_y_trues) :
        learn_rate = 0.01 #学习率
        epochs = 1000 #训练次数
        
        #画图数据
        self.loss=np.zeros(100)
        self.sum=0
        
        #开始训练
        for epoch in range(epochs) :
            for x,y_true in zip(data,all_y_trues) :
                #计算h1
                h1=sigmoid(self.w11*x[0]+self.w12*x[1]+self.w13*x[2]+self.w14*x[3]+self.b1)
                #计算h2
                h2=sigmoid(self.w21*x[0]+self.w22*x[1]+self.w23*x[2]+self.w24*x[3]+self.b2)
                #计算输出节点
                y_pred=self.w1*h1+self.w2*h2+self.b3
                #反向传播计算导数
                d_L_d_ypred=-2*(y_true-y_pred)
                d_ypred_d_w1=h1
                d_ypred_d_w2=h2
                d_ypred_d_b3=0
                d_ypred_d_h1=self.w1
                d_ypred_d_h2=self.w2
                sum_1=self.w11*x[0]+self.w12*x[1]+self.w13*x[2]+self.w14*x[3]+self.b1
                d_h1_d_w11=x[0]*deriv_sigmoid(sum_1)
                d_h1_d_w12=x[1]*deriv_sigmoid(sum_1)
                d_h1_d_w13=x[2]*deriv_sigmoid(sum_1)
                d_h1_d_w14=x[3]*deriv_sigmoid(sum_1)
                d_h1_d_b1=deriv_sigmoid(sum_1)
                sum_2=self.w21*x[0]+self.w22*x[1]+self.w23*x[2]+self.w24*x[3]+self.b2
                d_h1_d_w21=x[0]*deriv_sigmoid(sum_2)
                d_h1_d_w22=x[1]*deriv_sigmoid(sum_2)
                d_h1_d_w23=x[2]*deriv_sigmoid(sum_2)
                d_h1_d_w24=x[3]*deriv_sigmoid(sum_2)
                d_h1_d_b2=deriv_sigmoid(sum_2)
                
                #梯度下降法
                self.w11-=learn_rate * d_L_d_ypred * d_ypred_d_h1 * d_h1_d_w11
                self.w12-=learn_rate * d_L_d_ypred * d_ypred_d_h1 * d_h1_d_w12
                self.w13-=learn_rate * d_L_d_ypred * d_ypred_d_h1 * d_h1_d_w13
                self.w14-=learn_rate * d_L_d_ypred * d_ypred_d_h1 * d_h1_d_w14
                self.b1-=learn_rate * d_L_d_ypred * d_ypred_d_h1 * d_h1_d_b1
                self.w21-=learn_rate * d_L_d_ypred * d_ypred_d_h2 * d_h1_d_w21  
                self.w22-=learn_rate * d_L_d_ypred * d_ypred_d_h2 * d_h1_d_w22
                self.w23-=learn_rate * d_L_d_ypred * d_ypred_d_h2 * d_h1_d_w23
                self.w24-=learn_rate * d_L_d_ypred * d_ypred_d_h2 * d_h1_d_w24
                self.b2-=learn_rate * d_L_d_ypred * d_ypred_d_h2 * d_h1_d_b2
                self.w1-=learn_rate * d_L_d_ypred * d_ypred_d_w1
                self.w2-=learn_rate * d_L_d_ypred * d_ypred_d_w2
                self.b3-=learn_rate * d_L_d_ypred * d_ypred_d_b3
                
            if epoch %10 ==0:
                y_preds = np.apply_along_axis(self.feedforward,1,data)
                loss=mse_loss(all_y_trues,y_preds)
                print("Epoch %d loss:%.3f" % (epoch,loss))
                self.loss[self.sum] = loss
                self.sum = self.sum+1
          
#进用科学计数法
pd.set_option('float_format',lambda x:'%.3f' % x)
np.set_printoptions(threshold=sys.maxsize)

#将DataFrame转化成array

DataArray = data.values

Y = DataArray[:,5]
X = DataArray[:,1:5]

X = np.array(X)#转化成array，自变量
Y = np.array(Y)#转化成array，因变量中拍金额

#处理数据
data = np.array(X)

data_mean = np.sum(data,axis=0)/np.size(data,0)
data = (data-data_mean) / np.max(data)

all_y_trues = np.array(Y)
all_y_trues_mean = np.sum(all_y_trues)/np.size(all_y_trues)
all_y_trues = (all_y_trues-all_y_trues_mean)/np.max(all_y_trues)

#训练数据
network = OurNeuralNetwork()

network.train(data,all_y_trues)

#输出神经网络参数
print("w11-->%.3f" % network.w11)
print("w12-->%.3f" % network.w12)
print("w13-->%.3f" % network.w13)
print("w14-->%.3f" % network.w14)
print("w21-->%.3f" % network.w21)
print("w22-->%.3f" % network.w22)
print("w23-->%.3f" % network.w23)
print("w24-->%.3f" % network.w24)
print("w1-->%.3f" % network.w1)
print("w2-->%.3f" % network.w2)
print("b1-->%.3f" % network.b1)
print("b2-->%.3f" % network.b2)
print("b3-->%.3f" % network.b3)

#标题显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

#测试数据
testData = np.array([0.1,0.9,0.3,0.85]) #出价用户数，平均基准价，上拍次数，平均报价
testPrice = network.feedforward(testData)

#损失函数曲线图
plt.plot(np.arange(100),network.loss)
plt.show()

#真实值与预测值的对比
y_preds = np.apply_along_axis(network.feedforward,1,data)
plt.plot(np.arange(85817),all_y_trues,"r^")
plt.plot(np.arange(85817),y_preds,"bs")
plt.title("红色为真实数据,蓝色为预测数据")
plt.show()


                
                





















































