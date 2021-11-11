from dingtalkchatbot.chatbot import DingtalkChatbot, FeedLink
import time
import hmac
import urllib
import hashlib
import base64
import pandas as pd
import pymssql


if __name__ == "__main__":
    connect = pymssql.connect(host='172.16.1.25', user='b2bstat', password='Xsb2b2020', database='B2B',charset='utf8')  #建立连接
    if connect:
        print("连接成功!")
        
    cursor=connect.cursor()
    
#定义获取数据函数
def function() :
    try:
        sql='''
        SELECT DISTINCT * FROM B2B.dbo.LHH_FeedBackForFunction
        WHERE CAST(运行时间 AS DATE)=CAST(GETDATE() AS DATE)
        ORDER BY 运行结果 DESC
            '''
        result=pd.read_sql(sql,con=connect)

    except:
        b='所有程序当日运行失败'
    else :
        b='当日结果已展现'
    return result
if __name__ == '__main__':
    a=function()
    b = []
    if len(a) >= 13:        
        for i in range(len(a)) :
            if a['运行结果'][i].find('失败') != -1:
               b.appen(a['运行结果'][i])
        if len(b) == 0:
            b='所有程序运行成功,运行数量共计 13 个'
        else :
            b=b
    else :
        b=a
        b.rename(columns={'运行结果':'程序出现漏洞，运行数量不足'},inplace=True)
        
    
def getSIGN() :
    timestamp = str(round(time.time() * 1000)) #时间戳
    urlToken = 'https://oapi.dingtalk.com/robot/send?access_token=8a2bd460fd1d6c4dba90591f8e3e56b48d268a24bb6b77821b43fa167eea719b' #Webhook码
    secret = 'SEC36c4fbab17caa32f21a164f59db5b03d43e12d9b7ee74f61c6c060524c0c72b2' #密码形式有三种，一种为自定义关键词，一种为加签码，一种为IP地址，将对应密码放置于此即可
    
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp,secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    
    hmac_code = hmac.new(secret_enc, string_to_sign_enc,digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    
    SignMessage = urlToken + "&timestamp=" + timestamp + "&sign=" + sign
    return SignMessage 

SignMessage = getSIGN()
xiaoDing = DingtalkChatbot(SignMessage) #初始化机器人

def send_content():
    xiaoDing.send_text(str(b),is_at_all=False)

if __name__ == '__main__':
    getSIGN()
    send_content()