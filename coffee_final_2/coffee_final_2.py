# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 15:57:01 2022

@author: user
"""
import os
outputPath = os.path.join("..", "output")

import datetime
import time
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel, cosine_similarity

# connect MySQL
import mysql.connector
import sqlalchemy
import MySQLdb


# 確定有連上資料庫
def checkDBServer(myHost="192.168.43.201", account="root", pw=""):
    try:
        # connect db server
        serverConn = mysql.connector.connect(host = myHost,
                                             user = account, password = pw)
        
        # list db，把現在有的資料庫顯示出來
        myCur = serverConn.cursor()
        myCur.execute("SHOW DATABASES") # DATABASE要記得加S!!!!!
        # for db in myCur: print(db)
        
        # end or return conn，用完要關
        serverConn.close()
    except Exception as mess:
        print("Connection DB Server Fail!!--->\n", mess, sep="")

# 讀訂單
def pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee",
                    tableName=""):
    try:
        connStr="mysql://" + account + ":@" + myHost + ":" + port + "/" + dbName + "?charset=utf8mb4&binary_prefix=true"
        dbConn = sqlalchemy.create_engine(connStr, isolation_level="READ UNCOMMITTED")
        df = pd.read_sql(tableName, dbConn)
        # print("\n\nPandas read from mysql ---->\n", df.columns, sep="")
        # print("Data is ---->\n", df.head())
    except Exception as mess:
        print("Read Data Table by Pandas Fail---->", mess)
    return df

def connectDB(myHost="192.168.43.201", account="root", pw="", dbName="coffee"):
    try:
        dbConn = mysql.connector.connect(host=myHost,
                                         user=account,
                                         password=pw,
                                         database=dbName)
        return dbConn
    except Exception as mess:
        print("Connection DB Fail!!! -->", mess)


def new_customer(memberlist, mix_orderList):
    ### 判斷兩個dataframe不同的值
    different = [x for x in memberlist['C_ID'].values if x not in mix_orderList['C_ID'].values]
        
    # 存新顧客的資料
    df_new_milk = pd.DataFrame(columns=['C_ID','favorite','sweet', 'preferredtime', 'coldhot'], index=range(0,12))
    df_new_sugar = pd.DataFrame(columns=['C_ID','favorite','sweet', 'preferredtime', 'coldhot'], index=range(0,12))
    df_new_both = pd.DataFrame(columns=['C_ID','favorite','sweet', 'preferredtime', 'coldhot'], index=range(0,12))
    df_new_nan = pd.DataFrame(columns=['C_ID','favorite','sweet', 'preferredtime', 'coldhot'], index=range(0,12))
    df_new_final = pd.DataFrame()
        
    # 設定0~11時段
    date = datetime.datetime.now()
    mm = date.month # 現在的月份
    
    ### 開始分析
    if(different != None):
        for i in different:
            for ii in memberlist.index:
                if (i == memberlist['C_ID'].values[ii]):
                    allergen_member = memberlist.values[ii]
                    # print(allergen_member[9])
                    
                    if (allergen_member[9] == "1"): # 牛奶過敏
                        pre_time = 0    
                        # print("牛奶過敏")
                        for ii in range(0,12):
                            df_new_milk['C_ID'].values[ii] = allergen_member[0]
                            df_new_milk['favorite'].values[ii] = "0"
                            df_new_milk['sweet'].values[ii] = "5"
                            df_new_milk['preferredtime'].values[ii] = pre_time
                            if ((mm == 4)|(mm == 5)|(mm == 6)|(mm == 7)|(mm == 8)|(mm == 9)):
                                df_new_milk['coldhot'].values[ii] = "2"
                            elif ((mm == 1)|(mm == 2)|(mm == 3)|(mm == 10)|(mm == 11)|(mm == 12)):
                                df_new_milk['coldhot'].values[ii] = "0"
                            pre_time = pre_time + 1
                        if (df_new_final.empty == True):
                            df_new_final = df_new_milk
                        else: 
                            df_new_final = df_new_final.append(df_new_milk, ignore_index=True)
                
                    elif (allergen_member[9] == "2"):
                        pre_time = 0
                        # print("糖過敏")
                        for sugar_ii in range(0,12):
                            df_new_sugar['C_ID'].values[sugar_ii] = allergen_member[0]
                            df_new_sugar['favorite'].values[sugar_ii] = "0"
                            df_new_sugar['sweet'].values[sugar_ii] = "0"
                            df_new_sugar['preferredtime'].values[sugar_ii] = pre_time
                            
                            if ((mm == 4)|(mm == 5)|(mm == 6)|(mm == 7)|(mm == 8)|(mm == 9)):
                                df_new_sugar['coldhot'].values[sugar_ii] = "2"
                            elif ((mm == 1)|(mm == 2)|(mm == 3)|(mm == 10)|(mm == 11)|(mm == 12)):
                                df_new_sugar['coldhot'].values[sugar_ii] = "0"
                            pre_time = pre_time + 1
                        if (df_new_final.empty == True):
                            df_new_final = df_new_sugar
                        else: df_new_final = df_new_final.append(df_new_sugar, ignore_index=True)
                    
                    elif ((allergen_member[9]=="12")): # 牛奶、糖過敏
                        pre_time = 0    
                            # print("牛奶&糖過敏")
                        for ii in range(0,12):
                            df_new_both['C_ID'].values[ii] = allergen_member[0]
                            df_new_both['favorite'].values[ii] = "0"
                            df_new_both['sweet'].values[ii] = "0"
                            df_new_both['preferredtime'].values[ii] = pre_time
                                
                            if ((mm == 4)|(mm == 5)|(mm == 6)|(mm == 7)|(mm == 8)|(mm == 9)):
                                df_new_both['coldhot'].values[ii] = "2"
                            elif ((mm == 1)|(mm == 2)|(mm == 3)|(mm == 10)|(mm == 11)|(mm == 12)):
                                df_new_both['coldhot'].values[ii] = "0"
                            pre_time = pre_time + 1
                        if (df_new_final.empty == True):
                            df_new_final = df_new_both
                        else: df_new_final = df_new_final.append(df_new_both, ignore_index=True)
                    else: # 沒有過敏原
                        pre_time = 0   
                        for ii in range(0,12):
                            df_new_nan['C_ID'].values[ii] = allergen_member[0]
                            df_new_nan['favorite'].values[ii] = "0"
                            df_new_nan['sweet'].values[ii] = "5"
                            df_new_nan['preferredtime'].values[ii] = pre_time
                                
                            if ((mm == 4)|(mm == 5)|(mm == 6)|(mm == 7)|(mm == 8)|(mm == 9)):
                                df_new_nan['coldhot'].values[ii] = "2"
                            elif ((mm == 1)|(mm == 2)|(mm == 3)|(mm == 10)|(mm == 11)|(mm == 12)):
                                df_new_nan['coldhot'].values[ii] = "0"
                            pre_time = pre_time + 1
                        if (df_new_final.empty == True):
                            df_new_final = df_new_nan
                        else: df_new_final = df_new_final.append(df_new_nan, ignore_index=True)
    df_new_final.columns = ['C_ID','favorite','sweet', 'preferred_time', 'coldhot']
    
    return df_new_final



def old_customer(memberlist, mix_orderlist):
    df = mix_orderlist
    
    # 轉成中文
    for i in df.index:
        try:
            if(df['sweet'].values[i]=="0"):
                df['sweet_ch'].values[i] = "無糖"
            elif(df['sweet'].values[i]=="1"):
                df['sweet_ch'].values[i] = "一分糖"
            elif(df['sweet'].values[i]=="2"):
                df['sweet_ch'].values[i] = "兩分糖"
            elif(df['sweet'].values[i]=="3"):
                df['sweet_ch'].values[i] = "三分糖"
            elif(df['sweet'].values[i]=="4"):
                df['sweet_ch'].values[i] = "四分糖"
            elif(df['sweet'].values[i]=="5"):
                df['sweet_ch'].values[i] = "五分糖"
            elif(df['sweet'].values[i]=="6"):
                df['sweet_ch'].values[i] = "六分糖"
            elif(df['sweet'].values[i]=="7"):
                df['sweet_ch'].values[i] = "七分糖"
            elif(df['sweet'].values[i]=="8"):
                df['sweet_ch'].values[i] = "八分糖"
            elif(df['sweet'].values[i]=="9"):
                df['sweet_ch'].values[i] = "九分糖"
            else:
                df['sweet_ch'].values[i] = "全糖"
        except Exception as e: print(e)
    
    for i in df.index:
        try:
            if(df['coldhot'].values[i]==0):
                df['temperature'].values[i] = "熱"
            elif(df['coldhot'].values[i]==1):
                df['temperature'].values[i] = "正常冰"
            elif(df['coldhot'].values[i]==2):
                df['temperature'].values[i] = "少冰"
            elif(df['coldhot'].values[i]==3):
                df['temperature'].values[i] = "微冰"
            else:
                df['temperature'].values[i] = "去冰"
        except Exception as e: print(e)
    
    
    for i in df.index:
        try:
            if (df['pcp_id'].values[i] == "0"): df['product_ch'].values[i] = "美式咖啡"
            elif (df['pcp_id'].values[i] == "1"): df['product_ch'].values[i] = "拿鐵"
        except Exception as e: print(e)
    
    
    df['month'] = df['date'].dt.month
    
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
    df['hour'] = df['time'].dt.hour
    
    
    
    # # preferred time
    for i in df.index:
        # 春天早上
        if(((df['month'].values[i]==1)|(df['month'].values[i]==2)|(df['month'].values[i]==3))&
            ((df['hour'].values[i]==6)|(df['hour'].values[i]==7)|(df['hour'].values[i]==8)|
              (df['hour'].values[i]==9)|(df['hour'].values[i]==10)|(df['hour'].values[i]==11))):
            df['preferred_time'].values[i] = 0
        
        # 春天中午
        elif(((df['month'].values[i]==1)|(df['month'].values[i]==2)|(df['month'].values[i]==3))&
            ((df['hour'].values[i]==12)or(df['hour'].values[i]==13)or(df['hour'].values[i]==14)or
            (df['hour'].values[i]==15)or(df['hour'].values[i]==16)or(df['hour'].values[i]==17))):
            df['preferred_time'].values[i] = 1
        # 春天晚上
        elif(((df['month'].values[i]==1)|(df['month'].values[i]==2)|(df['month'].values[i]==3))&
            ((df['hour'].values[i]==18)|(df['hour'].values[i]==19)|(df['hour'].values[i]==20)|
            (df['hour'].values[i]==21)|(df['hour'].values[i]==22)|(df['hour'].values[i]==23)|
            (df['hour'].values[i]==0)|(df['hour'].values[i]==1)|(df['hour'].values[i]==2)|
            (df['hour'].values[i]==3)|(df['hour'].values[i]==4)|(df['hour'].values[i]==5))):
            df['preferred_time'].values[i] = 2
        
        # 夏天早上
        elif(((df['month'].values[i]==4)|(df['month'].values[i]==5)|(df['month'].values[i]==6))&
            ((df['hour'].values[i]==6)|(df['hour'].values[i]==7)|(df['hour'].values[i]==8)|
              (df['hour'].values[i]==9)|(df['hour'].values[i]==10)|(df['hour'].values[i]==11))):
            df['preferred_time'].values[i] = 3
        # 夏天中午
        elif(((df['month'].values[i]==4)|(df['month'].values[i]==5)|(df['month'].values[i]==6))&
            ((df['hour'].values[i]==12)or(df['hour'].values[i]==13)or(df['hour'].values[i]==14)or
            (df['hour'].values[i]==15)or(df['hour'].values[i]==16)or(df['hour'].values[i]==17))):
            df['preferred_time'].values[i] = 4
        # 夏天晚上
        elif(((df['month'].values[i]==4)|(df['month'].values[i]==5)|(df['month'].values[i]==6))&
            ((df['hour'].values[i]==18)|(df['hour'].values[i]==19)|(df['hour'].values[i]==20)|
            (df['hour'].values[i]==21)|(df['hour'].values[i]==22)|(df['hour'].values[i]==23)|
            (df['hour'].values[i]==0)|(df['hour'].values[i]==1)|(df['hour'].values[i]==2)|
            (df['hour'].values[i]==3)|(df['hour'].values[i]==4)|(df['hour'].values[i]==5))):
            df['preferred_time'].values[i] = 5
        
        # 秋天早上
        elif(((df['month'].values[i]==7)|(df['month'].values[i]==8)|(df['month'].values[i]==9))&
            ((df['hour'].values[i]==6)|(df['hour'].values[i]==7)|(df['hour'].values[i]==8)|
              (df['hour'].values[i]==9)|(df['hour'].values[i]==10)|(df['hour'].values[i]==11))):
            df['preferred_time'].values[i] = 6
        # 秋天中午
        elif(((df['month'].values[i]==7)|(df['month'].values[i]==8)|(df['month'].values[i]==9))&
            ((df['hour'].values[i]==12)|(df['hour'].values[i]==13)|(df['hour'].values[i]==14)|
            (df['hour'].values[i]==15)|(df['hour'].values[i]==16)|(df['hour'].values[i]==17))):
            df['preferred_time'].values[i] = 7
        # 秋天晚上
        elif(((df['month'].values[i]==7)|(df['month'].values[i]==8)|(df['month'].values[i]==9))&
            ((df['hour'].values[i]==18)|(df['hour'].values[i]==19)|(df['hour'].values[i]==20)|
            (df['hour'].values[i]==21)|(df['hour'].values[i]==22)|(df['hour'].values[i]==23)|
            (df['hour'].values[i]==0)|(df['hour'].values[i]==1)|(df['hour'].values[i]==2)|
            (df['hour'].values[i]==3)|(df['hour'].values[i]==4)|(df['hour'].values[i]==5))):
            df['preferred_time'].values[i] = 8
        
        # 冬天早上
        elif(((df['month'].values[i]==10)|(df['month'].values[i]==11)|(df['month'].values[i]==12))&
            ((df['hour'].values[i]==6)|(df['hour'].values[i]==7)|(df['hour'].values[i]==8)|
              (df['hour'].values[i]==9)|(df['hour'].values[i]==10)|(df['hour'].values[i]==11))):
            df['preferred_time'].values[i] = 9
        # 冬天中午
        elif(((df['month'].values[i]==10)|(df['month'].values[i]==11)|(df['month'].values[i]==12))&
            ((df['hour'].values[i]==12)|(df['hour'].values[i]==13)|(df['hour'].values[i]==14)|
            (df['hour'].values[i]==15)|(df['hour'].values[i]==16)|(df['hour'].values[i]==17))):
            df['preferred_time'].values[i] = 10
        # 冬天晚上
        elif(((df['month'].values[i]==10)|(df['month'].values[i]==11)|(df['month'].values[i]==12))&
            ((df['hour'].values[i]==18)|(df['hour'].values[i]==19)|(df['hour'].values[i]==20)|
            (df['hour'].values[i]==21)|(df['hour'].values[i]==22)|(df['hour'].values[i]==23)|
            (df['hour'].values[i]==0)|(df['hour'].values[i]==1)|(df['hour'].values[i]==2)|
            (df['hour'].values[i]==3)|(df['hour'].values[i]==4)|(df['hour'].values[i]==5))):
            df['preferred_time'].values[i] = 11
    
    
    #### 開始分析
    filter1 = (df['C_ID']==1)
    filter2 = (df['C_ID']==2)
    filter3 = (df['C_ID']==3)
    
    ffilter1 = []
    ffilter2 = []
    ffilter3 = []
    
    ffilter1 = df.loc[filter1, ['product_ch', 'sweet_ch', 'temperature']]
    ffilter2 = df.loc[filter2, ['product_ch', 'sweet_ch', 'temperature']]
    ffilter3 = df.loc[filter3, ['product_ch', 'sweet_ch', 'temperature']]
    
    
    ffilter1_mix = ffilter1['product_ch'] + " " + ffilter1['sweet_ch'] + " " + ffilter1['temperature']
    ffilter2_mix = ffilter2['product_ch'] + " " + ffilter2['sweet_ch'] + " " + ffilter2['temperature']
    ffilter3_mix = ffilter3['product_ch'] + " " + ffilter3['sweet_ch'] + " " + ffilter3['temperature']
    
    
    ff1_mix = ffilter1_mix.str.cat()
    ff2_mix = ffilter2_mix.str.cat()
    ff3_mix = ffilter3_mix.str.cat()
    
    
    
    for i in df.index:
        if(df['C_ID'].values[i]==1):
            df['text'] = df['product_ch'] + " " + df['sweet_ch'] + " " + df['temperature']
        elif(df['C_ID'].values[i]==2):
            df['text'] = df['product_ch'] + " " + df['sweet_ch'] + " " + df['temperature']
        else:
            df['text'] = df['product_ch'] + " " + df['sweet_ch'] + " " + df['temperature']
    
    
    # TF-IDF
    count = TfidfVectorizer(analyzer='word',ngram_range=(1, 2),min_df=0, stop_words='english')
    # count_matrix = count.fit_transform([ff1_mix], [ff2_mix], [ff3_mix])
    count_matrix = count.fit_transform(df['text']) # 算出來是訂單跟訂單之間的相似度
    # print(count_matrix)
    cosine_sim = cosine_similarity(count_matrix, count_matrix)
    # print(cosine_sim)
    
    sim_scores = list(enumerate(cosine_sim))
    sim_scores = sorted(sim_scores)
    sim_scores = sim_scores[1:]
    product_recommend = [i[0] for i in sim_scores]
    products = df.iloc[product_recommend][['C_ID','product_ch', 'sweet_ch', 'preferred_time','temperature']]
    
    
    # 要傳出去的frame
    groupCoffee = products.groupby([products['preferred_time'], products['C_ID']]) # groupby
    
    
    # 挑出每個群組中最大的數
    df_final = pd.DataFrame() # 存要傳出去的結果
    
    df_final = df_final.append(groupCoffee.first())
    df_final = df_final.reset_index()
    
    ### 轉換代碼
    for i in df_final.index:
        try:
            if (df_final['product_ch'].values[i] == "拿鐵"):
                df_final['product_ch'].values[i] = 1
            elif (df_final['product_ch'].values[i] == "美式咖啡"):
                df_final['product_ch'].values[i] = 0
            else: print("try again")
        except Exception as e: print(e)
    
    
    for i in df_final.index:
        try:
            if(df_final['sweet_ch'].values[i]=="全糖"):
                df_final['sweet_ch'].values[i] = "A"
            elif(df_final['sweet_ch'].values[i]=="九分糖"):
                df_final['sweet_ch'].values[i] = "9"
            elif(df_final['sweet_ch'].values[i]=="八分糖"):
                df_final['sweet_ch'].values[i] = "8"
            elif(df_final['sweet_ch'].values[i]=="七分糖"):
                df_final['sweet_ch'].values[i] = "7"
            elif(df_final['sweet_ch'].values[i]=="六分糖"):
                df_final['sweet_ch'].values[i] = "6"
            elif(df_final['sweet_ch'].values[i]=="五分糖"):
                df_final['sweet_ch'].values[i] = "5"
            elif(df_final['sweet_ch'].values[i]=="四分糖"):
                df_final['sweet_ch'].values[i] = "4"
            elif(df_final['sweet_ch'].values[i]=="三分糖"):
                df_final['sweet_ch'].values[i] = "3"
            elif(df_final['sweet_ch'].values[i]=="兩分糖"):
                df_final['sweet_ch'].values[i] = "2"
            elif(df_final['sweet_ch'].values[i]=="一分糖"):
                df_final['sweet_ch'].values[i] = "1"
            elif(df_final['sweet_ch'].values[i]=="無糖"):
                df_final['sweet_ch'].values[i] = "0"
        except Exception as e: print(e)
    
    
    for i in df_final.index:
        try:
            if(df_final['temperature'].values[i]=="熱"):
                df_final['temperature'].values[i] = 0
            elif(df_final['temperature'].values[i]=="正常冰"):
                df_final['temperature'].values[i] = 1
            elif(df_final['temperature'].values[i]=="少冰"):
                df_final['temperature'].values[i] = 2
            elif(df_final['temperature'].values[i]=="微冰"):
                df_final['temperature'].values[i] = 3
            elif(df_final['temperature'].values[i]=="去冰"):
                df_final['temperature'].values[i] = 4
        except Exception as e: print(e)
    
    
    df_final = df_final[['C_ID','product_ch','sweet_ch','preferred_time','temperature']]
    df_final.columns = ['C_ID','favorite','sweet', 'preferred_time', 'coldhot'] # 改欄位名稱
    return df_final



def main(memberlist, orderlist, productdetail):
    checkDBServer(myHost="192.168.43.201", account="root", pw="") # 檢查是否有連上資料庫
    
    # 先把訂單跟詳細資料做合併
    mix_orderList = pd.merge(orderlist, productdetail, on='O_ID')
    mix_orderList['sweet_ch']=" " # 加欄位存資料
    mix_orderList['preferred_time']=" "
    mix_orderList['temperature']=" " # 加欄位存資料
    mix_orderList['product_ch']=" "
    
    
    old = old_customer(memberlist, mix_orderList)
    new = new_customer(memberlist, mix_orderList)
    
    ### 把新顧客跟舊顧客推薦的東西合在一起
    mix = pd.concat([old, new])
    # print(mix)
    mix = mix.sort_values(by=['C_ID','preferred_time'])
    mix = mix.reset_index()
    mix = mix.drop(['index'], axis=1)
    
    
    
    # 取資料庫中的值
    # mix_before = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="favorite") # 讀資料庫原本的東西
    # member_before = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="register") # 讀資料庫原本的東西
    
    ### 把結果寫資料庫
    def updateData(dbConn="", tableName="favorite", sqlStr=""):
        try:
            if dbConn != "":
                myCur = dbConn.cursor() # 資料庫連接
                if sqlStr == "":
                    # if (len(mix) == len(mix_before)): # 如果長度一樣
                    if(len(mix)>len(mix_before)): # 如果長度不一樣
                        # print("mix ",len(mix))
                        # print("mix before  ",len(mix_before))
                        minus = len(mix)-len(mix_before)
                        mix_tail = mix.tail(minus)
                        mix_tail = mix_tail.reset_index()
                        mix_tail = mix_tail.drop(['index'], axis=1)
                        
                        for i in mix_tail.index:
                            sqlStr = "INSERT INTO " + tableName + "(C_ID, favorite, sweet, preferred_time, coldhot) " + "VALUES('" + str(mix_tail['C_ID'].values[i]) + "', " + str(mix_tail['favorite'].values[i]) + ", " + str(mix_tail['sweet'].values[i]) + ", " + str(mix_tail['preferred_time'].values[i]) + ", " + str(mix_tail['coldhot'].values[i]) + ");"
                            # print(sqlStr)
                            myCur.execute(sqlStr) # 執行SQL語法
                            dbConn.commit()
                    
                    # mix_beforee = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="favorite") # 讀資料庫原本的東西
                    # print("mix beforee ", len(mix_beforee))
                    for i in mix.index:
                        if (((mix['C_ID'].values[i])==(mix_beforee['C_ID'].values[i]))and
                            ((mix['favorite'].values[i])!=(mix_beforee['favorite'].values[i]))): # 品項
                            # sqlStr = "UPDATE " + tableName + " SET favorite=" + str(mix['favorite'].values[i]) + " WHERE C_ID = 'C000000007' AND preferred_time=" + str(mix['preferred_time'].values[i]) + ";"
                            sqlStr = "UPDATE " + tableName + " SET favorite=" + str(mix['favorite'].values[i]) + " WHERE C_ID = '" + str(mix['C_ID'].values[i]) + "' AND preferred_time=" + str(mix['preferred_time'].values[i]) + ";"
                            myCur.execute(sqlStr) # 執行SQL語法
                            dbConn.commit()
                                
                        if (((mix['C_ID'].values[i])==(mix_beforee['C_ID'].values[i]))and
                            ((mix['sweet'].values[i]) != (mix_beforee['sweet'].values[i]))): # 甜度
                            # sqlStr = "UPDATE " + tableName + " SET `favorite`.`sweet` = " + "'" + str(mix['sweet'].values[i]) + "' WHERE `favorite`.`C_ID` = " + "'" + str(mix['C_ID'].values[i]) + "'" + " AND " + "`favorite`.`preferredtime` = " + "'" + str(mix['preferredtime'].values[i]) + "';"
                            sqlStr = "UPDATE " + tableName + " SET sweet='" + str(mix['sweet'].values[i]) + "' WHERE C_ID = '" + str(mix['C_ID'].values[i]) + "' AND preferred_time=" + str(mix['preferred_time'].values[i]) + ";"
                            # print(sqlStr)
                            myCur.execute(sqlStr) # 執行SQL語法
                            dbConn.commit()
                            
                        if (((mix['C_ID'].values[i]) == (mix_beforee['C_ID'].values[i]))and
                            ((mix['coldhot'].values[i])!=(mix_beforee['coldhot'].values[i]))): # 溫度
                            sqlStr = "UPDATE " + tableName + " SET coldhot=" + str(mix['coldhot'].values[i]) + " WHERE C_ID = '" + str(mix['C_ID'].values[i]) + "' AND preferred_time=" + str(mix['preferred_time'].values[i]) + ";"
                            myCur.execute(sqlStr) # 執行SQL語法
                            dbConn.commit()
            else:
                print("DB connection error, processing abort!!")
        # except Exception as mess: print("Update Data Fail ---->", mess)
        except Exception as mess: print("len ", len(mix))
    
    
    def pandasWriteData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee",
                    tableName="favorite"):
        try:
            connStr = "mysql://" + account + ":@" + myHost + ":" + port + "/" + dbName + "?charset=utf8mb4&binary_prefix=true"
            dbConn = sqlalchemy.create_engine(connStr, isolation_level="READ UNCOMMITTED")
            # write data to sql
            mix.to_sql(tableName, dbConn, if_exists="append", index=False)
        except Exception as mess:
            print("Write Data Table by Pandas Fail --->", mess)
    
    
    if(favorite.empty): 
        pandasWriteData(myHost="192.168.43.201", port = "3306", account="root", pw="", tableName="favorite")
        print("寫入完成")
    else: 
        updateData(dbConn, tableName="favorite", sqlStr="")
        print("已更新完畢")
    

if(__name__=="__main__"):
    dbConn = connectDB(myHost="192.168.43.201", account="root", pw="", dbName="coffee")
    while True:
        print("開始")
        memberlist = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="register") # 讀會員資料
        orderlist = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="orderrecorred") # 讀訂單資料
        productdetail = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="product_detail")
        favorite = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="favorite")
        # update favorite
        mix_before = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="favorite") # 讀資料庫原本的東西
        mix_beforee = pandasReadData(myHost="192.168.43.201", port = "3306", account="root", pw="", dbName="coffee", tableName="favorite") # 讀資料庫原本的東西
        
        main(memberlist, orderlist, productdetail)
        time.sleep(10)