# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 09:36:56 2019

@author: lixiaopeng
"""

import pandas as pd #导入所需模块
hf = pd.read_csv('../data2/hf.csv') #读取数据
len(hf) #查看数据长度
hf.columns #查看数据各变量名称
features=['XSL', 'CAL_SUM', 'FHL', 'AVG_FZL', 'AVG_TEMP', 'CHG_DATE', 'TG_CAP',
       'CD_TOTAL',  'PER_CAP','season'] #筛选出本次分析所需字段
X=hf[features] #将筛选出的字段从数据中提出 并赋值给X
##数据预处理
#导入需要的模块
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_scaler = scaler.fit_transform(X)
#数据标准化
X_scaler[:5,:]
##K-means聚类
from sklearn.cluster import KMeans,MiniBatchKMeans
%matplotlib inline
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=10)
#使用肘部法则 判断聚类的个数
import numpy as np
#coding:utf-8
#我们计算K值从2到10对应的平均畸变程度：
from sklearn.cluster import KMeans
#用scipy求解距离
from scipy.spatial.distance import cdist
K=range(2,15)
meandistortions=[]
for k in K:
    kmeans=KMeans(n_clusters=k, init='k-means++',n_jobs=-1)
    kmeans.fit(X_scaler)
    meandistortions.append(sum(np.min(
            cdist(X_scaler,kmeans.cluster_centers_,
                 'euclidean'),axis=1))/X_scaler.shape[0])
plt.plot(K,meandistortions,'bx-')
plt.xlabel('k')
plt.ylabel(u'平均畸变程度',fontproperties=font)
# plt.title(u'用肘部法则来确定最佳的K值',fontproperties=font)
# 使用k-means聚类方法将数据聚成三类
from sklearn.cluster import KMeans
kM=KMeans(n_clusters=3, init='k-means++',n_jobs=-1).fit(X_scaler)
#展示第一类台区各指标变量情况
hf[hf['kM']==0].describe()[features].T
#展示第二类台区各指标变量情况
hf[hf['kM']==1].describe()[features].T
#展示第三类台区各指标变量情况
hf[hf['kM']==2].describe()[features].T
## DBSCAN聚类
#导入模块
from sklearn.cluster import DBSCAN
# db = DBSCAN(eps=0.8, min_samples=10).fit(X_scaler)
# hf['cluster_db']=db.labels_
#进行聚类
from sklearn import metrics
from sklearn.cluster import MiniBatchKMeans
for eps in [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]:
    for sample in [5,10,15,20,25,30]:
        y_pred = DBSCAN(eps=eps, min_samples=sample,n_jobs=-1).fit(X_scaler).labels_
        score= metrics.calinski_harabaz_score(X_scaler, y_pred)  
        print("%s:%s:%s"%(eps,sample,score))
centroids = db.cluster_centers_ #获取聚类中心
hf.groupby('cluster_db').mean()
import pandas as pd
df=pd.read_csv('../data2/processed_alldata.csv')
df.head()
df.city_type.value_counts()
dfbz=df[(df['city_type']==3)].dropna()
len(dfbz)
df.columns
dfbz.to_csv('../data2/bz.csv',index=False)
hf=df[(df['city_type']==0)].dropna()
hf.to_csv('../data2/hf.csv',index=False)
cz=df[(df['city_type']==1)].dropna()
cz.to_csv('../data2/cz.csv',index=False)
hs=df[(df['city_type']==2)].dropna()
hs.to_csv('../data2/hs.csv',index=False)
import pandas as pd
df=pd.read_csv('../data/processed_alldata.csv')
df.head()
df.city_type.value_counts()
dfbz_13=df[(df['city_type']==3)&((df['season']==1)|(df['season']==3))].dropna()
len(dfbz_13)
df.columns
dfbz_13.to_csv('../data/bz_13.csv',index=False)
dfbz_24=df[(df['city_type']==3)&((df['season']==2)|(df['season']==4))].dropna()
dfbz_24.to_csv('../data/bz_24.csv',index=False)
hf_24=df[(df['city_type']==0)&((df['season']==2)|(df['season']==4))].dropna()
hf_24.to_csv('../data/hf_24.csv',index=False)
hf_13=df[(df['city_type']==0)&((df['season']==1)|(df['season']==3))].dropna()
hf_13.to_csv('../data/hf_13.csv',index=False)
cz_13=df[(df['city_type']==1)&((df['season']==1)|(df['season']==3))].dropna()
cz_13.to_csv('../data/cz_13.csv',index=False)
cz_24=df[(df['city_type']==1)&((df['season']==2)|(df['season']==4))].dropna()
cz_24.to_csv('../data/cz_24.csv',index=False)
hs_24=df[(df['city_type']==2)&((df['season']==2)|(df['season']==4))].dropna()
hs_24.to_csv('../data/hs_24.csv',index=False)
hs_13=df[(df['city_type']==2)&((df['season']==1)|(df['season']==3))].dropna()
hs_13.to_csv('../data/hs_13.csv',index=False)
import pandas as pd
#读取数据
df=pd.read_csv('../dataset2/mydata1.csv',encoding='gbk',index_col='index',low_memory=False)
df['TG_CITY_TYPE'].head()
df.columns
df.info()
df['tg_DATATYPE'].value_counts()
df1=df[['TG_NO','CHG_DATE','TG_CAP','tg_cltype','tg_DATATYPE','machine_tag',
        'swgf_type','Vtype','CD_TOTAL','area_type','city_type','PER_CAP','tg_usertype','CONS_NUM']]
df2=df1.copy()
df2.info()
df2.describe()
##导线传输电能方式
df2['tg_DATATYPE'].value_counts()
##台区类型
df2['machine_tag'].value_counts()
df2.machine_tag.replace(2,0,inplace=True)
##户均容量缺失值和异常值处理
##把户均容量小于0.5和大于等于400定义为异常值，利用均值代替异常值和缺失值
#读取数据
df2=pd.read_csv('../data2/proc',encoding='gbk',index_col='index',low_memory=False)
#per_cap缺失值用户均容量均值替代
bins = [-0.1,0.5,1,2,3,4,5,6,7,8,9,10,50,300,999,2000]
cats = pd.cut(df2.PER_CAP,bins)
pd.value_counts(cats)
# df2.PER_CAP.fillna(df1.PER_CAP.mean(),inplace=True)
import matplotlib.pyplot as plt
plt.plot(df2.PER_CAP,'*')
plt.show()
from numpy import nan as NA
df2.PER_CAP[(df2['PER_CAP']<=0.5)|(df2['PER_CAP']>=300)]=NA
#根据城市类型和地区类型的均值进行对应填充
for i in range(0,5):
    for j in range(0,5):
        df2.loc[(df2['area_type']==i)&(df2['city_type']==j)&(df2['PER_CAP'].isnull()),'PER_CAP']=\
        df2[(df2['area_type']==i)&(df2['city_type']==j)].PER_CAP.mean()
for i in range(0,5):
    for j in range(0,5):
        df2.loc[(df2['area_type']==i)&(df2['city_type']==j)&(df2['CONS_NUM'].isnull()),'CONS_NUM']=\
        df2[(df2['area_type']==i)&(df2['city_type']==j)].CONS_NUM.mean()
# area_type为4的都是空值，所以填充后还是空值，因此用城市类型对应填充
for j in range(0,5):
     df2.loc[(df2['city_type']==j)&(df2['PER_CAP'].isnull()),'PER_CAP']=\
        df2[(df2['city_type']==j)].PER_CAP.mean()
for j in range(0,5):
     df2.loc[(df2['city_type']==j)&(df2['CONS_NUM'].isnull()),'CONS_NUM']=\
        df2[(df2['city_type']==j)].CONS_NUM.mean()
df2.info()
##导线长度缺失值和异常值处理
#异常值是导线长度小于等于20，大于10000的，利用随机森林对缺失值做拟合
bins = [-0.1,0.1,20,50,100,500,1000,1500,2000,10000,20000,50000,100000,150000000]
cats = pd.cut(df2.CD_TOTAL,bins)
pd.value_counts(cats)
df2.CD_TOTAL[(df2['CD_TOTAL']<=20)|(df2['CD_TOTAL']>=10000)]=NA
df2.info()
known_cd_date = df2[df2.CD_TOTAL.notnull()]
known_cd_date.iloc[:,[3,4,5,6,7,9,10,11,12]].info()
df2[df2.CD_TOTAL.isnull()].iloc[:,[3,4,5,6,7,10,11,12]].info()
### 使用 RandomForestClassifier 填补缺失的年龄属性
from sklearn.ensemble import RandomForestRegressor
def set_missing_ages(df):
    # 把已有的数值型特征取出来丢进Random Forest Regressor中
    # 台区分成已知导线长度和未知导线长度两部分
    known_cd = df2[df2.CD_TOTAL.notnull()]
    unknown_cd = df2[df2.CD_TOTAL.isnull()]
    # y导线长度
    y = known_cd.iloc[:, 8].as_matrix()
    # X即特征属性值
    X = known_cd.iloc[:,[3,4,5,6,7,9,10,11,12]].as_matrix()
    # fit到RandomForestRegressor之中
    rfr = RandomForestRegressor(random_state=0, n_estimators=200, n_jobs=-1)
    rfr.fit(X, y)
    # 用得到的模型进行未知导线长度结果预测
    predictedDate = rfr.predict(unknown_cd.iloc[:,[3,4,5,6,7,9,10,11,12]].as_matrix())
    # 用得到的预测结果填补原缺失数据
    df2.loc[ (df2.CD_TOTAL.isnull()), 'CD_TOTAL' ] = predictedDate
    return df2, rfr

df2, rfr = set_missing_ages(df2)
df2.info()
##台区电压异常值处理
df2.loc[df2['TG_NO']=='mc19246','TG_CAP'] = 100
df2.loc[df2['TG_NO']=='mc14467','TG_CAP'] = 30
df2.loc[df2['TG_NO']=='mc14436','TG_CAP'] = 30
df2.loc[df2['TG_NO']=='mc06452','TG_CAP'] = 100
df2.loc[df2['TG_NO']=='mc06348','TG_CAP'] = 100
df2.loc[df2['TG_NO']=='mc02352','TG_CAP'] = 100
df2.loc[df2['TG_NO']=='mc02231','TG_CAP'] = 200
##台区使用年限缺失值利用KNN算法补全
bins = [-200,0.1,20,50,100,500,1000,1500,2000,5000,10000,20000]
cats = pd.cut(df2.CHG_DATE,bins)
pd.value_counts(cats)
df2.loc[(df2.CHG_DATE<=0), 'CD_TOTAL' ] = df2.CHG_DATE.mean()
known_date = df2[df2.CHG_DATE.notnull()]
known_date.iloc[:,2:12]
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor 
def knn_missing_filled(x_train, y_train, test, k = 3, dispersed = True):    
    if dispersed:        
        clf = KNeighborsClassifier(n_neighbors = k, weights = "distance")    
    else:        
        clf = KNeighborsRegressor(n_neighbors = k, weights = "distance")        
        clf.fit(x_train, y_train) 
    return  clf.predict(test)


known_date = df2[df2.CHG_DATE.notnull()]
unknown_date = df2[df2.CHG_DATE.isnull()]
y_train = known_date.iloc[:, 1].as_matrix()
# X即特征属性值
X_train = known_date.iloc[:,2:12].as_matrix()
test=unknown_date.iloc[:,2:12].as_matrix()
pre_test = knn_missing_filled(X_train,y_train,test,k=3,dispersed = False)
df2.loc[ (df2.CHG_DATE.isnull()), 'CHG_DATE' ] = pre_test
df2.info()
df2.to_csv('../dataset2/isnull70184.csv')
import pandas as pd
import os
datafile =r'H:/MyProject/data/raw_data/combined.csv'
pwd = os.getcwd()
os.chdir(os.path.dirname(datafile))
df=pd.read_csv(os.path.basename(datafile),encoding='gbk')
os.chdir(pwd)
df.info()
df.head()
df2 =df[['TG_NO','REPORT_DATE','XSL','CAL_SUM','FHL','AVG_FZL','AVG_TEMP']].copy()
df2 = pd.read_csv('../data2/processed_alldata.csv')
df2.info()
df2['REPORT_DATE']=pd.to_datetime(df2['REPORT_DATE'])
def season(x):
    if x in [3,4,5]:
        return 1
    elif x in [6,7,8]:
        return 2
    elif x in [9,10,11]:
        return 3
    else:
        return 4
df2['dow'] = df2.REPORT_DATE.apply(lambda x: x.dayofweek)
df2['doy'] = df2.REPORT_DATE.apply(lambda x: x.dayofyear)
df2['day'] = df2.REPORT_DATE.apply(lambda x: x.day)
df2['month'] = df2.REPORT_DATE.apply(lambda x: x.month)
df2['season'] =df2.month.apply(lambda x:season(x))
df2.head()
df2.sort_values(['TG_NO','REPORT_DATE'],inplace=True)
df2.head(20)
##处理电量异常值
bins = [-3000000,30,100,200,300,500,600,700,800,900,1000,2000,10000,20000,30000,50000,100000,1000000,2000000]
cats = pd.cut(df2.CAL_SUM,bins)
pd.value_counts(cats)
from numpy import nan as NA
df2.CAL_SUM[(df2['CAL_SUM']<=30)|(df2['CAL_SUM']>=10000)]=NA
df2.CAL_SUM.fillna(method='ffill',inplace=True)
##负荷率异常值处理
#FHL小于0和大于0.8确定为异常值
bins = [-200000,0,0.1,0.5,0.6,0.7,0.8,0.9,0.99,1]
cats = pd.cut(df2.FHL,bins)
pd.value_counts(cats)
df2.FHL[(df2['FHL']>=0.8)|(df2['FHL']<=0)]=NA
df2.FHL.fillna(method='ffill',inplace=True)
##平均负载率异常值处理
bins = [-0.1,0,0.1,0.2,0.5,0.6,0.7,0.8,0.9,0.99,1,2,3,10,100]
cats = pd.cut(df2.AVG_FZL,bins)
pd.value_counts(cats)
df2.AVG_FZL[(df2['AVG_FZL']>=10)|(df2['AVG_FZL']<=0)]=NA
df2.AVG_FZL.fillna(method='ffill',inplace=True)
##线损率异常值处理
bins = [-0.1,0,0.1,0.2,0.5,1,2,3,4,5,6,7,8,9,12]
cats = pd.cut(df2.XSL,bins)
pd.value_counts(cats)
df3 = df2[df2['XSL']>=0.1].copy()
df3
df3.to_csv('../dataset2/time_series_index_notoutiler.csv')
df4=pd.read_csv('../dataset2/notnull70184.csv',index_col='index')
df3['TG_NO'] =df3['TG_NO'].astype('str')
df4['TG_NO'] = df4['TG_NO'].astype('str')
df3.iloc[:,1:].head()
df4.iloc[:,0:].head()
df_cb = pd.merge(df3.iloc[:,1:], df4.iloc[:,0:], how='inner', on=['TG_NO', 'TG_NO'])
len(df_cb)
df_cb[df_cb['CHG_DATE']<0].index
df_cb.drop(df_cb[df_cb['CHG_DATE']<0].index,inplace=True)
df_cb.describe()['CHG_DATE']
df_cb.to_csv('../dataset2/processed_alldata.csv',index=False)
df_cb.info()
df_cb.describe()
df_cb[df_cb['TG_NO']=='2049854482']
















































