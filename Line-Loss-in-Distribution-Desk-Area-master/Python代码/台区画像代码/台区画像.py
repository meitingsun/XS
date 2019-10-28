# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 10:26:35 2019

@author: lixiaopeng
"""

##根据负载率进行台区画像
#导入所需模块
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import os,re
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (20.0, 12.0)
#读取文件夹中的csv数据文件
csvs = []
for file in os.listdir("./tqxs-data/4-28"):
    if file.endswith(".csv"):
        csvs.append(file)
fout=open("./tqxs-data/4-28/combined.csv","a")
# 第一个文件咱们保存头部column name信息:
for line in open("./tqxs-data/4-28/"+csvs[0],'r'):
    fout.write(line)
# 后面的部分可以跳过 headers:    
for file in csvs[1:]:
    f = open("./tqxs-data/4-28/"+file,'r')
    f.readline() # 跳过 header
    for line in f.readlines():
         fout.write(line)
    f.close() # 关闭文件
fout.close()
import os
datafile =r'./tqxs-data/4-28/combined.csv'
pwd = os.getcwd()
os.chdir(os.path.dirname(datafile))
df=pd.read_csv(os.path.basename(datafile),encoding='gbk')
os.chdir(pwd)
#查看数据
df.head()
df.describescribe()
#分析各个类型负载率的均值及个数
dfgt0 = df[(df['AVG_FZL']>0.05)&(df['AVG_FZL']<50)].copy()
dfgt0['fzl_class'],index =pd.qcut(dfgt0['AVG_FZL'],200,retbins=True)
df_fzl_count=dfgt0.XSL.groupby(dfgt0['fzl_class']).agg(['mean','count'])
df_fzl_count
idx_name =list(index[1:])
idx =range(len(idx_name))
fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(index[0:200],df_fzl_count['mean'],'o-',color='r')

#设置图例并且设置图例的字体及大小
font2 = {
'weight' : 'normal',
'size'   : 20,
}

ax.set_xlabel('平均负载率区间',font2)
ax.set_ylabel('线损率(%)',font2)
ax.set_title('线损率随平均负载率的变化趋势',font2,y=1.03)

#设置坐标刻度值的大小以及刻度值的字体
ax.set_ylim(3.0,6.0)
plt.xticks(rotation=45)
new_ticks = np.linspace(0,4,41,dtype='float')
ax.set_xticks(new_ticks)
ax.tick_params(labelsize=15)
#将文件保存至文件中并且画出图
plt.savefig(r'H:\MyProject\pic\线损率随平均负载率的变化趋势.png',dpi=300,bbox_inches = 'tight')
plt.show()


##根据负荷率进行台区画像
dffhgt0 = df[(df['FHL']>0.05)&(df['FHL']<50)].copy()
dffhgt0['fhl_class'],index2 =pd.qcut(dffhgt0['FHL'],200,retbins=True)
df_fhl_count=dffhgt0.XSL.groupby(dffhgt0['fhl_class']).agg(['mean','count'])
df_fhl_count
idx_name =list(index2[1:])
idx =range(len(idx_name))
fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(index2[0:200],df_fhl_count['mean'],'o-',color='g')
#设置图例并且设置图例的字体及大小
font2 = {
'weight' : 'normal',
'size'   : 20,
}

ax.set_xlabel('负荷率区间',font2)
ax.set_ylabel('线损率(%)',font2)
ax.set_title('线损率随负荷率的变化趋势',font2,y=1.03)

#设置坐标刻度值的大小以及刻度值的字体
ax.set_ylim(3.5,5.0)
plt.xticks(rotation=45)
new_ticks = np.linspace(0,0.8,41,dtype='float')
ax.set_xticks(new_ticks)
ax.tick_params(labelsize=15)
#将文件保存至文件中并且画出图
plt.savefig(r'H:\MyProject\pic\线损率随平均负荷率的变化趋势.png',dpi=300,bbox_inches = 'tight')
plt.show()

##根据峰谷差率进行台区画像
#查看数据
dffg0 = df[(df['FGCL']>0.1)&(df['FGCL']<10)].copy()
dffg0['fgl_class'],index3 =pd.qcut(dffg0['FGCL'],200,retbins=True)
df_fgl_count=dffg0.XSL.groupby(dffg0['fgl_class']).agg(['mean','count'])
df_fgl_count
idx_name =list(index3[1:])
idx =range(len(idx_name))
fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(index3[0:200],df_fgl_count['mean'],'o-',color='g')



# ax1 = ax.twinx() 
# ax1.plot(df['AVG_CHG_DATE_90'], df['TOTAL'], '^-',color='g')
#设置图例并且设置图例的字体及大小
font2 = {
'weight' : 'normal',
'size'   : 20,
}


# ax1.set_ylabel('台区个数',font2)
ax.set_xlabel('峰谷差率区间',font2)
ax.set_ylabel('线损率(%)',font2)
ax.set_title('线损率随峰谷差率的变化趋势',font2,y=1.03)

#设置坐标刻度值的大小以及刻度值的字体
# ax.set_xticks(rotation=90)
ax.set_ylim(3.5,5.0)
# ax1.set_ylim(0,100000)
# ax.set_xlim((0,200))
plt.xticks(rotation=45)
new_ticks = np.linspace(0,6.5,41,dtype='float')
ax.set_xticks(new_ticks)
ax.tick_params(labelsize=15)
#将文件保存至文件中并且画出图
plt.savefig(r'H:\MyProject\pic\线损率随峰谷差率的变化趋势.png',dpi=300,bbox_inches = 'tight')
plt.show()

##根据时间变量进行台区画像
#导入模块
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#设置参数 防止出现画图错误
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
#读入数据
df = pd.read_excel('./tqxs-data/date.xlsx')
#查看数据
df.head()
df.describe()
#画图
plt.rcParams['figure.figsize']=(16,10)
fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(df['AVG_CHG_DATE_90'],df['AVG_XSL'],'o-',color='b')
ax.fill_between(df['AVG_CHG_DATE_90'],df['AVG_XSL']-df['STD_XSL'],df['AVG_XSL']+df['STD_XSL'],alpha=0.1,color='b')


# ax1 = ax.twinx() 
# ax1.plot(df['AVG_CHG_DATE_90'], df['TOTAL'], '^-',color='g')
#设置图例并且设置图例的字体及大小
font2 = {
'weight' : 'normal',
'size'   : 20,
}


ax1.set_ylabel('台区个数',font2)
ax.set_xlabel('时间/季度',font2)
ax.set_ylabel('线损率',font2)
ax.set_title('线损率随时间的变化趋势',font2,y=1.03)

#设置坐标刻度值的大小以及刻度值的字体
ax.set_ylim(0.0,10.0)
# ax1.set_ylim(0,100000)
ax.set_xlim((0,200))
new_ticks = np.linspace(0,200,21,dtype='int')
ax.set_xticks(new_ticks)
ax.tick_params(labelsize=20)
# ax1.tick_params(labelsize=20)
#添加垂直于x轴的虚线
ax.axvline(88, linestyle='--', color='red') #
# ax_top = ax.twiny()
# xticks = ax.get_xticks()
# ax.set_xticks(xticks)
# xlim = ax.get_xlim()
# ax_top.set_xlim(xlim)
# top_tick = [88]
# top_label = ['88']
# ax_top.set_xticks(top_tick)
# ax_top.set_xticklabels(top_label)

#将文件保存至文件中并且画出图
plt.savefig('year_line_loss.png',dpi=600,bbox_inches = 'tight')
plt.show()

##根据温度变量进行台区画像
#导入模块
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import numpy as np
#更换参数 防止导入中文数据出错
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
#读入数据
df = pd.read_excel('./tqxs-data/201802_201903A/temperature.xlsx')
#查看数据
df
df.describe()
#画图
plt.rcParams['figure.figsize']=(20,12)
fig = plt.figure()
#设置输出的图片大小
ax = fig.add_subplot(111)

#在同一幅图片上画两条折线
ax.plot(df['CEIL(T.AVG_TEMP)'],df['AVG_XSL'],'o-',color='b')
# ax.fill_between(df['CEIL(T.AVG_TEMP)'],df['AVG_XSL']-df['STD_XSL'],df['AVG_XSL']+df['STD_XSL'],alpha=0.1,color='b')

#设置图例并且设置图例的字体及大小
font2 = {
'weight' : 'normal',
'size'   : 30,
}
plt.xlabel('温度区间',font2)
plt.ylabel('线损率',font2)
plt.title('2018/02-2019/03线损率随温度的变化趋势',font2,y=1.05)
#设置坐标刻度值的大小以及刻度值的字体
plt.ylim(3.25,4.70)
plt.xlim(-10,40)
new_ticks = np.linspace(-10,40,19,dtype='int')
plt.xticks(new_ticks)
plt.tick_params(labelsize=23)
ax.axvline(7, linestyle='--', color='red') #
ax.axvline(23, linestyle='--', color='red') #
plt.show()

##根据供电半径变量进行台区画像
#导入模块
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (16.0, 10.5)
#导入数据
df20 = pd.read_csv(r'./tqxs-data/4-29/radius.csv')
#查看数据
df20.head()
dflength=df20.sort_values('MAXLENGTH',ascending=True).dropna().copy()
dflength.head()
#进行数据处理及变换，方便进一步数据可视化

df_radius = dflength[['MAXLENGTH','XSL_AVG']].copy()

bins = [-0.1,100,200,300,400,500,600,700,800,900,1000,1500,2000]
df_radius['length_class'] =pd.cut(df_radius['MAXLENGTH'],bins,labels=['0~100','100~200','200~300','300~400','400~500',
                                        '500~600','600~700','700~800','800~900','900~1000','1000~1500','1500~2000'])
# df_radius['length_class'],index =pd.qcut(df_radius['MAXLENGTH'],10,retbins=True)
df_radius_agg=df_radius.XSL_AVG.groupby(df_radius['length_class']).agg(['mean','count'])
df_radius_agg['percentage']= df_radius_agg['count']/df_radius_agg['count'].sum()
# df2['percentage']=per.apply(lambda x:'%.2f%%' % (x * 100)) 
df_radius_agg
#画图
x_name_radius_class = list(['0~100','100~200','200~300','300~400','400~500',
                                        '500~600','600~700','700~800','800~900','900~1000','1000~1500','1500~2000'])
idx = np.arange(len(x_name_radius_class))
width=0.5
fig = plt.figure()
font2 = {
'weight' : 'normal',
'size'   : 20,
}
ax1 = fig.add_subplot(111)
ax1.bar(idx,df_radius_agg['mean'],color='#1E90FF',width=0.5)
ax1.plot(idx, df_radius_agg['mean'], 'r',marker='*',ms=10)
plt.xticks(idx+width/2, x_name_radius_class, rotation=40)
plt.ylim((0,5.5))
plt.title(u"亳州市台区供电半径与线损率分布",font2) # puts a title on our graph
plt.ylabel(u"线损率(%)",font2) 
plt.xlabel('供电半径区间',font2)
plt.tick_params(labelsize=16)

ax2 = ax1.twinx()
ax2.plot(idx, df_radius_agg['percentage']*100 , 'y',marker='*',ms=10,linewidth=3)
# ax2.set_xlim([0,40])
ax2.set_ylim([0,30])
ax2.set_ylabel('台区数占比（%）',fontsize='15')
ax2.set_xlabel('台区数占比（%）')
plt.tick_params(labelsize=16)

for x, y,z in zip(idx,df_radius_agg['percentage'],df_radius_agg['mean']):
    ax2.text(x, y*100-2,str(round(y*100,2))+'%' , ha='center', va='bottom', fontsize=20,rotation=0)
    ax1.text(x, z, str(round(z,2)), ha='center', va='bottom', fontsize=20,rotation=0)
    
plt.savefig(r'H:\MyProject\pic\亳州市台区供电半径的线损率分布.png',dpi=300,bbox_inches = 'tight')
plt.show()

##根据地区进行台区画像
#导入模块
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (16.0, 10.5) 
#导入数据
df=pd.read_excel('./tqxs-data/4-11/lineloss_classes.xlsx')
#查看数据
df.head()
df.info()
#进行数据处理及变换
classes = ['< 0','NaN','0~4','4~7','7~12','> 12']
per_less0=df.XSL_0.sum()/df.XSL_TOTAL.sum()
per_less0
per_0_4=df.XSL_0_4.sum()/df.XSL_TOTAL.sum()
per_0_4
per_4_7=df.XSL_4_7.sum()/df.XSL_TOTAL.sum()
per_4_7
per_7_12=df.XSL_7_12.sum()/df.XSL_TOTAL.sum()
per_7_12
per_more12=df.XSL_12.sum()/df.XSL_TOTAL.sum()
per_more12
per_null=df.XSL_NULL.sum()/df.XSL_TOTAL.sum()
per_null
df1 = df.groupby(df['CITY'])[['XSL_TOTAL','XSL_NULL','XSL_0','XSL_0_4','XSL_4_7','XSL_7_12','XSL_12']].sum()
#画图
fig = plt.figure()
# fig.set(alpha=0.2,) 
font2 = {
'weight' : 'normal',
'size'   : 15,
}
colors = ['yellow', 'orange', 'green','purple','pink','red']
explode = (0.15, 0.15, 0,0,0,0.15)
labels = ['NaN','< 0','0~4','4~7','7~12','> 12']

#所有地区
axall=plt.subplot2grid((2,4),(0,0),rowspan=2,colspan=2)            
patches, l_text, p_text =axall.pie(total_percent, explode=explode, colors=colors,
                                       labeldistance=1.1, autopct='%2.1f%%', shadow=False,
                                       startangle=90, pctdistance=0.6,textprops={'fontsize':20,'color':'black'})
axall.axis('equal')
axall.legend(loc='upper left', bbox_to_anchor=(-0.1, 1), labels=labels,fontsize=12) 
axall.set_title(u"线损率等级分布",font2,y=0.9) 
axall.tick_params(labelsize=16)

#合肥
axhf=plt.subplot2grid((2,4),(0,2))           
patches_hf, l_text_hf, p_text_hf =axhf.pie(df1.iloc[1,7:13], explode=explode, colors=colors,
                                        autopct='%2.1f%%', shadow=False,
                                            startangle=90, pctdistance=0.6,textprops={'fontsize':15,'color':'black'})                           
axhf.axis('equal')
# axhf.legend(loc='upper left', bbox_to_anchor=(-0.1, 1), labels=labels,fontsize=12) 
axhf.set_title(u"合肥地区线损率等级分布",font2) 
axhf.tick_params(labelsize=16)

#滁州
axcz=plt.subplot2grid((2,4),(0,3))           
patches_cz, l_text_cz, p_text_cz =axcz.pie(df1.iloc[2,7:13], explode=explode, colors=colors,
                                       autopct='%2.1f%%', shadow=False,                                          
                                           startangle=90, pctdistance=0.6,textprops={'fontsize':15,'color':'black'})   
axcz.axis('equal')
# axcz.legend(loc='upper left', bbox_to_anchor=(-0.1, 1), labels=labels,fontsize=12) 
axcz.set_title(u"滁州地区线损率等级分布",font2) 
axcz.tick_params(labelsize=16)

#黄山
axhs=plt.subplot2grid((2,4),(1,2))           
patches_hs, l_text_hs, p_text_hs =axhs.pie(df1.iloc[3,7:13], explode=explode, colors=colors,
                                        autopct='%2.1f%%', shadow=False,                                            
                                           startangle=90, pctdistance=0.6,textprops={'fontsize':15,'color':'black'})   
axhs.axis('equal')
# axhs.legend(loc='upper left', bbox_to_anchor=(-0.1, 1), labels=labels,fontsize=12) 
axhs.set_title(u"黄山地区线损率等级分布",font2) 
axhs.tick_params(labelsize=16)

#亳州
axbz=plt.subplot2grid((2,4),(1,3))           
patches_bz, l_text_bz, p_text_bz =axbz.pie(df1.iloc[0,7:13], explode=explode, colors=colors,
                                        autopct='%2.1f%%', shadow=False,
                                           startangle=90, pctdistance=0.6,textprops={'fontsize':15,'color':'black'})   
axbz.axis('equal')
# axbz.legend(loc='upper left', bbox_to_anchor=(-0.1, 1), labels=labels,fontsize=12) 
axbz.set_title(u"亳州地区线损率等级分布",font2) 
axbz.tick_params(labelsize=16)

plt.savefig("../线损率等级分布（含异常值）.png",dpi=600,bbox_inches='tight') 
plt.show()

##根据电量变量进行台区画像
#导入模块
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os,re
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (20.0, 12.0) 
#读入数据
csvs = []
for file in os.listdir("./tqxs-data/4-23/电量/台区-天/csv/"):
    if file.endswith(".csv"):
        csvs.append(file)
for file in csvs:
    filename = re.split('\.',file)
    df=pd.read_excel('./tqxs-data/4-23/电量/台区-天/'+file)
    df.to_csv('./tqxs-data/4-23/电量/台区-天/csv/'+filename[0]+'.csv')
fout=open("./tqxs-data/4-23/电量/台区-天/csv/combined.csv","a")
# 第一个文件咱们保存头部column name信息:
for line in open("./tqxs-data/4-23/电量/台区-天/csv/"+csvs[0],'r'):
    fout.write(line)
# 后面的部分可以跳过 headers:    
for file in csvs[1:]:
    f = open("./tqxs-data/4-23/电量/台区-天/csv/"+file,'r')
    f.readline() # 跳过 header
    for line in f.readlines():
         fout.write(line)
    f.close() # 关闭文件
fout.close()      
# 查看数据
dfall.head()
dfall.describe()
dfallgt0['CAL_SUM'].max()
#进行数据处理
dfallgt0 = dfall[(dfall['CAL_SUM']>20)&(dfall['XSL']>0)].copy()
dfallgt0['cal_class'],index =pd.qcut(dfallgt0['CAL_SUM'],300,retbins=True)
df_cal_count=dfallgt0.XSL.groupby(dfallgt0['cal_class']).agg(['mean','count'])
df_cal_count['percentage']=df_cal_count['count']/df_cal_count['count'].sum()
df_cal_count
#画图
idx_name =list(index[1:])
idx =range(len(idx_name))
fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(index[0:300],df_cal_count['mean'],'o-',color='b')
font2 = {
'weight' : 'normal',
'size'   : 20,
}

ax.set_xlabel('电量区间',font2)
ax.set_ylabel('线损率(%)',font2)
ax.set_title('线损率随电量的变化趋势',font2,y=1.03)

#设置坐标刻度值的大小以及刻度值的字体
ax.set_ylim(3.0,5.0)
plt.xticks(rotation=45)
new_ticks = np.linspace(0,4000,41,dtype='int')
ax.set_xticks(new_ticks)
ax.tick_params(labelsize=15)
#添加垂直于x轴的虚线
ax.axvline(1000, linestyle='--', color='red') #
#将文件保存至文件中并且画出图
plt.savefig(r'H:\MyProject\线损率随电量的变化趋势.png',dpi=300,bbox_inches = 'tight')
plt.show()

##根据日期变量进行台区画像
#导入模块
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#画图显示中文
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize'] = (20.0, 12.5) 
#导入数据
import os
datafile =r'../data/combined.csv'
pwd = os.getcwd()
os.chdir(os.path.dirname(datafile))
df=pd.read_csv(os.path.basename(datafile),encoding='gbk')
os.chdir(pwd)
#查看数据
df.head()
#数据预处理
df['DATA_DATE']=pd.to_datetime(df['DATA_DATE'])
df = df.sort_values('DATA_DATE')
df_date_count_hf=df[df['CITY']=='合肥市'].XSL.groupby(
    df['DATA_DATE'][df['CITY']=='合肥市']).agg(['mean','count'])
df_date_count_hf
df_date_count_cz=df[df['CITY']=='滁州市'].XSL.groupby(
    df['DATA_DATE'][df['CITY']=='滁州市']).agg(['mean','count'])
df_date_count_cz
df_date_count_hs=df[df['CITY']=='黄山市'].XSL.groupby(
    df['DATA_DATE'][df['CITY']=='黄山市']).agg(['mean','count'])
df_date_count_hs
df_date_count_bz=df[df['CITY']=='亳州市'].XSL.groupby(
    df['DATA_DATE'][df['CITY']=='亳州市']).agg(['mean','count'])
df_date_count_bz
#画图
import numpy as np
import matplotlib.dates as mdate
import matplotlib as mpl
plt.rcParams['figure.figsize'] = (16.0, 10.0) 
fig = plt.figure()
ax = fig.add_subplot(111)

labels=['合肥','滁州','黄山','亳州']
font2 = {
'weight' : 'normal',
'size'   : 20,
}

ax.plot(df_date_count_hf[df_date_count_hf['mean']>0].index,df_date_count_hf[df_date_count_hf['mean']>0]['mean'],'o-',color='b')
ax.plot(df_date_count_cz[df_date_count_cz['mean']>0].index,df_date_count_cz[df_date_count_cz['mean']>0]['mean'],'o-',color='y')
ax.plot(df_date_count_hs[df_date_count_hs['mean']>0].index,df_date_count_hs[df_date_count_hs['mean']>0]['mean'],'o-',color='g')
ax.plot(df_date_count_bz[df_date_count_bz['mean']>0].index,df_date_count_bz[df_date_count_bz['mean']>0]['mean'],'o-',color='r')
mondayFormatter = mpl.dates.DateFormatter('%Y/%m/%d')
monLoc = mpl.dates.MonthLocator()
ax.xaxis.set_major_locator(monLoc)
ax.xaxis.set_major_formatter(mondayFormatter)
plt.xlabel('日期',font2)
plt.ylabel('线损率',font2)
plt.title('2018/06~2019/03线损率随日期的变化趋势',font2,y=1.05)
plt.ylim((3,5.5))
ax.tick_params(labelsize=20)
plt.xticks(rotation=30)
ax.axvline('2019-03-01', linestyle='--', color='black') 
ax.axvline('2018-09-01', linestyle='--', color='black') 
ax.axvline('2018-12-01', linestyle='--', color='black') 
plt.legend(loc='upper left',  labels=labels,fontsize=20) 
plt.savefig("../pic/线损率随日期的变化.png",dpi=300,bbox_inches='tight') 
plt.show()



























