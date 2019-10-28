#导入所用包
library(forecast)
library(XLConnect)
sz <- read.csv("熵值法所用数据.csv",header = T) #数据读入
head(sz)
str(sz)
colnames(sz)
sz$TG_NO <- NULL #台区名称索引列删除
#sz$XSL_AVG <- NULL #台区平均线损率列删除
head(sz)
library(VIM) #加载VIM包进行缺失值可视化
aggr(sz,prop = FALSE, numbers = TRUE)
library(mice);library(DMwR) #加载DMwR包 进行缺失值插补
mydata <- knnImputation(sz,k=10,meth = 'weighAvg')
#归一化处理
min.max.norm <- function(x){
  (x-min(x))/(max(x)-min(x))
}
max.min.nrom <- function(x){
  (max(x)-x)/(max(x)-min(x))
}
sz_1 <- apply(mydata[,c(2,3,5,6,8,10)],2,min.max.norm)#正向
sz_2 <- apply(mydata[,c(1,4,7,9,11,12)],2,max.min.nrom)#负向
sz_t <- cbind(sz_1,sz_2)
#求出所有样本对指标XJ的贡献总量
first1 <- function(data){
  x <- c(data)
  for (i in 1:length(data))
    x[i] = data[i]/sum(data[])
  return(x)
}
dataframe <- apply(sz_t,2,first1)
#将上步生成的矩阵每个元素变成每个元素与该ln(元素)的积并计算信息熵
first2 <- function(data)
{
  x <- c(data)
  for(i in 1:length(data)){
    if(data[i] == 0){
      x[i] = 0
    }else{
      x[i] = data[i] * log(data[i])
    }
  }
  return(x)
}
dataframe1 <- apply(dataframe,2,first2)
k <- 1/log(length(dataframe1[,1]))
d <- -k * colSums(dataframe1)
#计算冗余度
d <- 1-d
#计算各项指标的权重
w <- d/sum(d)
w
#计算评分
w1 <- as.data.frame(w)
dataframe0 <- as.data.frame(dataframe)
mydata$score <- dataframe0$TG_CAP * w1[1,] * 1000000 +
           dataframe0$PER_CAP * w1[2,] * 1000000 +
           dataframe0$PERCENT_GY * w1[3,] * 1000000 +
           dataframe0$PERCENT_SY * w1[4,] * 1000000 +
           dataframe0$CL_01 * w1[5,] * 1000000 +
           dataframe0$DATATYPE_DLD * w1[6,] * 1000000 +
           dataframe0$CHG_DATE * w1[7,] * 1000000 +
           dataframe0$PERCENT_JM * w1[8,] * 1000000 +
           dataframe0$PERCENT_NY * w1[9,] * 1000000 +
           dataframe0$CL_02 * w1[10,] * 1000000 +
           dataframe0$DATATYPE_DX * w1[11,] * 1000000 +
           dataframe0$CD_TOTAL * w1[12,] * 1000000

