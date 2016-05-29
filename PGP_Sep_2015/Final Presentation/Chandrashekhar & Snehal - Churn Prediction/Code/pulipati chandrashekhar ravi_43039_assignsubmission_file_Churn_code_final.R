library(data.table)
library(lubridate)
library(dplyr)

#reading all the files of july month
jul_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_data1.csv"))
jul_pay<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_pay1.csv"))
jul_usg<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Jul_usg1.csv"))

#change the colnames
colnames(jul_data) <- c("id","J_Usr-2g","J_Usr-3g","J_Vol-2g","J_Vol-3g")

colnames(jul_pay) <- c("id","J_cust_value_seg","J_cust_group",
                       "J_section","J_acct","J_mob_actv","J_REV","J_RC",
                       "J_NRC","J_ADJ","J_Usage")

colnames(jul_usg)[1] <-"id"

#merge data
m <- merge(jul_data,jul_pay,by = "id",all = TRUE)
final_merge <-merge(jul_usg,m,by = "id",all = TRUE)


#derive variables from final_merge
final_merge <- final_merge %>%
  mutate(J_LOC_OG = var2a+var4a) %>%
  mutate(J_STD_OG = var3a+var5a) %>%
  mutate(J_TOT_OG = var9a+var17a+J_LOC_OG+J_STD_OG) %>%
  mutate(J_LOC_IC = var11a+var13a) %>%
  mutate(J_STD_IC = var12a+var14a) %>%
  mutate(J_TOT_ROAM = var17a+var18a) %>%
  mutate(J_TOT_IC = var16a+var18a+J_LOC_IC+J_STD_IC) %>%
  mutate(J_TOT_MOU_OGIC = J_TOT_OG+J_TOT_IC)
#
final_merge <- as.data.frame(final_merge)

#remove the unnecessary columns
final_merge <- final_merge[,-c(2:17)]

#finding NA values of every column
sapply(final_merge, function(x) sum(is.na(x)))

#check the class of every column
str(final_merge)
sapply(final_merge,class)


#there are two columns "J_acct","J_mob_actv" with same data delete one column
final_merge[[9]] <- NULL

#change the format of date 
final_merge$J_mob_actv <-format(dmy(final_merge$J_mob_actv),"%d/%m/%Y")

#replace the NA values
final_merge$J_mob_actv <-replace(final_merge$J_mob_actv,is.na(final_merge$J_mob_actv),"01/07/2013")

#check if the na value still exist
sum(is.na(final_merge$J_mob_actv))

#check duplicate id
dup <- final_merge[duplicated(final_merge),]
uni <- unique(final_merge,by ="id")

#write the file in hdfs
write.csv(uni, file ="JULY_data.csv")
hdfs.put("JULY_data.csv","/tmp/snehal_project/")

########################################################################################

#reading all the files of august month
aug_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_data1.csv"))
aug_pay<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_pay1.csv"))
aug_usg<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_usg1.csv"))

#change the colnames
colnames(aug_data) <- c("id","A_Usr-2g","A_Usr-3g","A_Vol-2g","A_Vol-3g")

colnames(aug_pay) <- c("id","A_cust_value_seg","A_cust_group",
                       "A_section","A_acct","A_mob_actv","A_REV","A_RC",
                       "A_NRC","A_ADJ","A_Usage")

colnames(aug_usg)[1] <-"id"

#merge data
m <- merge(aug_data,aug_pay,by = "id",all = TRUE)
final_merge <-merge(aug_usg,m,by = "id",all = TRUE)

#derive variables from final_merge
final_merge <- final_merge %>%
  mutate(A_LOC_OG = var2a+var4a) %>%
  mutate(A_STD_OG = var3a+var5a) %>%
  mutate(A_TOT_OG = var9a+var17a+A_LOC_OG+A_STD_OG) %>%
  mutate(A_LOC_IC = var11a+var13a) %>%
  mutate(A_STD_IC = var12a+var14a) %>%
  mutate(A_TOT_ROAM = var17a+var18a) %>%
  mutate(A_TOT_IC = var16a+var18a+A_LOC_IC+A_STD_IC) %>%
  mutate(A_TOT_MOU_OGIC = A_TOT_OG+A_TOT_IC)
#
final_merge <- as.data.frame(final_merge)

#remove the unnecessary columns
final_merge <- final_merge[,-c(2:17)]

#finding NA values of every column
sapply(final_merge, function(x) sum(is.na(x)))

#check the class of every column
str(final_merge)
sapply(final_merge,class)


#there are two columns "A_acct","A_mob_actv" with same data delete one column
final_merge[[9]] <- NULL

#change the format of date 
final_merge$A_mob_actv <-format(dmy(final_merge$A_mob_actv),"%d/%m/%Y")

#replace the NA values
final_merge$A_mob_actv <-replace(final_merge$A_mob_actv,is.na(final_merge$A_mob_actv),"01/08/2013")

#check if the na value still exist
sum(is.na(final_merge$A_mob_actv))


#check duplicate id
dup <- final_merge[duplicated(final_merge),]
uni <- unique(final_merge,by ="id")

#write the file in hdfs
write.csv(uni, file ="AUGUST_data.csv")
hdfs.put("AUGUST_data.csv","/tmp/snehal_project/")

########################################################################
sep_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_data1.csv"))
sep_pay<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_pay1.csv"))
sep_usg<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_usg1.csv"))

#change the colnames
colnames(sep_data) <- c("id","S_Usr-2g","S_Usr-3g","S_Vol-2g","S_Vol-3g")

colnames(sep_pay) <- c("id","S_cust_value_seg","S_cust_group",
                       "S_section","S_acct","S_mob_actv","S_REV","S_RC",
                       "S_NRC","S_ADJ","S_Usage")

colnames(sep_usg)[1] <-"id"

#merge data
m <- merge(sep_data,sep_pay,by = "id",all = TRUE)
final_merge <-merge(sep_usg,m,by = "id",all = TRUE)

#derive variables from final_merge
final_merge <- final_merge %>%
  mutate(S_LOC_OG = var2a+var4a) %>%
  mutate(S_STD_OG = var3a+var5a) %>%
  mutate(S_TOT_OG = var9a+var17a+S_LOC_OG+S_STD_OG) %>%
  mutate(S_LOC_IC = var11a+var13a) %>%
  mutate(S_STD_IC = var12a+var14a) %>%
  mutate(S_TOT_ROAM = var17a+var18a) %>%
  mutate(S_TOT_IC = var16a+var18a+S_LOC_IC+S_STD_IC) %>%
  mutate(S_TOT_MOU_OGIC = S_TOT_OG+S_TOT_IC)
#
final_merge <- as.data.frame(final_merge)

#remove the unnecessary columns
final_merge <- final_merge[,-c(2:17)]

#finding NA values of every column
sapply(final_merge, function(x) sum(is.na(x)))

#check the class of every column
str(final_merge)
sapply(final_merge,class)

#there are two columns "S_acct","S_mob_actv" with same data delete one column
final_merge[[9]] <- NULL

#change the format of date 
final_merge$S_mob_actv <-format(dmy(final_merge$S_mob_actv),"%d/%m/%Y")

#replace the NA values
final_merge$S_mob_actv <-replace(final_merge$S_mob_actv,is.na(final_merge$S_mob_actv),"01/09/2013")

#check if the na value still exist
sum(is.na(final_merge$S_mob_actv))

#check duplicate id
dup <- final_merge[duplicated(final_merge),]
uni <- unique(final_merge,by ="id")

#write the file in hdfs
write.csv(uni, file ="SEPTEMBER_data.csv")
hdfs.put("SEPTEMBER_data.csv","/tmp/snehal_project/")

######################################################################################
#reading all the files of october month
oct_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_data1.csv"))
oct_pay<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_pay1.csv"))
oct_usg<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_usg1.csv"))

#change the colnames
colnames(oct_data) <- c("id","O_Usr-2g","O_Usr-3g","O_Vol-2g","O_Vol-3g")

colnames(oct_pay) <- c("id","O_cust_value_seg","O_cust_group",
                       "O_section","O_acct","O_mob_actv","O_REV","O_RC",
                       "O_NRC","O_ADJ","O_Usage")

colnames(oct_usg)[1] <-"id"
#merge data
m <- merge(oct_data,oct_pay,by = "id",all = TRUE)
final_merge <-merge(oct_usg,m,by = "id",all = TRUE)

#derive variables from final_merge
final_merge <- final_merge %>%
  mutate(O_LOC_OG = var2a+var4a) %>%
  mutate(O_STD_OG = var3a+var5a) %>%
  mutate(O_TOT_OG = var9a+var17a+O_LOC_OG+O_STD_OG) %>%
  mutate(O_LOC_IC = var11a+var13a) %>%
  mutate(O_STD_IC = var12a+var14a) %>%
  mutate(O_TOT_ROAM = var17a+var18a) %>%
  mutate(O_TOT_IC = var16a+var18a+O_LOC_IC+O_STD_IC) %>%
  mutate(O_TOT_MOU_OGIC = O_TOT_OG+O_TOT_IC)
#
final_merge <- as.data.frame(final_merge)

#remove the unnecessary columns
final_merge <- final_merge[,-c(2:17)]

#finding NA values of every column
sapply(final_merge, function(x) sum(is.na(x)))

#check the class of every column
str(final_merge)
sapply(final_merge,class)

#there are two columns "O_acct","O_mob_actv" with same data delete one column
final_merge[[9]] <- NULL

#change the format of date 
final_merge$O_mob_actv <-format(dmy(final_merge$O_mob_actv),"%d/%m/%Y")

#replace the NA values
final_merge$O_mob_actv <-replace(final_merge$O_mob_actv,is.na(final_merge$O_mob_actv),"01/10/2013")

#check if the na value still exist
sum(is.na(final_merge$O_mob_actv))

#check duplicate id
dup <- final_merge[duplicated(final_merge),]
uni <- unique(final_merge,by ="id")

#write the file in hdfs
write.csv(uni, file ="OCTOBER_data.csv")
hdfs.put("OCTOBER_data.csv","/tmp/snehal_project/")

################################################################################
nov_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_data1.csv"))
nov_pay<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_pay1.csv"))
nov_usg<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_usg1.csv"))

#change the colnames
colnames(nov_data) <- c("id","N_Usr-2g","N_Usr-3g","N_Vol-2g","N_Vol-3g")

colnames(nov_pay) <- c("id","N_cust_value_seg","N_cust_group",
                       "N_section","N_acct","N_mob_actv","N_REV","N_RC",
                       "N_NRC","N_ADJ","N_Usage")

colnames(nov_usg)[1] <-"id"

#merge data
m <- merge(nov_data,nov_pay,by = "id",all = TRUE)
final_merge <-merge(nov_usg,m,by = "id",all = TRUE)

#derive variables from final_merge
final_merge <- final_merge %>%
  mutate(N_LOC_OG = var2a+var4a) %>%
  mutate(N_STD_OG = var3a+var5a) %>%
  mutate(N_TOT_OG = var9a+var17a+N_LOC_OG+N_STD_OG) %>%
  mutate(N_LOC_IC = var11a+var13a) %>%
  mutate(N_STD_IC = var12a+var14a) %>%
  mutate(N_TOT_ROAM = var17a+var18a) %>%
  mutate(N_TOT_IC = var16a+var18a+N_LOC_IC+N_STD_IC) %>%
  mutate(N_TOT_MOU_OGIC = N_TOT_OG+N_TOT_IC)
#
final_merge <- as.data.frame(final_merge)

#remove the unnecessary columns
final_merge <- final_merge[,-c(2:17)]

#finding NA values of every column
sapply(final_merge, function(x) sum(is.na(x)))

#check the class of every column
str(final_merge)
sapply(final_merge,class)

#there are two columns "N_acct","N_mob_actv" with same data delete one column
final_merge[[9]] <- NULL

#change the format of date 
final_merge$N_mob_actv <-format(dmy(final_merge$N_mob_actv),"%d/%m/%Y")

#replace the NA values
final_merge$N_mob_actv <-replace(final_merge$N_mob_actv,is.na(final_merge$N_mob_actv),"01/11/2013")

#check if the na value still exist
sum(is.na(final_merge$N_mob_actv))

#check duplicate id
dup <- final_merge[duplicated(final_merge),]
uni <- unique(final_merge,by ="id")

#write the file in hdfs
write.csv(uni, file ="NOVEMBER_data.csv")
hdfs.put("NOVEMBER_data.csv","/tmp/snehal_project/")

#######################################################
jul <- fread(paste("/opt/hadoop/bin/hadoop","fs -text /tmp/snehal_project/JULY_data.csv"))
aug <- fread(paste("/opt/hadoop/bin/hadoop","fs -text /tmp/snehal_project/AUGUST_data.csv"))
sep <- fread(paste("/opt/hadoop/bin/hadoop","fs -text /tmp/snehal_project/SEPTEMBER_data.csv"))
oct <- fread(paste("/opt/hadoop/bin/hadoop","fs -text /tmp/snehal_project/OCTOBER_data.csv"))
nov <- fread(paste("/opt/hadoop/bin/hadoop","fs -text /tmp/snehal_project/NOVEMBER_data.csv"))

m <- merge(jul,aug,by = "id",all = TRUE)
m1 <- merge(m,sep,by = "id",all = TRUE)
m2 <- merge(m1,oct,by="id",all=TRUE)
m2$V1.x <- NULL
m2$V1.y <- NULL
m2$V1.x <- NULL

m3 <-merge(m2,nov,by="id",all=TRUE)

#write the final csv file
write.csv(m3, file ="all_months.csv")
hdfs.put("/home/snehal_s/all_months.csv","/tmp/snehal_project/")


#unlist(".RData")
#rm(list = ls())

###############################################################Pre-processing
library(data.table)
a <- fread("/home/snehal_s/all_months.csv")
all$V1 <- NULL
sapply(all, function(x) sum(is.na(x)))

#remove columns which are not necessary
all$V1 <- NULL
all$J_cust_value_seg <- NULL
all$J_cust_group <- NULL
all$J_section <- NULL
all$A_cust_value_seg <- NULL
all$A_cust_group <- NULL
all$A_section <- NULL
all$S_cust_value_seg <- NULL
all$S_cust_group <- NULL
all$S_section <- NULL
all$O_cust_value_seg <- NULL
all$V1.y <- NULL
all$O_cust_group <- NULL
all$O_section <- NULL
all$N_cust_value_seg <- NULL
all$N_cust_group <- NULL
all$N_section <- NULL

#write the final csv file(not needed)
write.csv(all, file ="all_months_without_columns.csv")

hdfs.put("/home/snehal_s/all_months_without_columns.csv","/tmp/snehal_project/")

################################################################################
#all the columns without the columns whic are not needed
#converting into data frame
all <- as.data.frame(all)

#removing outliers function for numeric columns to detect outlier
outliers <- function(x){
  quantiles <- quantile( x, c(.1, .9 ), na.rm = TRUE)
  x[ x <= quantiles[1] ] <- quantiles[1]
  x[ x >= quantiles[2] ] <- quantiles[2]
  x
}

#removing the character columns as the outlier function won't work on it
num <- all[,-c(1,6,24,42,60,78)]

#apply outlier function on numeric columns
apply_outlier <-as.data.frame(apply(num, 2, outliers))

#columns bind the character and the numeric columns after applying outliers function
char <- all[,c(1,6,24,42,60,78)]
col_bind <- as.data.frame(cbind(char,apply_outlier))

#there are na values in each month so replace it with the 1st date of that month

#change the format of date july month
library(lubridate)
col_bind$J_mob_actv <-format(dmy(col_bind$J_mob_actv),"%d/%m/%Y")
#replace the NA values
col_bind$J_mob_actv <-replace(col_bind$J_mob_actv,is.na(col_bind$J_mob_actv),"01/07/2013")

#change the format of date august month
col_bind$A_mob_actv <-format(dmy(col_bind$A_mob_actv),"%d/%m/%Y")
#replace the NA values
col_bind$A_mob_actv <-replace(col_bind$A_mob_actv,is.na(col_bind$A_mob_actv),"01/08/2013")
sum(is.na(col_bind$A_mob_actv))

#change the format of date september month
col_bind$S_mob_actv <-format(dmy(col_bind$S_mob_actv),"%d/%m/%Y")
#replace the NA values
col_bind$S_mob_actv <-replace(col_bind$S_mob_actv,is.na(col_bind$S_mob_actv),"01/09/2013")
sum(is.na(col_bind$S_mob_actv))

#change the format of date october month
col_bind$O_mob_actv <-format(dmy(col_bind$O_mob_actv),"%d/%m/%Y")
#replace the NA values
col_bind$O_mob_actv <-replace(col_bind$O_mob_actv,is.na(col_bind$O_mob_actv),"01/10/2013")
sum(is.na(col_bind$O_mob_actv))

#change the format of date august month
col_bind$N_mob_actv <-format(dmy(col_bind$N_mob_actv),"%d/%m/%Y")
#replace the NA values
col_bind$N_mob_actv <-replace(col_bind$N_mob_actv,is.na(col_bind$N_mob_actv),"01/11/2013")
sum(is.na(col_bind$N_mob_actv))


#replacing numeric with median
numeric_col <- col_bind[,c(7:91)]

#median function to replace the numeric columns with median
Median=function(x){
  x<-as.numeric(as.character(x))
  x[is.na(x)] =median(x, na.rm=TRUE)
  x
}

#apply median function on numeric column
numeric_median <- as.data.frame(apply(numeric_col,2,Median))

#column bind numeric_median and character column
char_col <- col_bind [,c(1:6)]
num_char_col <- as.data.frame(cbind(char_col,numeric_median))

sapply(num_char_col, function(x) sum(is.na(x)))

#write this in a csv file without any NA's
write.csv(num_char_col, file ="new_months_without_na.csv")

###########################################################################

#read the csv file with all the months without any NA's
w_na <-fread("/home/hduser/new_months_without_na.csv")
w_na$V1 <- NULL

#add disconnection file
Discon <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Discon1.csv"))

Discon <- as.data.frame(Discon)
Discon$Churn <- 1
#change the column name of id so it can be merge with the months file 
colnames(Discon)[1] <- "id"
colnames(Discon)[2] <- "Discon_Date"

#merge the file with the months not containing na's,i.e w_na
all_months_Dis<- merge(w_na, Discon, by = "id", all = TRUE)
all_months_Dis$Churn[is.na(all_months_Dis$Churn)] <- 0

#change the format of the date
all_months_Dis$Discon_Date <- format(dmy(all_months_Dis$Discon_Date),"%d/%m/%Y")
all_months_Dis$Discon_Date <-replace(all_months_Dis$Discon_Date ,is.na(all_months_Dis$Discon_Date),"30/11/2014")

#to check if there are any unique id's
uniq <- unique(all_months_Dis,by="id")

#there are 1831489 unique ids so we take that id's only
#write this into another csv
write.csv(uniq, file ="/home/hduser/snehal/final_months_with_discon.csv")

#################################################################################
#read the file whick consists of all the months and the discon col 
m_d <- fread("/home/hduser/snehal/final_months_with_discon.csv")
m_d <- as.data.frame(m_d)

#there are na's which got introduced after merging discon file replace that na's with median 
m_d$V1 <- NULL

#separate the character and numeric columns
char_c <- m_d[,c(1:6,92,93)]
num_c <- m_d[,c(7:91)]

#applying median function on num_c
Median=function(x){
  x<-as.numeric(as.character(x))
  x[is.na(x)] =median(x, na.rm=TRUE)
  x
}

#apply median function on numeric column
numeric_median <- as.data.frame(apply(num_c,2,Median))

#there are na values in each month so replace it with the last date of that month

library(lubridate)

#replace the NA values of the july month 
m_d$J_mob_actv <-replace(m_d$J_mob_actv,is.na(m_d$J_mob_actv),"01/07/2013")
sum(is.na(m_d$J_mob_actv))

#replace the NA values of the august month
m_d$A_mob_actv <-replace(m_d$A_mob_actv,is.na(m_d$A_mob_actv),"01/08/2013")
sum(is.na(m_d$A_mob_actv))

#replace the NA values of the september month
m_d$S_mob_actv <-replace(m_d$S_mob_actv,is.na(m_d$S_mob_actv),"01/09/2013")
sum(is.na(m_d$S_mob_actv))

#replace the NA values of the october
m_d$O_mob_actv <-replace(m_d$O_mob_actv,is.na(m_d$O_mob_actv),"01/10/2013")
sum(is.na(m_d$O_mob_actv))

#replace the NA values of the november
m_d$N_mob_actv <-replace(m_d$N_mob_actv,is.na(m_d$N_mob_actv),"01/11/2013")
sum(is.na(m_d$N_mob_actv))

#cbind m_d with the numeric column ie m_d
final_df <- cbind(char_c,numeric_median)

char_c <- m_d[,c(1:6,92,93)]

#check for na
sapply(final_df, function(x) sum(is.na(x)))

#write this into csv
write.csv(final_df , file = "/home/hduser/snehal/final_churn_file.csv")

#########################################################################################################
library(data.table)

#dividing the data into test and train dataset
final_churn<- fread("/home/hduser/snehal/final_churn_file.csv")
View(final_churn)
all_months_Dis$Discon_Date <- format(dmy(all_months_Dis$Discon_Date),"%d/%m/%Y")
final_churn$V1=NULL
class(final_churn)
final_churn <- as.data.frame(final_churn)

#changing the class of the columns
final_churn$Churn=as.factor(final_churn$Churn)
final_churn$J_mob_actv <- as.Date(final_churn$J_mob_actv,"01/07/2013")
test$J_mob_actv <-replace(test$J_mob_actv,is.na(test$J_mob_actv),"01/07/2013")
final_churn$A_mob_actv <- as.Date(final_churn$A_mob_actv)
final_churn$S_mob_actv <- as.Date(final_churn$S_mob_actv)
final_churn$O_mob_actv <- as.Date(final_churn$O_mob_actv)
final_churn$N_mob_actv <- as.Date(final_churn$N_mob_actv)
final_churn$Discon_Date <- as.Date(final_churn$Discon_Date)

class1 <- subset(final_churn,Churn=="1")
nrow(class1)

class0 <- subset(final_churn,Churn=="0")
nrow(class0)

s1 <- sample(nrow(class1))
s0 <- sample(nrow(class0))

train1 <- class1[s1[1:round(nrow(class1)*0.7)],]
test1 <- class1[s1[(round(nrow(class1)*0.7)+1):nrow(class1)],]

train0 <- class0[s0[1:round(nrow(class0)*0.7)],]
test0 <- class0[s0[(round(nrow(class0)*0.7)+1):nrow(class0)],]

train <- rbind(train1,train0)
test <- rbind(test1, test0)


write.csv(train, "/home/hduser/snehal/train.csv")
write.csv(test, "/home/hduser/snehal/test.csv")


class(train$Churn)


### changing date format of test data 
class(test$J_mob_actv)
class(test$Churn)

library(data.table)
library(car)
library(caret)
library(lubridate)

setwd("/home/hduser/snehal/")
train = fread("/home/hduser/snehal/train.csv")


# removing the column

train$V1 = NULL
train$id = NULL

#changing the date format

train$J_mob_actv <-format(dmy(train$J_mob_actv),"%Y/%m/%d")
train$J_mob_actv <-replace(train$J_mob_actv,is.na(train$J_mob_actv),"2013/07/01")
train$A_mob_actv <-format(dmy(train$A_mob_actv),"%Y/%m/%d")
train$A_mob_actv <-replace(train$A_mob_actv,is.na(train$A_mob_actv),"2013/08/01")
train$S_mob_actv <-format(dmy(train$S_mob_actv),"%Y/%m/%d")
train$S_mob_actv <-replace(train$S_mob_actv,is.na(train$S_mob_actv),"2013/09/01")
train$O_mob_actv <-format(dmy(train$O_mob_actv),"%Y/%m/%d")
train$O_mob_actv <-replace(train$O_mob_actv,is.na(train$O_mob_actv),"2013/10/01")
train$N_mob_actv <-format(dmy(train$N_mob_actv),"%Y/%m/%d")
train$N_mob_actv <-replace(train$N_mob_actv,is.na(train$N_mob_actv),"2013/11/01")
train$Discon_Date <-format(dmy(train$Discon_Date),"%Y/%m/%d")
train$Discon_Date <-replace(train$Discon_Date,is.na(train$Discon_Date),"2013/12/01")

#changing the class of the columns

train$J_mob_actv <- as.Date(train$J_mob_actv)
train$A_mob_actv <- as.Date(train$A_mob_actv)
train$S_mob_actv <- as.Date(train$S_mob_actv)
train$O_mob_actv <- as.Date(train$O_mob_actv)
train$N_mob_actv <- as.Date(train$N_mob_actv)
train$Discon_Date <- as.Date(train$Discon_Date)

class(train$Churn)
train$Churn=as.factor(train$Churn)

sum(is.na(train))
View(train)

# reading the test data set

test = fread("/home/hduser/snehal/test.csv")


#removing the column

test$V1 = NULL
test$id=NULL

#changing the date format

test$J_mob_actv <-format(dmy(test$J_mob_actv),"%Y/%m/%d")
test$J_mob_actv <-replace(test$J_mob_actv,is.na(test$J_mob_actv),"2013/07/01")
test$A_mob_actv <-format(dmy(test$A_mob_actv),"%Y/%m/%d")
test$A_mob_actv <-replace(test$A_mob_actv,is.na(test$A_mob_actv),"2013/08/01")
test$S_mob_actv <-format(dmy(test$S_mob_actv),"%Y/%m/%d")
test$S_mob_actv <-replace(test$S_mob_actv,is.na(test$S_mob_actv),"2013/09/01")
test$O_mob_actv <-format(dmy(test$O_mob_actv),"%Y/%m/%d")
test$O_mob_actv <-replace(test$O_mob_actv,is.na(test$O_mob_actv),"2013/10/01")
test$N_mob_actv <-format(dmy(test$N_mob_actv),"%Y/%m/%d")
test$N_mob_actv <-replace(test$N_mob_actv,is.na(test$N_mob_actv),"2013/11/01")
test$Discon_Date <-format(dmy(test$Discon_Date),"%Y/%m/%d")
test$Discon_Date <-replace(test$Discon_Date,is.na(test$Discon_Date),"2013/12/01")


#changing the class of the columns

test$J_mob_actv <- as.Date(test$J_mob_actv)
test$A_mob_actv <- as.Date(test$A_mob_actv)
test$S_mob_actv <- as.Date(test$S_mob_actv)
test$O_mob_actv <- as.Date(test$O_mob_actv)
test$N_mob_actv <- as.Date(test$N_mob_actv)
test$Discon_Date <- as.Date(test$Discon_Date)


class(test$Churn)
test$Churn=as.factor(test$Churn)
View(test)

#Applying the logistic regression

fit = glm( Churn ~.,family = binomial(link = "logit"),data = train)

summary(fit)



#prediction 

predict=predict(fit,test,type="response")
predict
summary(predict)

test$V6 <- ifelse(predict >= 0.5, 1, 0)
View(test)

#confusion matrix
confusionMatrix(test$V6,test$Churn)
roc_fit <- roc(test$Churn,test$V6)
plot(roc_fit)



