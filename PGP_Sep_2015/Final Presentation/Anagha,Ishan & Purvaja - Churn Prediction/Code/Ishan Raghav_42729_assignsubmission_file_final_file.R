######################### CHURN PREDICTION MODEL #############################

### JULY ###

### Reading July Data File ###
July_data1=fread("~/Documents/Aegis/project churn/Data/July_data1.csv",na.strings = c("NA"))
July_data1_data=as.data.frame(July_data1)
July_d=July_d=unique(July_data1_data,by='id')

### Reading July Pay File ###
July_pay1=fread("~/Documents/Aegis/project churn/Data/July_pay1.csv",na.strings = c("NA"))
July_pay1_data=as.data.frame(July_pay1)
July_p=unique(July_pay1_data,by='id')

### Reading July Usage File ###
July_usg1=fread("~/Documents/Aegis/project churn/Data/Jul_usg1.csv",na.strings = c("NA"))
July_usg1_data=as.data.frame(July_usg1)

### Renaming the column name ###
setnames(July_usg1_data,'ID','id')
July_u=unique(July_usg1_data,by='id')

### Merge the three files ###
jpu=merge(July_p,July_u,by='id',all=TRUE)
jul=merge(jpu,July_d,by='id',all=TRUE)

### Date Format ###
jul$acct_actv1=as.Date(jul$acct_actv1, "%d%b%Y")
jul$mob_actv1=as.Date(jul$mob_actv1, "%d%b%Y")

jul$cust_value_seg=as.factor(jul$cust_value_seg)
jul$cust_group=as.factor(jul$cust_group)
jul$section=as.factor(jul$section)

#Ignoring the rows which has more than 27 Na's
jul = jul[rowSums(is.na(jul)) <= 27,]

#defining a fucntion to replace the outliers by NA using IQR
remove_outliers=function(x) {
  # Get Q1 and Q3
  qnt=quantile(x, probs=c(.25, .75),na.rm = TRUE)
  # Get the interquartile range time 1.5
  H=1.5 * IQR(x, na.rm = TRUE)
  # Apply on a copy of the original data
  y=x
  y[x < (qnt[1] - H)]=NA
  y[x > (qnt[2] + H)]=NA
  y
}


outj=data.frame(apply(jul[7:31],2,remove_outliers))
july=cbind(jul[,1:6],outj)

#defining a mode function to replace the categorical values with mode
getmode=function(v) {
  v[which.max(v)]
}

july[,2][is.na(july[,2])]=getmode(july[,2])
july[,3][is.na(july[,3])]=getmode(july[,3])
july[,4][is.na(july[,4])]=getmode(july[,4])

summary(july)

#replacing the NA's using mean
for(i in 1:ncol(july[,7:31])){
  july[,7:31][is.na(july[,7:31][,i]), i]=mean(july[,7:31][,i], na.rm = TRUE)
}           


july$cust_group=NULL
july$section=NULL
july$mob_actv1=NULL
july$var17a.x=NULL
july$var18a.x=NULL
july$var9a=NULL
july$VAR2=NULL
july$Var3=NULL

#replacing the NA's in the acctivation column with the first date of the July month
july$acct_actv1[is.na(july$acct_actv1)]="2014-07-01"

#renaming the columns
colnames(july) = c("id","Cust_Value_Seg","Acct_actv","Tot_Usg","RC","Rev","LOC_OG_L","STD_OG_L","LOC_OG_M","STD_OG_M","STD_OG","Tot_OG","LOC_IC_L","STD_IC_L","LOC_IC_M","STD_IC_M","STD_IC","ISD_IC","Ro_OG","Ro_IC","Tot_IC","Volg2G","Volg3G")

july[,24]=NULL

#deriving variables
july=mutate(july, DataRatio = Volg2G / (Volg2G + Volg3G))
july=mutate(july, tot_IC_L = LOC_IC_L + STD_IC_L)
july=mutate(july, tot_OG_L = LOC_OG_L + STD_OG_L)
july=mutate(july, tot_IC_M = LOC_IC_M + STD_IC_M)
july=mutate(july, tot_OG_M = LOC_OG_M + STD_OG_M)
july=mutate(july, tot_L = LOC_IC_L + STD_IC_L + LOC_OG_L + STD_OG_L)
july=mutate(july, tot_M = LOC_IC_M + STD_IC_M + LOC_OG_M + STD_OG_M)
july=mutate(july, tot_Ratio = (tot_L / (tot_L + tot_M)))
july=mutate(july, tot_OG = LOC_OG_L + STD_OG_L + LOC_OG_M + STD_OG_M + STD_OG)
july=mutate(july, tot_IC = LOC_IC_L + STD_IC_L + LOC_IC_M + STD_IC_M + STD_IC + ISD_IC)
july=mutate(july, Ratio_OG = (tot_OG / (tot_OG + tot_IC)))
july=mutate(july, Ratio_IC = (tot_IC / (tot_OG + tot_IC)))
july=mutate(july, tot_Roam = Ro_OG + Ro_IC)


july=july[-c(7:21)]


write.csv(july, file = 'Jul_fin.csv')

############################# August ##############################################
#Read the CSV Data file
Aug1 <-fread("Aug_data1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Aug_Unique_Data <- unique(Aug1, by = "id")


######################################################################################

#Read the csv Pay file
Aug_pay <-fread("Aug_pay1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Aug_pay1 = as.data.frame(Aug_pay)

Aug_Unique_Pay <- unique(Aug_pay, by = "id")

######################################################################################

#Read the CSV Usage file

Aug_usage <-fread("Aug_usg1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Aug_usage = as.data.frame(Aug_usage)

colnames(Aug_usage)[1]<- 'id'

Aug_Unique_Usage <- unique(Aug_usage, by = "id")


#####################################################################################

#Merge all three files

Aug_Pay_Usg = merge(Aug_Unique_Pay,Aug_Unique_Usage, by = "id",all=TRUE)
Aug_Final = merge(Aug_Pay_Usg,Aug_Unique_Data, by = "id", all=TRUE)

Aug_Final = as.data.frame(Aug_Final)

#####################################################################################

#Ignoring the rows which has more than 27 Na's
Aug_Final = Aug_Final[rowSums(is.na(Aug_Final)) <= 27,]

#Outlier Removal

remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

summary(Aug_Final)

Fin1 <- as.data.frame(apply(Aug_Final[,7:31],2, remove_outliers))

#Replace NA with mean of the column
Fin1[] <- lapply(Fin1, function(x) { 
  x[is.na(x)] <- mean(x, na.rm = TRUE)
  x
})
Fin1=as.data.frame(Fin1)

#Replace NA with Mode

Aug=Aug_Final[,2]
Aug=as.data.frame(Aug,stringsAsFactors = FALSE)

Augu = apply(Aug, 2, function(x){ 
  x[is.na(x)] <- names(which.max(table(x)))
  return(x)})

Augu=as.data.frame(Augu)

### Replace the NA with last date of the month
Aug_Final$acct_actv1[is.na(Aug_Final$acct_actv1)] <- "01AUG2014"


#Removing the duplicate columns
August_Final$cust_group <- NULL
August_Final$section <- NULL
August_Final$var17a.x <- NULL
August_Final$var18a.x <- NULL
August_Final$var9a <- NULL
August_Final$mob_actv1 <- NULL
August_Final$VAR2 <- NULL
August_Final$VAR3 <- NULL

#Cbind
August_Final = cbind(August_Final$id,Augu,August_Final[,3:23])

colnames(August_Final) = c("id","Cust_Value_Seg","Acct_actv","Tot_Usg","RC","Rev","LOC_OG_L","STD_OG_L","LOC_OG_M","STD_OG_M","STD_OG","Tot_OG","LOC_IC_L","STD_IC_L","LOC_IC_M","STD_IC_M","STD_IC","ISD_IC","Ro_OG","Ro_IC","Tot_IC","Volg2G","Volg3G")

#Deriving Variables

library(dplyr)

August_Final <- mutate(August_Final, DataRatio = Volg2G / (Volg2G + Volg3G))
August_Final <- mutate(August_Final, tot_IC_L = LOC_IC_L + STD_IC_L)
August_Final <- mutate(August_Final, tot_OG_L = LOC_OG_L + LOC_OG_L)
August_Final <- mutate(August_Final, tot_IC_M = LOC_IC_M + STD_IC_M)
August_Final <- mutate(August_Final, tot_OG_M = LOC_OG_M + STD_OG_M)
August_Final <- mutate(August_Final, tot_L = LOC_IC_L + STD_IC_L + LOC_OG_L + LOC_OG_L)
August_Final <- mutate(August_Final, tot_M = LOC_IC_M + STD_IC_M + LOC_OG_M + STD_OG_M)
August_Final <- mutate(August_Final, tot_Ratio = (tot_L / (tot_L + tot_M)))
August_Final <- mutate(August_Final, tot_OG = LOC_OG_L + STD_OG_L + LOC_OG_M + STD_OG_M + STD_OG)
August_Final <- mutate(August_Final, tot_IC = LOC_IC_L + STD_IC_L + LOC_IC_M + STD_IC_M + STD_IC + ISD_IC)
August_Final <- mutate(August_Final, Ratio_OG  = (tot_OG / (tot_OG + tot_IC)))
August_Final <- mutate(August_Final, tot_Roam = Ro_OG + Ro_IC)

August_Final <- August_Final[-c(7:21)]

write_csv=write.csv(August_Final,"D:/chrurn data/August.csv")


##### SEPTEMBER #######

### Reading September Data File ###

Sep_data1=fread("~/Documents/Aegis/project churn/Data/Sep_data1.csv",na.strings = c("NA"))
Sep_data1_data=as.data.frame(Sep_data1)
Sep_d=unique(Sep_data1_data,by='id')

### Reading September Pay File ###

Sep_pay1=fread("~/Documents/Aegis/project churn/Data/Sep_pay1.csv",na.strings = c("NA"))
Sep_pay1_data=as.data.frame(Sep_pay1)
Sep_p=unique(Sep_pay1_data,by='id')

### Reading September Usage File ###

Sep_usg1=fread("~/Documents/Aegis/project churn/Data/Sep_usg1.csv",na.strings = c("NA"))
Sep_usg1_data=as.data.frame(Sep_usg1)
#renaming the column name
setnames(Sep_usg1_data,'ID','id')
Sep_u=unique(Sep_usg1_data,by='id')

spu=merge(Sep_p,Sep_u,by='id',all=TRUE)
sep=merge(spu,Sep_d,by='id',all=TRUE)

#date format
sep$acct_actv1=as.Date(sep$acct_actv1, "%d%b%Y")
sep$mob_actv1=as.Date(sep$mob_actv1, "%d%b%Y")

sep$cust_value_seg=as.factor(sep$cust_value_seg)
sep$cust_group=as.factor(sep$cust_group)
sep$section=as.factor(sep$section)

##Ignoring the rows which has more than 30 Na's
sep = sep[rowSums(is.na(sep)) <= 27,]

#defining a fucntion to replace the outliers by NA using IQR
remove_outliers=function(x) {
  # Get Q1 and Q3
  qnt=quantile(x, probs=c(.25, .75),na.rm = TRUE)
  # Get the interquartile range time 1.5
  H=1.5 * IQR(x, na.rm = TRUE)
  # Apply on a copy of the original data
  y=x
  y[x < (qnt[1] - H)]=NA
  y[x > (qnt[2] + H)]=NA
  y
}


outs=data.frame(apply(sep[7:31],2,remove_outliers))
sept=cbind(sep[,1:6],outs)

getmode=function(v) {
  v[which.max(v)]
}

sept[,2][is.na(sept[,2])]=getmode(sept[,2])
sept[,3][is.na(sept[,3])]=getmode(sept[,3])
sept[,4][is.na(sept[,4])]=getmode(sept[,4])

#defining a mode function to replace the categorical values with mode
for(i in 1:ncol(sept[,7:31])){
  sept[,7:31][is.na(sept[,7:31][,i]), i]=mean(sept[,7:31][,i], na.rm = TRUE)
}           


sept$cust_group=NULL
sept$section=NULL
sept$mob_actv1=NULL
sept$var17a.x=NULL
sept$var18a.x=NULL
sept$var9a=NULL
sept$VAR2=NULL
sept$VAR3=NULL

#replacing the NA's in the acctivation column with the first date of the September month
sept$acct_actv1[is.na(sept$acct_actv1)]="2014-09-01"

#renaming the columns
colnames(sept) = c("id","Cust_Value_Seg","Acct_actv","Tot_Usg","RC","Rev","LOC_OG_L","STD_OG_L","LOC_OG_M","STD_OG_M","STD_OG","Tot_OG","LOC_IC_L","STD_IC_L","LOC_IC_M","STD_IC_M","STD_IC","ISD_IC","Ro_OG","Ro_IC","Tot_IC","Volg2G","Volg3G")
summary(sept)

sept=mutate(sept, DataRatio = Volg2G / (Volg2G + Volg3G))
sept=mutate(sept, tot_IC_L = LOC_IC_L + STD_IC_L)
sept=mutate(sept, tot_OG_L = LOC_OG_L + STD_OG_L)
sept=mutate(sept, tot_IC_M = LOC_IC_M + STD_IC_M)
sept=mutate(sept, tot_OG_M = LOC_OG_M + STD_OG_M)
sept=mutate(sept, tot_L = LOC_IC_L + STD_IC_L + LOC_OG_L + STD_OG_L)
sept=mutate(sept, tot_M = LOC_IC_M + STD_IC_M + LOC_OG_M + STD_OG_M)
sept=mutate(sept, tot_Ratio = (tot_L / (tot_L + tot_M)))
sept=mutate(sept, tot_OG = LOC_OG_L + STD_OG_L + LOC_OG_M + STD_OG_M + STD_OG)
sept=mutate(sept, tot_IC = LOC_IC_L + STD_IC_L + LOC_IC_M + STD_IC_M + STD_IC + ISD_IC)
sept=mutate(sept, Ratio_OG = (tot_OG / (tot_OG + tot_IC)))
sept=mutate(sept, Ratio_IC = (tot_IC / (tot_OG + tot_IC)))
sept=mutate(sept, tot_Roam = Ro_OG + Ro_IC)

sept=sept[-c(7:21)]


write.csv(sept, file = 'Sep_fin.csv')

#################################### OCTOBER ########################################
#Read the CSV Data file
oct1 <-fread("Oct_data1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
oct1 = as.data.frame(oct1)
Oct_Unique_Data <- unique(oct1, by = "id")


######################################################################################

#Read the csv Pay file
oct_pay <-fread("Oct_pay1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
oct_pay1 = as.data.frame(oct_pay)

Oct_Unique_Pay <- unique(oct_pay, by = "id")

######################################################################################

#Read the CSV Usage file

oct_usage <-fread("Oct_usg1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
oct_usage = as.data.frame(oct_usage)

colnames(oct_usage)[1]<- 'id'

Oct_Unique_Usage <- unique(oct_usage, by = "id")


#####################################################################################

#Merge all three files

Oct_Pay_Usg = merge(Oct_Unique_Pay,Oct_Unique_Usage, by = "id",all=TRUE)
Oct_Final = merge(Oct_Pay_Usg,Oct_Unique_Data, by = "id", all=TRUE)

Oct_Final = as.data.frame(Oct_Final)
#####################################################################################

#Ignoring the rows which has more than 30 Na's
Oct_Final = Oct_Final[rowSums(is.na(Oct_Final)) <= 27,]

#Outlier Removal

remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

Fin <- as.data.frame(apply(Oct_Final[,7:31],2, remove_outliers))

October_Final = cbind(Oct_Final[,1:6],Fin)

#Replace NA with mean of the column
October_Final[] <- lapply(October_Final, function(x) { 
  x[is.na(x)] <- mean(x, na.rm = TRUE)
  x
})
October_Final=as.data.frame(October_Final)

#Replace NA with Mode

oct=Oct_Final[,2]
oct=as.data.frame(oct,stringsAsFactors = FALSE)

Octo = apply(oct, 2, function(x){ 
  x[is.na(x)] <- names(which.max(table(x)))
  return(x)})

Octo=as.data.frame(Octo)

October_Final$acct_actv1[is.na(October_Final$acct_actv1)] <- "01OCT2014"


#Removing the duplicate columns
October_Final$cust_group <- NULL
October_Final$section <- NULL
October_Final$var17a.x <- NULL
October_Final$var18a.x <- NULL
October_Final$var9a <- NULL
October_Final$VAR2 <- NULL
October_Final$VAR3 <- NULL
October_Final$mob_actv1 <- NULL


#Cbind
October_Final = cbind(October_Final$id,Octo,October_Final[,3:23])

colnames(October_Final) = c("id","Cust_Value_Seg","Acct_actv","Tot_Usg","RC","Rev","LOC_OG_L","STD_OG_L","LOC_OG_M","STD_OG_M","STD_OG","Tot_OG","LOC_IC_L","STD_IC_L","LOC_IC_M","STD_IC_M","STD_IC","ISD_IC","Ro_OG","Ro_IC","Tot_IC","Volg2G","Volg3G")

#Deriving Variables

library(dplyr)

October_Final <- mutate(October_Final, DataRatio = Volg2G / (Volg2G + Volg3G))
October_Final <- mutate(October_Final, tot_IC_L = LOC_IC_L + STD_IC_L)
October_Final <- mutate(October_Final, tot_OG_L = LOC_OG_L + STD_OG_L)
October_Final <- mutate(October_Final, tot_IC_M = LOC_IC_M + STD_IC_M)
October_Final <- mutate(October_Final, tot_OG_M = LOC_OG_M + STD_OG_M)
October_Final <- mutate(October_Final, tot_L = LOC_IC_L + STD_IC_L + LOC_OG_L + STD_OG_L)
October_Final <- mutate(October_Final, tot_M = LOC_IC_M + STD_IC_M + LOC_OG_M + STD_OG_M)
October_Final <- mutate(October_Final, tot_Ratio = (tot_L / (tot_L + tot_M)))
October_Final <- mutate(October_Final, tot_OG = LOC_OG_L + STD_OG_L + LOC_OG_M + STD_OG_M + STD_OG)
October_Final <- mutate(October_Final, tot_IC = LOC_IC_L + STD_IC_L + LOC_IC_M + STD_IC_M + STD_IC + ISD_IC)
October_Final <- mutate(October_Final, Ratio_OG  = (tot_OG / (tot_OG + tot_IC)))
October_Final <- mutate(October_Final, Ratio_IC = (tot_IC / (tot_OG + tot_IC)))
October_Final <- mutate(October_Final, tot_Roam = Ro_OG + Ro_IC)

October_Final <- October_Final[-c(7:21)]

write_csv=write.csv(October_Final,"D:/chrurn data/October.csv")


############################## NOVEMBER #############################################
#Read the CSV Data file
Nov <-fread("Nov_data1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Nov = as.data.frame(Nov)
Nov_Unique_Data <- unique(Nov, by = "id")


######################################################################################

#Read the csv Pay file
Nov_pay <-fread("Nov_pay1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Nov_pay1 = as.data.frame(Nov_pay)

Nov_Unique_Pay <- unique(Nov_pay, by = "id")

######################################################################################

#Read the CSV Usage file

Nov_usage <-fread("Nov_usg1.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
Nov_usage = as.data.frame(Nov_usage)

colnames(Nov_usage)[1]<- 'id'

Nov_Unique_Usage <- unique(Nov_usage, by = "id")


#####################################################################################

#Merge all three files

Nov_Pay_Usg = merge(Nov_Unique_Pay,Nov_Unique_Usage, by = "id",all=TRUE)
Nov_Final = merge(Nov_Pay_Usg,Nov_Unique_Data, by = "id", all=TRUE)

Nov_Final = as.data.frame(Nov_Final)

#####################################################################################

#Ignoring the rows which has more than 30 Na's
Nov_Final = Nov_Final[rowSums(is.na(Nov_Final)) <= 27,]

#Outlier Removal

remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

Fin2 <- as.data.frame(apply(Nov_Final[,7:31],2, remove_outliers))

November_Final = cbind(Nov_Final[,1:6],Fin2)

#Removing the duplicate columns
November_Final$cust_group <- NULL
November_Final$section <- NULL
November_Final$var17a.x <- NULL
November_Final$var18a.x <- NULL
November_Final$var9a <- NULL
November_Final$VAR2 <- NULL
November_Final$VAR3 <- NULL
November_Final$mob_actv1 <- NULL

#Replace NA with mean of the column
November_Final[] <- lapply(November_Final, function(x) { 
  x[is.na(x)] <- mean(x, na.rm = TRUE)
  x
})
November_Final=as.data.frame(November_Final)

#Replace NA with Mode

Novem=Nov_Final[,2]
Novem=as.data.frame(Novem,stringsAsFactors = FALSE)

Novm = apply(Novem, 2, function(x){ 
  x[is.na(x)] <- names(which.max(table(x)))
  return(x)})

Novm=as.data.frame(Novm)

November_Final$acct_actv1[is.na(November_Final$acct_actv1)] <- "01NOV2014"

#Cbind
November_Final = cbind(November_Final$id,Novm,November_Final[,3:23])

colnames(November_Final) = c("id","Cust_Value_Seg","Acct_actv","Tot_Usg","RC","Rev","LOC_OG_L","STD_OG_L","LOC_OG_M","STD_OG_M","STD_OG","Tot_OG","LOC_IC_L","STD_IC_L","LOC_IC_M","STD_IC_M","STD_IC","ISD_IC","Ro_OG","Ro_IC","Tot_IC","Volg2G","Volg3G")

#Deriving Variables

library(dplyr)

November_Final <- mutate(November_Final, DataRatio = Volg2G / (Volg2G + Volg3G))
November_Final <- mutate(November_Final, tot_IC_L = LOC_IC_L + STD_IC_L)
November_Final <- mutate(November_Final, tot_OG_L = LOC_OG_L + STD_OG_L)
November_Final <- mutate(November_Final, tot_IC_M = LOC_IC_M + STD_IC_M)
November_Final <- mutate(November_Final, tot_OG_M = LOC_OG_M + STD_OG_M)
November_Final <- mutate(November_Final, tot_L = LOC_IC_L + STD_IC_L + LOC_OG_L + LOC_OG_L)
November_Final <- mutate(November_Final, tot_M = LOC_IC_M + STD_IC_M + LOC_OG_M + STD_OG_M)
November_Final <- mutate(November_Final, tot_Ratio = (tot_L / (tot_L + tot_M)))
November_Final <- mutate(November_Final, tot_OG = LOC_OG_L + STD_OG_L + LOC_OG_M + STD_OG_M + STD_OG)
November_Final <- mutate(November_Final, tot_IC = LOC_IC_L + STD_IC_L + LOC_IC_M + STD_IC_M + STD_IC + ISD_IC)
November_Final <- mutate(November_Final, Ratio_OG  = (tot_OG / (tot_OG + tot_IC)))
November_Final <- mutate(November_Final, Ratio_IC = (tot_IC / (tot_OG + tot_IC)))
November_Final <- mutate(November_Final, tot_Roam = Ro_OG + Ro_IC)

November_Final <- November_Final[-c(7:21)]

write_csv=write.csv(November_Final,"D:/chrurn data/November.csv")

#### MERGING FIVE MONTHS

## reading cleaned September and july files
m1=fread("Sep_fin.csv")
m4=fread("Jul_fin.csv")

sep_jul=merge(m1,m4,by='id',all=TRUE)

write.csv(sep_jul,file='sep_july.csv')

#Read the October CSV Data file
October <-fread("October.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
October = as.data.frame(October)
October$V1 <- NULL

#Read the August CSV Data file
August <-fread("August.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
August = as.data.frame(August)
August$V1 <- NULL

Oct_Aug = merge(October,August , by = "id",all=TRUE)

#Read the November CSV Data file
November <-fread("November.csv", header = TRUE,na.strings = c("NA"), stringsAsFactors = FALSE)
November = as.data.frame(November)
November$V1 <- NULL

Oct_Aug_Nov = merge(Oct_Aug,November , by = "id",all=TRUE)

Oc_Au_No = write.csv(Oct_Aug_Nov,"D:/chrurn data/Oc_Au_No.csv")

#reading the merged files
m2=fread("sep_july.csv")
m3=fread("Oc_Au_No.csv")

m2$V1=NULL
m3$V1=NULL

#date format
m3$Acct_actv.x=as.Date(m3$Acct_actv.x, "%d%b%Y")
m3$Acct_actv.y=as.Date(m3$Acct_actv.y, "%d%b%Y")
m3$Acct_actv=as.Date(m3$Acct_actv, "%d%b%Y")

five_month=merge(m2,m3,by='id',all=TRUE)
five_month$V1=NULL
write.csv(five_month,file='five_month.csv')

### CLEANING THE FIVE MONTH CSV ####

## reading the Five months data file
fiv=fread("five_month.csv")
fi=as.data.frame(fiv)
fi$V1=NULL

id=fi[,1]
id=as.data.frame(id,stringsAsFactors = FALSE)


#July
act_jul=fi[,3]
act_jul=as.data.frame(act_jul,stringsAsFactors = FALSE)

#August
act_Aug=fi[,63]
act_Aug=as.data.frame(act_Aug,stringsAsFactors = FALSE)

#Cbind
act_jul_aug=cbind(act_jul,act_Aug)

#Replace the NA value in August with that of July
act_jul_aug$act_Aug[is.na(act_jul_aug$act_Aug)] <- act_jul_aug$act_jul[is.na(act_jul_aug$act_Aug)]

#Replace the NA value in July with that of August
act_jul_aug$act_jul[is.na(act_jul_aug$act_jul)] <- act_jul_aug$act_Aug[is.na(act_jul_aug$act_jul)]

#September
act_sep=fi[,23]
act_sep=as.data.frame(act_sep,stringsAsFactors = FALSE)

#Cbind
act_jul_aug_sep=cbind(act_jul_aug,act_sep)

#Replace the NA value in September with that of July and August
act_jul_aug_sep$act_sep[is.na(act_jul_aug_sep$act_sep)] <- act_jul_aug_sep$act_Aug[is.na(act_jul_aug_sep$act_sep)]

#Replace the NA value in August with that of September
act_jul_aug_sep$act_Aug[is.na(act_jul_aug_sep$act_Aug)] <- act_jul_aug_sep$act_sep[is.na(act_jul_aug_sep$act_Aug)]

#Replace the NA value in July with that of September
act_jul_aug_sep$act_jul[is.na(act_jul_aug_sep$act_jul)] <- act_jul_aug_sep$act_sep[is.na(act_jul_aug_sep$act_jul)]

#October
act_oct=fi[,43]
act_oct=as.data.frame(act_oct,stringsAsFactors = FALSE)



#Cbind
act_JAS_Oct=cbind(act_jul_aug_sep,act_oct)

#Replace the NA value in October with that of JAS
act_JAS_Oct$act_oct[is.na(act_JAS_Oct$act_oct)] <- act_JAS_Oct$act_jul[is.na(act_JAS_Oct$act_oct)]

#Replace the NA value in September with that of October
act_JAS_Oct$act_sep[is.na(act_JAS_Oct$act_sep)] <- act_JAS_Oct$act_oct[is.na(act_JAS_Oct$act_sep)]

#Replace the NA value in August with that of October
act_JAS_Oct$act_Aug[is.na(act_JAS_Oct$act_Aug)] <- act_JAS_Oct$act_oct[is.na(act_JAS_Oct$act_Aug)]

#Replace the NA value in July with that of October
act_JAS_Oct$act_jul[is.na(act_JAS_Oct$act_jul)] <- act_JAS_Oct$act_oct[is.na(act_JAS_Oct$act_jul)]

#November
act_Nov=fi[,83]
act_Nov=as.data.frame(act_Nov,stringsAsFactors = FALSE)

#Cbind
act_JASO_Nov=cbind(act_JAS_Oct,act_Nov)

#Replace the NA value in November with that of JASO
act_JASO_Nov$act_Nov[is.na(act_JASO_Nov$act_Nov)] <- act_JASO_Nov$act_jul[is.na(act_JASO_Nov$act_Nov)]

#Replace the NA value in October with that of November
act_JASO_Nov$act_oct[is.na(act_JASO_Nov$act_oct)] <- act_JASO_Nov$act_Nov[is.na(act_JASO_Nov$act_oct)]

#Replace the NA value in September with that of November
act_JASO_Nov$act_sep[is.na(act_JASO_Nov$act_sep)] <- act_JASO_Nov$act_Nov[is.na(act_JASO_Nov$act_sep)]

#Replace the NA value in August with that of November
act_JASO_Nov$act_Aug[is.na(act_JASO_Nov$act_Aug)] <- act_JASO_Nov$act_Nov[is.na(act_JASO_Nov$act_Aug)]

#Replace the NA value in July with that of November
act_JASO_Nov$act_jul[is.na(act_JASO_Nov$act_jul)] <- act_JASO_Nov$act_Nov[is.na(act_JASO_Nov$act_jul)]

#Cross-check for NA's
sapply(act_JASO_Nov, function(x) sum(is.na(x)))


#Removing the duplicate columns
fi$Cust_Value_Seg.x.x <- NULL
fi$Acct_actv.x.x <- NULL
fi$Cust_Value_Seg.y.x <- NULL
fi$Acct_actv.y.x <- NULL
fi$Cust_Value_Seg.x.y <- NULL
fi$Acct_actv.x.y <- NULL
fi$Cust_Value_Seg.y.y <- NULL
fi$Acct_actv.y.y <- NULL
fi$Cust_Value_Seg<- NULL
fi$Acct_actv <- NULL
fi$id <- NULL


#Outlier Removal for numeric data's

remove_outliers <- function(x) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = TRUE)
  H <- 1.5 * IQR(x, na.rm = TRUE)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

Rm_Out <- as.data.frame(apply(fi[,1:90],2, remove_outliers))

for(i in 1:ncol(Rm_Out[,1:90])){
  Rm_Out[,1:90][is.na(Rm_Out[,1:90][,i]), i]=mean(Rm_Out[,1:90][,i], na.rm = TRUE)
}

sapply(Rm_Out, function(x) sum(is.na(x)))


five_month = cbind(id$id,act_JASO_Nov$act_jul,act_JASO_Nov$act_Aug,act_JASO_Nov$act_sep,act_JASO_Nov$act_oct,act_JASO_Nov$act_Nov,Rm_Out)

colnames(five_month)[1] <- 'id'
colnames(five_month)[2] <- 'act_jul'
colnames(five_month)[3] <- 'act_aug'
colnames(five_month)[4] <- 'act_sept'
colnames(five_month)[5] <- 'act_oct'
colnames(five_month)[6] <- 'act_nov'

five_month=write.csv(five_month,file='five_month_new.csv')


############ merging five months csv with disconnection file ###########

#reading the csv file
five_month=fread("five_month_new.csv")
five_month=as.data.frame(five_month)
five_month$V1=NULL

#reading the disconnection file
discon=fread("Discon1.csv")
#renaming the column name
setnames(discon,'ID','id')
discon$Date2=as.Date(discon$Date2, "%d%b%Y")
discon=as.data.frame(discon)


fin=merge(five_month,discon,by='id',all=TRUE)

#date format
fin$Date2[is.na(fin$Date2)]="2015-11-30"

#inorder to check NA's
sapply(fin, function(x) sum(is.na(x)))

fin=na.omit(fin)

#deriving a target variable
fin$churn_status=ifelse(fin$Date2=="2015-11-30",0,1) 

#deriving a new varibale
No_of_days <- as.Date(as.character(fin$Date2), format="%Y-%m-%d",origin = "1899-12-30")-as.Date(as.character(fin$act_jul), format="%Y-%m-%d",origin = "1899-12-30")
No_of_days

final=cbind(fin,No_of_days)
View(final)

write.csv(final,file='final.csv')

####### MODEL ######

#### reading the csv file on server using hadoop ####
fin <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text  /tmp/anaghaproject/final2.csv"))
fin$V1<-NULL

fin$churn_status=as.factor(fin$churn_status)
str(fin)
#date format
fin$act_jul=as.Date(fin$act_jul, "%Y-%m-%d")
fin$Date2=as.Date(fin$Date2, "%Y-%m-%d")
#check multicollinearity
cor(fin)
fin$churn_status=factor(fin$churn_status)
str(fin)
fin$V1=NULL
fin$id<-NULL
fin$Ratio_IC.x.x<-NULL
fin$Ratio_IC.x.y<-NULL
fin$Ratio_IC.y.y<-NULL
fin$Ratio_IC.y.x<-NULL
fin$Ratio_IC<-NULL
#fin$No_of_days_complaint=NULL
fin$No_of_days=NULL
fin$re1=NULL


##### GLM ###

## subsetting the classes ##
class1=subset(fin,churn_status=="1")
class0=subset(fin,churn_status=="0")

#inorder to have the same sample
set.seed(5)

s1=sample(nrow(class1))
s0=sample(nrow(class0))

## taking 70% for train and 30% for test ##
train1=class1[s1[1:round(nrow(class1)*0.70)],]
test1=class1[s1[(round(nrow(class1)*0.70)+1):nrow(class1)],]

train0=class0[s0[1:round(nrow(class0)*0.70)],]
test0=class0[s0[(round(nrow(class0)*0.70)+1):nrow(class0)],]

train=rbind(train1,train0)
test=rbind(test1,test0)

##### Logistic Regression ####
fit=glm(churn_status ~., binomial(link="logit"),train)
summary(fit)
#step(fit)

### checking the collineariy again
alias(fit)

tt = test
tt$churn_status = NULL

output=predict(fit,tt,type = "response")

outputnew=ifelse(output>=0.5,1,0)

#### ROC curve ####
roc=roc(churn_status~outputnew,test)
plot(roc)

##### Accuracy #####
confusionMatrix(outputnew,test$churn_status,positive = "1")
