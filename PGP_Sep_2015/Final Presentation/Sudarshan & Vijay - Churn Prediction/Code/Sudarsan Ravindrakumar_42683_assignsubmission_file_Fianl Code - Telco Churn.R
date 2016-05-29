library(data.table)
library(dplyr)

####### Reading the CSVs of JULy and changing the "ID" name of "July_usg1"####

July_data1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_data1.csv"))
July_usg1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Jul_usg1.csv"))
colnames(July_usg1)[1] <- "id"
July_pay1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_pay1.csv"))

July_data_usg = merge(July_data1, July_usg1, by = "id", all = TRUE)
July_dup = merge(July_data_usg, July_pay1, by = "id", all = TRUE, allow.cartesian=TRUE)

###### Deleting the irrelevant columns###
July_del<- July_dup %>% select(-c(cust_value_seg:acct_actv1)) 

#### Deleting the rows which has All NAs ####
July_dup <- July_del[rowSums(is.na(July_del)) <= 25,]

###### Taking the Unique IDs ####
July_dup = unique(July_dup, by = "id")


##### Function for Outlier replacement with .5 and .95 percentile of data ####
replace_outliers <- function(x){
  quantiles <- quantile( x, c(.05, .95 ), na.rm = TRUE)
  x[ x <= quantiles[1] ] <- quantiles[1]
  x[ x >= quantiles[2] ] <- quantiles[2]
  x
}

#### Seperating the data as Numerical, Categorical and Characters for easy replament of Outliers
### and NA Values###
July_dup<-as.data.frame(July_dup)
July_dup11 <- as.data.frame(July_dup[,-c(1,2,3,22)])
July_new1 <- as.data.frame(apply(July_dup11, 2, replace_outliers))
July_new0 <- as.data.frame(July_dup[,c(1,2,3,22)])

July_witNA <- as.data.frame(cbind(July_new0,July_new1))

###### Function for replacing NAs with Median in numerical columns ###
Median_for_numeric=function(x){
  x<-as.numeric(as.character(x))
  x[is.na(x)] =median(x, na.rm=TRUE)
  x
}

#### Replacing NAs with Median in Numerical column ###
July_replace_num_colNA=as.data.frame(apply(July_new1,2,Median_for_numeric))

###### Function for replacement of NA with mode in Categorical columns ###
Mode <- function (x, na.rm) {
  pin <- table(x)
  m <- names(which(pin == max(pin)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

replace_mode <- function(x){
  x[is.na(x)]<- Mode(x, na.rm = TRUE)
  x
}


July_Mode_col = as.data.frame(July_witNA[,c(2,3)])
July_id_acctdate = as.data.frame(July_witNA[,c(1,4)])

#### Mode replacement in categorical columns ####
July_Mode_col_replace <- as.data.frame(apply(July_Mode_col, 2, replace_mode))

##### Below is the dataset without any outliers or NAs ####
July_processed <- as.data.frame(cbind(July_id_acctdate,July_Mode_col_replace,July_replace_num_colNA))

###### Removing the columns With was not summing up properly When added like total minutes, etc., ###
July_processed <- as.data.frame(July_processed[,-c(11,13,18,22)])

#### Creating new Variables to out data set ###3
July_processed <- July_processed %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_MOU_OGIC = TOT_OG+TOT_IC)

#### Replacing the NA values of Date with the first date of the particular Month ##
library(lubridate)
July_mobAcct <-format(dmy(July_processed$mob_actv1),"%Y-%m-%d")
Df_July = as.data.frame(July_mobAcct,stringsAsFactors = FALSE)
Df_July$July_mobAcct[is.na(Df_July$July_mobAcct)] <- "2014-07-01"

July_processed$mob_actv1 <- Df_July$July_mobAcct


#######   AUGUST    #####

Aug_data1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_data1.csv"))
Aug_usg1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_usg1.csv"))
colnames(Aug_usg1)[1] <- "id"
Aug_pay1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_pay1.csv"))

Aug_data_usg = merge(Aug_data1, Aug_usg1, by = "id", all = TRUE)
Aug_dup = merge(Aug_data_usg, Aug_pay1, by = "id", all = TRUE, allow.cartesian=TRUE)

Aug_del<- Aug_dup %>% select(-c(cust_value_seg:acct_actv1)) 

Aug_dup <- Aug_del[rowSums(is.na(Aug_del)) <= 25,]

Aug_dup = unique(Aug_dup, by = "id")


Aug_dup<-as.data.frame(Aug_dup)
Aug_dup11 <- as.data.frame(Aug_dup[,-c(1,2,3,22)])
Aug_new1 <- as.data.frame(apply(Aug_dup11, 2, replace_outliers))
Aug_new0 <- as.data.frame(Aug_dup[,c(1,2,3,22)])

Aug_witNA <- as.data.frame(cbind(Aug_new0,Aug_new1))

Median_for_numeric=function(x){
  x<-as.numeric(as.character(x))
  x[is.na(x)] =median(x, na.rm=TRUE)
  x
}
Aug_replace_num_colNA=as.data.frame(apply(Aug_new1,2,Median_for_numeric))


Mode <- function (x, na.rm) {
  pin <- table(x)
  m <- names(which(pin == max(pin)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

replace_mode <- function(x){
  x[is.na(x)]<- Mode(x, na.rm = TRUE)
  x
}

Aug_Mode_col = as.data.frame(Aug_witNA[,c(2,3)])
Aug_id_acctdate = as.data.frame(Aug_witNA[,c(1,4)])

Aug_Mode_col_replace <- as.data.frame(apply(Aug_Mode_col, 2, replace_mode))

Aug_processed <- as.data.frame(cbind(Aug_id_acctdate,Aug_Mode_col_replace,Aug_replace_num_colNA))

Aug_processed <- as.data.frame(Aug_processed[,-c(11,13,18,22)])

Aug_processed <- Aug_processed %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_MOU_OGIC = TOT_OG+TOT_IC)

Aug_mobAcct <-format(dmy(Aug_processed$mob_actv1),"%Y-%m-%d")
Df_Aug = as.data.frame(Aug_mobAcct,stringsAsFactors = FALSE)

Df_Aug$Aug_mobAcct[is.na(Df_Aug$Aug_mobAcct)] <- "2014-08-01"

Aug_processed$mob_actv1 <- Df_Aug$Aug_mobAcct

########################################################################
nrow(July_processed)
nrow(Aug_processed)
sapply(July_processed, function(x) sum(is.na(x)))
sapply(Aug_processed, function(x) sum(is.na(x)))
########################################################################
############### COMBINING JULY AND AUGUST ##############

July_Aug <- merge(July_processed, Aug_processed, by = "id", all = TRUE)

#### If acct.date of the perivious month has NAs.. then it will be replaced by the particular
## cell value of the next month 
July_Aug$mob_actv1.x[is.na(July_Aug$mob_actv1.x)] <- July_Aug$mob_actv1.y[is.na(July_Aug$mob_actv1.x)]

### in this process we have tow acct dates and the Aug months date column will be deleted ###
July_Aug <- as.data.frame(July_Aug[,-c(32)])
names(July_Aug)

sapply(July_Aug, function(x) sum(is.na(x)))

########################################################################

###############  SEPTEMBER   #####################

#######   September    #####

Sep_data1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_data1.csv"))
Sep_usg1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_usg1.csv"))
colnames(Sep_usg1)[1] <- "id"
Sep_pay1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_pay1.csv"))

Sep_data_usg = merge(Sep_data1, Sep_usg1, by = "id", all = TRUE)
Sep_dup = merge(Sep_data_usg, Sep_pay1, by = "id", all = TRUE, allow.cartesian=TRUE)

Sep_del<- Sep_dup %>% select(-c(cust_value_seg:acct_actv1)) 

Sep_dup <- Sep_del[rowSums(is.na(Sep_del)) <= 25,]

Sep_dup = unique(Sep_dup, by = "id")


Sep_dup<-as.data.frame(Sep_dup)
Sep_dup11 <- as.data.frame(Sep_dup[,-c(1,2,3,22)])
Sep_new1 <- as.data.frame(apply(Sep_dup11, 2, replace_outliers))
Sep_new0 <- as.data.frame(Sep_dup[,c(1,2,3,22)])

Sep_witNA <- as.data.frame(cbind(Sep_new0,Sep_new1))

Sep_replace_num_colNA=as.data.frame(apply(Sep_new1,2,Median_for_numeric))

Sep_Mode_col = as.data.frame(Sep_witNA[,c(2,3)])
Sep_id_acctdate = as.data.frame(Sep_witNA[,c(1,4)])

Sep_Mode_col_replace <- as.data.frame(apply(Sep_Mode_col, 2, replace_mode))

Sep_processed <- as.data.frame(cbind(Sep_id_acctdate,Sep_Mode_col_replace,Sep_replace_num_colNA))

Sep_processed <- as.data.frame(Sep_processed[,-c(11,13,18,22)])

Sep_processed <- Sep_processed %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_MOU_OGIC = TOT_OG+TOT_IC)

Sep_mobAcct <-format(dmy(Sep_processed$mob_actv1),"%Y-%m-%d")
Df_Sep = as.data.frame(Sep_mobAcct,stringsAsFactors = FALSE)

Df_Sep$Sep_mobAcct[is.na(Df_Sep$Sep_mobAcct)] <- "2014-09-01"

Sep_processed$mob_actv1 <- Df_Sep$Sep_mobAcct

sapply(Sep_processed, function(x) sum(is.na(x)))



#######################################################################3
############### COMBINING JULY_August AND September ##############

July_Aug_Sep <- merge(July_Aug, Sep_processed, by = "id", all = TRUE)

July_Aug_Sep$mob_actv1.x[is.na(July_Aug_Sep$mob_actv1.x)] <- July_Aug_Sep$mob_actv1[is.na(July_Aug_Sep$mob_actv1.x)]

sapply(July_Aug_Sep, function(x) sum(is.na(x)))

July_Aug_Sep <- as.data.frame(July_Aug_Sep[,-c(61)])
names(July_Aug_Sep)

#####################################################################

write.csv(July_Aug_Sep, file = "July_Aug_Sep1.csv")

hdfs.put("July_Aug_Sep1.csv","/tmp/sudarsan_results/")

July_Aug_Sep <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/sudarsan_results/July_Aug_Sep1.csv"))
July_Aug_Sep <- as.data.frame(July_Aug_Sep)
July_Aug_Sep <- as.data.frame(July_Aug_Sep[,-c(1)])
########################################################################

##############################################################################
#############################  OCTOBER  #################################

#######   October    #####

Oct_data1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_data1.csv"))
Oct_usg1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_usg1.csv"))
colnames(Oct_usg1)[1] <- "id"
Oct_pay1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_pay1.csv"))

Oct_data_usg = merge(Oct_data1, Oct_usg1, by = "id", all = TRUE)
Oct_dup = merge(Oct_data_usg, Oct_pay1, by = "id", all = TRUE, allow.cartesian=TRUE)

Oct_del<- Oct_dup %>% select(-c(cust_value_seg:acct_actv1)) 

Oct_dup <- Oct_del[rowSums(is.na(Oct_del)) <= 25,]

Oct_dup = unique(Oct_dup, by = "id")


Oct_dup<-as.data.frame(Oct_dup)
Oct_dup11 <- as.data.frame(Oct_dup[,-c(1,2,3,22)])
Oct_new1 <- as.data.frame(apply(Oct_dup11, 2, replace_outliers))
Oct_new0 <- as.data.frame(Oct_dup[,c(1,2,3,22)])

Oct_witNA <- as.data.frame(cbind(Oct_new0,Oct_new1))

Oct_replace_num_colNA=as.data.frame(apply(Oct_new1,2,Median_for_numeric))

Oct_Mode_col = as.data.frame(Oct_witNA[,c(2,3)])
Oct_id_acctdate = as.data.frame(Oct_witNA[,c(1,4)])

Oct_Mode_col_replace <- as.data.frame(apply(Oct_Mode_col, 2, replace_mode))

Oct_processed <- as.data.frame(cbind(Oct_id_acctdate,Oct_Mode_col_replace,Oct_replace_num_colNA))

Oct_processed <- as.data.frame(Oct_processed[,-c(11,13,18,22)])

Oct_processed <- Oct_processed %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_MOU_OGIC = TOT_OG+TOT_IC)

Oct_mobAcct <-format(dmy(Oct_processed$mob_actv1),"%Y-%m-%d")
Df_Oct = as.data.frame(Oct_mobAcct,stringsAsFactors = FALSE)

Df_Oct$Oct_mobAcct[is.na(Df_Oct$Oct_mobAcct)] <- "2014-10-01"

Oct_processed$mob_actv1 <- Df_Oct$Oct_mobAcct

sapply(Oct_processed, function(x) sum(is.na(x)))

#####################################################################

############### COMBINING JULY AUG SEP WITH OCT ####################

July_Aug_Sep_Oct <- merge(July_Aug_Sep, Oct_processed, by = "id", all = TRUE)

July_Aug_Sep_Oct$mob_actv1.x[is.na(July_Aug_Sep_Oct$mob_actv1.x)] <- July_Aug_Sep_Oct$mob_actv1[is.na(July_Aug_Sep_Oct$mob_actv1.x)]

sapply(July_Aug_Sep_Oct, function(x) sum(is.na(x)))

July_Aug_Sep_Oct <- as.data.frame(July_Aug_Sep_Oct[,-c(90)])
names(July_Aug_Sep_Oct)

#######################################################################
####################### NOVEMBER ##################################

#######   November    #####
Nov_data1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_data1.csv"))
Nov_usg1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_usg1.csv"))
colnames(Nov_usg1)[1] <- "id"
Nov_pay1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_pay1.csv"))

Nov_data_usg = merge(Nov_data1, Nov_usg1, by = "id", all = TRUE)
Nov_dup = merge(Nov_data_usg, Nov_pay1, by = "id", all = TRUE, allow.cartesian=TRUE)

Nov_del<- Nov_dup %>% select(-c(cust_value_seg:acct_actv1)) 

Nov_dup <- Nov_del[rowSums(is.na(Nov_del)) <= 25,]

Nov_dup = unique(Nov_dup, by = "id")


Nov_dup<-as.data.frame(Nov_dup)
Nov_dup11 <- as.data.frame(Nov_dup[,-c(1,2,3,22)])
Nov_new1 <- as.data.frame(apply(Nov_dup11, 2, replace_outliers))
Nov_new0 <- as.data.frame(Nov_dup[,c(1,2,3,22)])

Nov_witNA <- as.data.frame(cbind(Nov_new0,Nov_new1))

Nov_replace_num_colNA=as.data.frame(apply(Nov_new1,2,Median_for_numeric))

Nov_Mode_col = as.data.frame(Nov_witNA[,c(2,3)])
Nov_id_acctdate = as.data.frame(Nov_witNA[,c(1,4)])

Nov_Mode_col_replace <- as.data.frame(apply(Nov_Mode_col, 2, replace_mode))

Nov_processed <- as.data.frame(cbind(Nov_id_acctdate,Nov_Mode_col_replace,Nov_replace_num_colNA))

Nov_processed <- as.data.frame(Nov_processed[,-c(11,13,18,22)])

Nov_processed <- Nov_processed %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_MOU_OGIC = TOT_OG+TOT_IC)

Nov_mobAcct <-format(dmy(Nov_processed$mob_actv1),"%Y-%m-%d")
Df_Nov = as.data.frame(Nov_mobAcct,stringsAsFactors = FALSE)

Df_Nov$Nov_mobAcct[is.na(Df_Nov$Nov_mobAcct)] <- "2014-11-01"

Nov_processed$mob_actv1 <- Df_Nov$Nov_mobAcct

sapply(Nov_processed, function(x) sum(is.na(x)))

##########################################################

###### COMBINING July Aug Sep Oct and Nov #########

July_Aug_Sep_Oct_Nov <- merge(July_Aug_Sep_Oct, Nov_processed, by = "id", all = TRUE)

July_Aug_Sep_Oct_Nov$mob_actv1.x[is.na(July_Aug_Sep_Oct_Nov$mob_actv1.x)] <- July_Aug_Sep_Oct_Nov$mob_actv1[is.na(July_Aug_Sep_Oct_Nov$mob_actv1.x)]

sapply(July_Aug_Sep_Oct_Nov, function(x) sum(is.na(x)))

July_Aug_Sep_Oct_Nov <- as.data.frame(July_Aug_Sep_Oct_Nov[,-c(119)])
names(July_Aug_Sep_Oct_Nov)
################################################################################
########################## WRITING THE FILE % MONTHS ############################

write.csv(July_Aug_Sep_Oct_Nov, file = "July_Aug_Sep_Oct_Nov1.csv")

### PUTTING IT TO HDFS #####

hdfs.put("July_Aug_Sep_Oct_Nov1.csv","/tmp/sudarsan_results/")

###########################
################# Open The 5 month Data  ###########

July_Aug_Sep_Oct_Nov<- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/sudarsan_results/July_Aug_Sep_Oct_Nov1.csv"), na.strings = c("NA", ""))

July_Aug_Sep_Oct_Nov <- as.data.frame(July_Aug_Sep_Oct_Nov)

July_Aug_Sep_Oct_Nov <- as.data.frame(July_Aug_Sep_Oct_Nov[,c(-1)])

names(July_Aug_Sep_Oct_Nov)
##July_Aug_Sep_Oct_Nov2 <- as.data.frame(July_Aug_Sep_Oct_Nov[,-c(1,2)])
##July_Aug_Sep_Oct_Nov <- as.data.frame(July_Aug_Sep_Oct_Nov[,c(-1)])


##July_Aug_Sep_Oct_Nov[as.logical((rowSums(is.na(July_Aug_Sep_Oct_Nov))-144)),]


July_Aug_Sep_Oct_Nov11 <- July_Aug_Sep_Oct_Nov[rowSums(is.na(July_Aug_Sep_Oct_Nov)) <= 144,]
##dim(July_Aug_Sep_Oct_Nov1)

July_Aug_Sep_Oct_Nov = unique(July_Aug_Sep_Oct_Nov, by = "id")

##July_Aug_Sep_Oct_Nov1 <- as.data.frame(July_Aug_Sep_Oct_Nov)

July_Aug_Sep_Oct_Nov[is.na(July_Aug_Sep_Oct_Nov)] <- 0
sapply(July_Aug_Sep_Oct_Nov, function(x) sum(is.na(x)))

############################################################

############## READING DISCON DATA #####################

Discon1 <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Discon1.csv"))
Discon1 <- as.data.frame(Discon1)
Discon1$Churn <- 1
colnames(Discon1)[1] <- "id"

JASON_Dis<- merge(July_Aug_Sep_Oct_Nov, Discon1, by = "id", all = TRUE)
sapply(JASON_Dis, function(x) sum(is.na(x)))
JASON_Dis$Churn[is.na(JASON_Dis$Churn)] <- 0

################## Dont Use The below code.. jus fa reference ###
Nov_mobAcct <-format(dmy(Nov_processed$mob_actv1),"%Y-%m-%d")
Df_Nov = as.data.frame(Nov_mobAcct,stringsAsFactors = FALSE)

Df_Nov$Nov_mobAcct[is.na(Df_Nov$Nov_mobAcct)] <- "2014-11-01"

Nov_processed$mob_actv1 <- Df_Nov$Nov_mobAcct

##########################

library(lubridate)
Dis_Date <-format(dmy(JASON_Dis$Date2),"%Y-%m-%d")
Df_Dis = as.data.frame(Dis_Date,stringsAsFactors = FALSE)

Df_Dis$Dis_Date[is.na(Df_Dis$Dis_Date)] <- "2014-11-30"

JASON_Dis$Date2 <- Df_Dis$Dis_Date


#### #####
NO_NA = na.omit(JASON_Dis)

write.csv(NO_NA, file = "NO_NA1.csv")

### PUTTING IT TO HDFS #####

hdfs.put("NO_NA1.csv","/tmp/sudarsan_results/")

#################

NO_NA<- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/sudarsan_results/NO_NA1.csv"),stringsAsFactors = FALSE, na.strings = c("NA", ""))

NO_NA <- as.data.frame(NO_NA)

NO_NA <- as.data.frame(NO_NA[,c(-1)])

NO_NA$Date2 <- as.Date(NO_NA$Date2)
NO_NA$mob_actv1.x <- as.Date(NO_NA$mob_actv1.x)
NO_NA$No_of_days <- as.Date(as.character(NO_NA$Date2), format="%Y-%m-%d",origin = "1899-12-30")-as.Date(as.character(NO_NA$mob_actv1.x), format="%Y-%m-%d",origin = "1899-12-30")

write.csv(NO_NA, file = "final_data.csv")

hdfs.put("final_data.csv","/tmp/sudarsan_results/")


###################################################################################
###################################################################################
##########################MACHINE LEARNING ###########################################


library(data.table)
library(dplyr)

df <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/sudarsan_results/final_data.csv"), stringsAsFactors = TRUE)
df <- as.data.frame(df)
names(df)
str(df)
aa <- as.data.frame(df[151])

##### Deleting the Date Column and the variables which has a factor level of 1 ####
df <- as.data.frame(df[,-c(1,3,23,24,42,51,81,82,110,111,139,140,149,150)])
str(df)
df <- as.data.frame(df[,-c(46,47)])
colnames(df)[136] <- "Churn"

##################################################
######## DELETING ALL VARIABLES CAUSING PROBLEM LIKE HAVING ONLY 1 LEVEL OF FACTOR ####
#### AND WRITED IT AS CSV ########
write.csv(df, file = "final.csv")
hdfs.put("final.csv","/tmp/sudarsan_results/")

####### Reading the final data #####

df <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/sudarsan_results/final.csv"), stringsAsFactors = TRUE)
df <- as.data.frame(df)
df <- as.data.frame(df[-1])
sapply(df, function(x) sum(is.na(x)))

##### Changing int columns to num columns in df ###
for (i in 1:ncol(df)) {
  if (class(df[,i])=="integer") {
    df[is.na(df[,i]),i] <- as.numeric(df[,i])
  } 
}


str(df)

class1 <- subset(df,Churn=="1")
nrow(class1)

class0 <- subset(df,Churn=="0")
nrow(class0)

library(car)
library(caret)
set.seed(123)
s1 <- sample(nrow(class1))
set.seed(123)
s0 <- sample(nrow(class0))

train1 <- class1[(1:round(nrow(class1)*0.7)),]
test1 <- class1[((round(nrow(class1)*0.7)+1):nrow(class1)),]

train0 <- class0[(1:round(nrow(class0)*0.7)),]
test0 <- class0[((round(nrow(class0)*0.7)+1):nrow(class0)),]

train <- rbind(train1,train0)
test <- rbind(test1, test0)

train_new <- train[-1]
test_new <- test[,-c(1,136)]

train_new$Churn <- as.factor(train_new$Churn)

class(train_new$Churn)

fit <- glm(Churn~., family = binomial(link = "logit"), data = train_new)
summary(fit)
pred <- predict(fit, test_new, type = "response")
test$final <- ifelse(pred >= 0.5, 1, 0)
library(caret)
install.packages("e1071")
library(e1071)
confusionMatrix(test$final, test[,136])

install.packages("pROC")
library(pROC)

g <- roc(as.numeric(test[,136]) ~ as.numeric(test$final), test)
plot(g)
auc(g)*100
