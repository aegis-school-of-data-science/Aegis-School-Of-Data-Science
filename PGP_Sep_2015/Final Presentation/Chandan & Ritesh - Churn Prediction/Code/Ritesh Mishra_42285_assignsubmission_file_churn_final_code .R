getwd()
unlist(".RData")
library(data.table)
library(dplyr)
library(lubridate)
library(car)  ## glm
library(caret)  ## confusion matrix
#rm(list = ls()) ## For spring cleaning

########## For parallel processing of CPU###############
library(parallel)
library(foreach)
library(doParallel)
registerDoParallel(cores = 2)
cl <- makeCluster(2)
registerDoParallel(cl)

##########FETCHING JULY DATA FROM HADOOP ###############

#Jul_data <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_data1.csv"))
#Jul_usg <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Jul_usg1.csv"))
#Jul_pay <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_pay1.csv"))

Jul_data = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/July_data1.csv")
Jul_usg = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/July_usg1.csv")
Jul_pay = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/July_pay1.csv")

colnames(Jul_usg)[1] <- "id"      ### CHANGING THE COL NAME FROM "ID" to "id"

Jul_data_usg = merge(Jul_data, Jul_usg, by = "id", all = TRUE)   ### MERGING JULY DATA and UsAGE

Jul_merge = merge(Jul_data_usg, Jul_pay, by = "id", all = TRUE, allow.cartesian=TRUE) ## MERGING DATA USAGE with PAY

Jul_del<- Jul_merge %>% select(-c(cust_value_seg:acct_actv1)) ### REMOVING THESE COLS 

Jul_WNA <- Jul_del[rowSums(is.na(Jul_del)) <= 25,]    ### REMOVING ROWS WHERE ALL THE VALUES ARE NA EXCEPT id column

Jul_WDR <- unique(Jul_WNA, by = "id") #### REMOVING THE DUPLICATE RECORDS
Jul_WDR <- as.data.frame(Jul_WDR)

Jul_temp <- as.data.frame(Jul_WDR[,-c(1,2,3,22)])  ###### DROPPING THE COLUMNS WITH CATEGORICAL DATA

##### FUNCTION TO REMOVE OUTLIERS ############
remove_outliers <- function(x) {
  quant <- quantile(as.numeric(x), probs=c(.25, .75), na.rm = TRUE)
   outl<- 1.5 * IQR(x, na.rm = TRUE)
  ss <- x
  ss[x < (quant[1] - outl)] <- NA
  ss[x > (quant[2] + outl)] <- NA
  ss
}

Jul_1 <- as.data.frame(apply(Jul_temp, 2, remove_outliers))
Jul_2 <- as.data.frame(Jul_WDR[,c(1,2,3,22)])

Jul_NA <- as.data.frame(cbind(Jul_2,Jul_1)) 
str(Jul_NA)

######## REPLACING NAs WITH MEDIAN,MEAN and MODE FOR NUMERICAL DATA, INTEGER DATA and CATEGORICAL DATA  RESPECTIVELY###################

Modalvalue <- function (x, na.rm) {
  pp <- table(x)
  m <- names(which(pp == max(pp)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

###### TRAVERSING EACH COLUMN AND REPLCING NA WITH APPROPRIATE STATISTICAL MEASURES ##############

for (i in 1:ncol(Jul_NA)) {
  if (class(Jul_NA[,i])=="numeric") {
    Jul_NA[is.na(Jul_NA[,i]),i] <- median(Jul_NA[,i], na.rm = TRUE)
  } else if (class(Jul_NA[,i])=="integer") {
    Jul_NA[is.na(Jul_NA[,i]),i] <- mean(Jul_NA[,i], na.rm = TRUE)
  } else if (class(Jul_NA[,i]) %in% c("character", "factor")) {
    Jul_NA[is.na(Jul_NA[,i]),i] <- Modalvalue(Jul_NA[,i], na.rm = TRUE)
  }
}

sapply(Jul_NA, function(x) sum(is.na(x)))

######## MUTATING NEW VARIABLES #############

Jul_newdata <- Jul_NA %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(IC_PROP = (TOT_IC)/(TOT_IC+TOT_OG)) %>%
  mutate(OG_PROP = (TOT_OG)/(TOT_IC+TOT_OG)) %>%
  mutate(vol2g_PROP = (vol2g)/(vol2g+vol3g)) %>%
  mutate(vol3g_PROP = (vol3g)/(vol2g+vol3g)) %>%
  mutate(TOT_MOU = TOT_OG+TOT_IC)%>%
  mutate(VOICE_PROP = (TOT_MOU)/((TOT_MOU)+(vol2g+vol3g))) %>%
  mutate(DATA_PROP = (vol2g+vol3g)/((TOT_MOU)+(vol2g+vol3g)))

dim(Jul_newdata)
sapply(Jul_newdata, function(x) sum(is.na(x))) ###### THERE WERE SOME NaN's ########


#### REMOVING NaN's ###########

for (i in 1:ncol(Jul_newdata)){
Jul_newdata[is.nan(Jul_newdata[,i]),i]<- 0
}

###### CHANGING THE DATE FORMAT and IMPUTING THE NA WITH THE FIRST DATE OF JULY############    

Jul_mobAcct <-format(dmy(Jul_newdata$mob_actv1),"%Y-%m-%d")
Df_Jul = as.data.frame(Jul_mobAcct,stringsAsFactors = FALSE)
Df_Jul$Jul_mobAcct[is.na(Df_Jul$Jul_mobAcct)] <- "2014-07-01"
Jul_newdata$mob_actv1 <- Df_Jul$Jul_mobAcct

sapply(Jul_newdata, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(Jul_newdata, file = "Jul_newdata.rda") ## .rda used to decrease the size of the csv   ######

#write.csv(Jul_newdata,"/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Final_Data/Final/Jul_newdata.csv")

########## FETCHING AUGUST DATA FROM HADOOP #############################################

#Aug_data <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_data1.csv"))
#Aug_usg <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_usg1.csv"))
#Aug_pay <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_pay1.csv"))

Aug_data <- fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Aug_data1.csv")
Aug_usg <- fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Aug_usg1.csv")
Aug_pay <- fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Aug_pay1.csv")

colnames(Aug_usg)[1] <- "id"      ### CHANGING THE COL NAME FROM "ID" to "id"

Aug_data_usg = merge(Aug_data, Aug_usg, by = "id", all = TRUE)   ### MERGING JULY DATA and UsAGE

Aug_merge = merge(Aug_data_usg, Aug_pay, by = "id", all = TRUE, allow.cartesian=TRUE) ## MERGING DATA USAGE with PAY

Aug_del<- Aug_merge %>% select(-c(cust_value_seg:acct_actv1)) ### REMOVING THESE COLS 

Aug_WNA <- Aug_del[rowSums(is.na(Aug_del)) <= 25,]    ### REMOVING ROWS WHERE ALL THE VALUES ARE NA EXCEPT id column

Aug_WDR <- unique(Aug_WNA, by = "id") #### REMOVING THE DUPLICATE RECORDS
Aug_WDR <- as.data.frame(Aug_WDR)
Aug_temp <- as.data.frame(Aug_WDR[,-c(1,2,3,22)])  ###### DROPPING THE COLUMNS WITH CATEGORICAL DATA

##### FUNCTION TO REMOVE OUTLIERS ############
remove_outliers <- function(x) {
  quant <- quantile(as.numeric(x), probs=c(.25, .75), na.rm = TRUE)
  outl<- 1.5 * IQR(x, na.rm = TRUE)
  ss <- x
  ss[x < (quant[1] - outl)] <- NA
  ss[x > (quant[2] + outl)] <- NA
  ss
}

Aug_1 <- as.data.frame(apply(Aug_temp, 2, remove_outliers))
Aug_2 <- as.data.frame(Aug_WDR[,c(1,2,3,22)])

Aug_NA <- as.data.frame(cbind(Aug_2,Aug_1))  

######## REPLACING NAs WITH MEDIAN,MEAN and MODE FOR NUMERICAL DATA, INTEGER DATA and CATEGORICAL DATA  RESPECTIVELY###################

Modalvalue <- function (x, na.rm) {
  pp <- table(x)
  m <- names(which(pp == max(pp)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

###### TRAVERSING EACH COLUMN AND REPLCING NA WITH APPROPRIATE STATISTICAL MEASURES ##############

for (i in 1:ncol(Aug_NA)) {
  if (class(Aug_NA[,i])=="numeric") {
    Aug_NA[is.na(Aug_NA[,i]),i] <- median(Aug_NA[,i], na.rm = TRUE)
  } else if (class(Aug_NA[,i])=="integer") {
    Aug_NA[is.na(Aug_NA[,i]),i] <- mean(Aug_NA[,i], na.rm = TRUE)
  } else if (class(Aug_NA[,i]) %in% c("character", "factor")) {
    Aug_NA[is.na(Aug_NA[,i]),i] <- Modalvalue(Aug_NA[,i], na.rm = TRUE)
  }
}

######## MUTATING NEW VARIABLES #############

Aug_newdata <- Aug_NA %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(IC_PROP = (TOT_IC)/(TOT_IC+TOT_OG)) %>%
  mutate(OG_PROP = (TOT_OG)/(TOT_IC+TOT_OG)) %>%
  mutate(vol2g_PROP = (vol2g)/(vol2g+vol3g)) %>%
  mutate(vol3g_PROP = (vol3g)/(vol2g+vol3g)) %>%
  mutate(TOT_MOU = TOT_OG+TOT_IC)%>%
  mutate(VOICE_PROP = (TOT_MOU)/((TOT_MOU)+(vol2g+vol3g))) %>%
  mutate(DATA_PROP = (vol2g+vol3g)/((TOT_MOU)+(vol2g+vol3g)))

sapply(Aug_newdata, function(x) sum(is.na(x))) ###### THERE WERE SOME NaN's ########

#### REMOVING NaN's ###########

for (i in 1:ncol(Aug_newdata)){
  Aug_newdata[is.nan(Aug_newdata[,i]),i]<- 0
}

###### CHANGING THE DATE FORMAT and IMPUTING THE NA WITH THE FIRST DATE OF AUGUST ############    

Aug_mobAcct <-format(dmy(Aug_newdata$mob_actv1),"%Y-%m-%d")
Df_Aug = as.data.frame(Aug_mobAcct,stringsAsFactors = FALSE)
Df_Aug$Aug_mobAcct[is.na(Df_Aug$Aug_mobAcct)] <- "2014-08-01"
Aug_newdata$mob_actv1 <- Df_Aug$Aug_mobAcct

sapply(Aug_newdata, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(Aug_newdata, file = "Aug_newdata.rda")

############### COMBINING JULY AND AUGUST DATA ##############

load("Jul_newdata.rda")
load("Aug_newdata.rda")

Jul_Aug <- merge(Jul_newdata, Aug_newdata, by = "id", all = TRUE)

names(Jul_Aug)

Jul_Aug$mob_actv1.x[is.na(Jul_Aug$mob_actv1.x)] <- Jul_Aug$mob_actv1.y[is.na(Jul_Aug$mob_actv1.x)]
Jul_Aug = as.data.frame(Jul_Aug)

names(Jul_Aug)
### Removed Activation date 
Jul_Aug <- as.data.frame(Jul_Aug[,-c(44)])

sapply(Jul_Aug, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new/")
save(Jul_Aug, file = "Jul_Aug.rda")

###############  FETCHING SEPTEMBER DATA  #####################

#Sep_data <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_data1.csv"))
#Sep_usg <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_usg1.csv"))
#Sep_pay <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_pay1.csv"))

Sep_data = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Sep_data1.csv")
Sep_usg = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Sep_usg1.csv")
Sep_pay = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Sep_pay1.csv")

colnames(Sep_usg)[1] <- "id"      ### CHANGING THE COL NAME FROM "ID" to "id"

Sep_data_usg = merge(Sep_data, Sep_usg, by = "id", all = TRUE)   ### MERGING JULY DATA and UsAGE

Sep_merge = merge(Sep_data_usg, Sep_pay, by = "id", all = TRUE, allow.cartesian=TRUE) ## MERGING DATA USAGE with PAY

Sep_del<- Sep_merge %>% select(-c(cust_value_seg:acct_actv1)) ### REMOVING THESE COLS

Sep_WNA <- Sep_del[rowSums(is.na(Sep_del)) <= 25,]    ### REMOVING ROWS WHERE ALL THE VALUES ARE NA EXCEPT id column

Sep_WDR <- unique(Sep_WNA, by = "id") #### REMOVING THE DUPLICATE RECORDS
Sep_WDR = as.data.frame(Sep_WDR)

Sep_temp <- as.data.frame(Sep_WDR[,-c(1,2,3,22)])  ###### DROPPING THE COLUMNS WITH CATEGORICAL DATA

##### FUNCTION TO REMOVE OUTLIERS ############
remove_outliers <- function(x) {
  quant <- quantile(as.numeric(x), probs=c(.25, .75), na.rm = TRUE)
  outl<- 1.5 * IQR(x, na.rm = TRUE)
  ss <- x
  ss[x < (quant[1] - outl)] <- NA
  ss[x > (quant[2] + outl)] <- NA
  ss
}

Sep_1 <- as.data.frame(apply(Sep_temp, 2, remove_outliers))
Sep_2 <- as.data.frame(Sep_WDR[,c(1,2,3,22)])

Sep_NA <- as.data.frame(cbind(Sep_2,Sep_1))  

######## REPLACING NAs WITH MEDIAN,MEAN and MODE FOR NUMERICAL DATA, INTEGER DATA and CATEGORICAL DATA  RESPECTIVELY###################

Modalvalue <- function (x, na.rm) {
  pp <- table(x)
  m <- names(which(pp == max(pp)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

###### TRAVERSING EACH COLUMN AND REPLCING NA WITH APPROPRIATE STATISTICAL MEASURES ##############

for (i in 1:ncol(Sep_NA)) {
  if (class(Sep_NA[,i])=="numeric") {
    Sep_NA[is.na(Sep_NA[,i]),i] <- median(Sep_NA[,i], na.rm = TRUE)
  } else if (class(Sep_NA[,i])=="integer") {
    Sep_NA[is.na(Sep_NA[,i]),i] <- mean(Sep_NA[,i], na.rm = TRUE)
  } else if (class(Sep_NA[,i]) %in% c("character", "factor")) {
    Sep_NA[is.na(Sep_NA[,i]),i] <- Modalvalue(Sep_NA[,i], na.rm = TRUE)
  }
}

######## MUTATING NEW VARIABLES #############

Sep_newdata <- Sep_NA %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(IC_PROP = (TOT_IC)/(TOT_IC+TOT_OG)) %>%
  mutate(OG_PROP = (TOT_OG)/(TOT_IC+TOT_OG)) %>%
  mutate(vol2g_PROP = (vol2g)/(vol2g+vol3g)) %>%
  mutate(vol3g_PROP = (vol3g)/(vol2g+vol3g)) %>%
  mutate(TOT_MOU = TOT_OG+TOT_IC)%>%
  mutate(VOICE_PROP = (TOT_MOU)/((TOT_MOU)+(vol2g+vol3g))) %>%
  mutate(DATA_PROP = (vol2g+vol3g)/((TOT_MOU)+(vol2g+vol3g)))

sapply(Sep_newdata, function(x) sum(is.na(x)))

###REMOVING NaN's ########
for (i in 1:ncol(Sep_newdata)){
  Sep_newdata[is.nan(Sep_newdata[,i]),i]<- 0
}

###### CHANGING THE DATE FORMAT and IMPUTING THE NA WITH THE FIRST DATE OF SEPTEMBER ############ 

Sep_mobAcct <-format(dmy(Sep_newdata$mob_actv1),"%Y-%m-%d")
Df_Sep = as.data.frame(Sep_mobAcct,stringsAsFactors = FALSE)

Df_Sep$Sep_mobAcct[is.na(Df_Sep$Sep_mobAcct)] <- "2014-09-01"

Sep_newdata$mob_actv1 <- Df_Sep$Sep_mobAcct

sapply(Sep_newdata, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(Sep_newdata, file = "Sep_newdata.rda")

############### COMBINING JULY, AUGUST and SEPTEMBER ##############
#Jul_Aug = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Final_Data/Final/Jul_Aug.csv")
#Sep_newdata = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Final_Data/Final/Sep_newdata.csv")

load("Jul_Aug.rda")
load("Sep_newdata.rda")

JAS <- merge(Jul_Aug, Sep_newdata, by = "id", all = TRUE)

names(JAS)                            

JAS$mob_actv1.x[is.na(JAS$mob_actv1.x)] <- JAS$mob_actv1[is.na(JAS$mob_actv1.x)]

sapply(JAS, function(x) sum(is.na(x)))

JAS$mob_actv1<- NULL

setwd("/media/hduser/Local_Disk_E/new")
save(JAS, file = "JAS.rda")

############################# FETCHING OCTOBER DATA  #################################
#Oct_data <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_data1.csv"))
#Oct_usg <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_usg1.csv"))
#Oct_pay <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_pay1.csv"))

Oct_data = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Oct_data1.csv")
Oct_usg = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Oct_usg1.csv")
Oct_pay = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Oct_pay1.csv")

colnames(Oct_usg)[1] <- "id"      ### CHANGING THE COL NAME FROM "ID" to "id"

Oct_data_usg = merge(Oct_data, Oct_usg, by = "id", all = TRUE)   ### MERGING JULY DATA and UsAGE

Oct_merge = merge(Oct_data_usg, Oct_pay, by = "id", all = TRUE, allow.cartesian=TRUE) ## MERGING DATA USAGE with PAY

Oct_del<- Oct_merge %>% select(-c(cust_value_seg:acct_actv1)) ### REMOVING THESE COLS 

Oct_WNA <- Oct_del[rowSums(is.na(Oct_del)) <= 25,]    ### REMOVING ROWS WHERE ALL THE VALUES ARE NA EXCEPT id column

Oct_WDR <- unique(Oct_WNA, by = "id") #### REMOVING THE DUPLICATE RECORDS
Oct_WDR <- as.data.frame(Oct_WDR)
Oct_temp <- as.data.frame(Oct_WDR[,-c(1,2,3,22)])  ###### DROPPING THE COLUMNS WITH CATEGORICAL DATA

##### FUNCTION TO REMOVE OUTLIERS ############
remove_outliers <- function(x) {
  quant <- quantile(as.numeric(x), probs=c(.25, .75), na.rm = TRUE)
  outl<- 1.5 * IQR(x, na.rm = TRUE)
  ss <- x
  ss[x < (quant[1] - outl)] <- NA
  ss[x > (quant[2] + outl)] <- NA
  ss
}

Oct_1 <- as.data.frame(apply(Oct_temp, 2, remove_outliers))
Oct_2 <- as.data.frame(Oct_WDR[,c(1,2,3,22)])

Oct_NA <- as.data.frame(cbind(Oct_2,Oct_1))  

######## REPLACING NAs WITH MEDIAN,MEAN and MODE FOR NUMERICAL DATA, INTEGER DATA and CATEGORICAL DATA  RESPECTIVELY###################

Modalvalue <- function (x, na.rm) {
  pp <- table(x)
  m <- names(which(pp == max(pp)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

###### TRAVERSING EACH COLUMN AND REPLCING NA WITH APPROPRIATE STATISTICAL MEASURES ##############

for (i in 1:ncol(Oct_NA)) {
  if (class(Oct_NA[,i])=="numeric") {
    Oct_NA[is.na(Oct_NA[,i]),i] <- median(Oct_NA[,i], na.rm = TRUE)
  } else if (class(Oct_NA[,i])=="integer") {
    Oct_NA[is.na(Oct_NA[,i]),i] <- mean(Oct_NA[,i], na.rm = TRUE)
  } else if (class(Oct_NA[,i]) %in% c("character", "factor")) {
    Oct_NA[is.na(Oct_NA[,i]),i] <- Modalvalue(Oct_NA[,i], na.rm = TRUE)
  }
}

######## MUTATING NEW VARIABLES #############

Oct_newdata <- Oct_NA %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(IC_PROP = (TOT_IC)/(TOT_IC+TOT_OG)) %>%
  mutate(OG_PROP = (TOT_OG)/(TOT_IC+TOT_OG)) %>%
  mutate(vol2g_PROP = (vol2g)/(vol2g+vol3g)) %>%
  mutate(vol3g_PROP = (vol3g)/(vol2g+vol3g)) %>%
  mutate(TOT_MOU = TOT_OG+TOT_IC)%>%
  mutate(VOICE_PROP = (TOT_MOU)/((TOT_MOU)+(vol2g+vol3g))) %>%
  mutate(DATA_PROP = (vol2g+vol3g)/((TOT_MOU)+(vol2g+vol3g)))

sapply(Oct_newdata, function(x) sum(is.na(x)))

###REMOVING NaN's ########
for (i in 1:ncol(Oct_newdata)){
  Oct_newdata[is.nan(Oct_newdata[,i]),i]<- 0
}

###### CHANGING THE DATE FORMAT and IMPUTING THE NA WITH THE FIRST DATE OF OCTOBER ############ 

Oct_mobAcct <-format(dmy(Oct_newdata$mob_actv1),"%Y-%m-%d")
Df_Oct = as.data.frame(Oct_mobAcct,stringsAsFactors = FALSE)

Df_Oct$Oct_mobAcct[is.na(Df_Oct$Oct_mobAcct)] <- "2014-10-01"

Oct_newdata$mob_actv1 <- Df_Oct$Oct_mobAcct

sapply(Oct_newdata, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(Oct_newdata, file = "Oct_newdata.rda")

############### COMBINING JULY AUG SEP WITH OCT ####################

load("JAS.rda")
load("Oct_newdata.rda")

JASO <- merge(JAS, Oct_newdata, by = "id", all = TRUE)
names(JASO)

JASO$mob_actv1.x[is.na(JASO$mob_actv1.x)] <- JASO$mob_actv1[is.na(JASO$mob_actv1.x)]

sapply(JASO, function(x) sum(is.na(x)))
names(JASO)

JASO <- as.data.frame(JASO[,-c(122)])###Removed Activatin date

setwd("/media/hduser/Local_Disk_E/new")
save(JASO, file = "JASO.rda")

####################### FETCHING NOVEMBER DATA ##################################
#Nov_data <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_data1.csv"))
#Nov_usg <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_usg1.csv"))
#Nov_pay <- fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_pay1.csv"))

Nov_data = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Nov_data1.csv")
Nov_usg = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Nov_usg1.csv")
Nov_pay = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Nov_pay1.csv")

colnames(Nov_usg)[1] <- "id"      ### CHANGING THE COL NAME FROM "ID" to "id"

Nov_data_usg = merge(Nov_data, Nov_usg, by = "id", all = TRUE)   ### MERGING JULY DATA and UsAGE

Nov_merge = merge(Nov_data_usg, Nov_pay, by = "id", all = TRUE, allow.cartesian=TRUE) ## MERGING DATA USAGE with PAY

Nov_del<- Nov_merge %>% select(-c(cust_value_seg:acct_actv1)) ### REMOVING THESE COLS 

Nov_WNA <- Nov_del[rowSums(is.na(Nov_del)) <= 25,]    ### REMOVING ROWS WHERE ALL THE VALUES ARE NA EXCEPT id column

Nov_WDR <- unique(Nov_WNA, by = "id") #### REMOVING THE DUPLICATE RECORDS
Nov_WDR <- as.data.frame(Nov_WDR)
Nov_temp <- as.data.frame(Nov_WDR[,-c(1,2,3,22)])  ###### DROPPING THE COLUMNS WITH CATEGORICAL DATA

##### FUNCTION TO REMOVE OUTLIERS ############
remove_outliers <- function(x) {
  quant <- quantile(as.numeric(x), probs=c(.25, .75), na.rm = TRUE)
  outl<- 1.5 * IQR(x, na.rm = TRUE)
  ss <- x
  ss[x < (quant[1] - outl)] <- NA
  ss[x > (quant[2] + outl)] <- NA
  ss
}

Nov_1 <- as.data.frame(apply(Nov_temp, 2, remove_outliers))
Nov_2 <- as.data.frame(Nov_WDR[,c(1,2,3,22)])

Nov_NA <- as.data.frame(cbind(Nov_2,Nov_1))

######## REPLACING NAs WITH MEDIAN,MEAN and MODE FOR NUMERICAL DATA, INTEGER DATA and CATEGORICAL DATA  RESPECTIVELY###################

Modalvalue <- function (x, na.rm) {
  pp <- table(x)
  m <- names(which(pp == max(pp)))
  if (length(m) > 1) m <- ">1 mode"
  return(m)
}

###### TRAVERSING EACH COLUMN AND REPLCING NA WITH APPROPRIATE STATISTICAL MEASURES ##############

system.time(for (i in 1:ncol(Nov_NA)) {
  if (class(Nov_NA[,i])=="numeric") {
    Nov_NA[is.na(Nov_NA[,i]),i] <- median(Nov_NA[,i], na.rm = TRUE)
  } else if (class(Nov_NA[,i])=="integer") {
    Nov_NA[is.na(Nov_NA[,i]),i] <- mean(Nov_NA[,i], na.rm = TRUE)
  } else if (class(Nov_NA[,i]) %in% c("character", "factor")) {
    Nov_NA[is.na(Nov_NA[,i]),i] <- Modalvalue(Nov_NA[,i], na.rm = TRUE)
  }
})
######## MUTATING NEW VARIABLES #############

Nov_newdata <- Nov_NA %>%
  mutate(LOC_OG = var2a+var4a) %>%
  mutate(STD_OG = var3a+var5a) %>%
  mutate(TOT_OG = var9a+var17a.x+LOC_OG+STD_OG) %>%
  mutate(LOC_IC = var11a+var13a) %>%
  mutate(STD_IC = var12a+var14a) %>%
  mutate(TOT_IC = var16a.x+var18a.x+LOC_IC+STD_IC) %>%
  mutate(TOT_ROAM = var17a.x+var18a.x) %>%
  mutate(IC_PROP = (TOT_IC)/(TOT_IC+TOT_OG)) %>%
  mutate(OG_PROP = (TOT_OG)/(TOT_IC+TOT_OG)) %>%
  mutate(vol2g_PROP = (vol2g)/(vol2g+vol3g)) %>%
  mutate(vol3g_PROP = (vol3g)/(vol2g+vol3g)) %>%
  mutate(TOT_MOU = TOT_OG+TOT_IC)%>%
  mutate(VOICE_PROP = (TOT_MOU)/((TOT_MOU)+(vol2g+vol3g))) %>%
  mutate(DATA_PROP = (vol2g+vol3g)/((TOT_MOU)+(vol2g+vol3g)))

sapply(Nov_newdata, function(x) sum(is.na(x)))

###REMOVING NaN's ########
for (i in 1:ncol(Nov_newdata)){
  Nov_newdata[is.nan(Nov_newdata[,i]),i]<- 0
}

###### CHANGING THE DATE FORMAT and IMPUTING THE NA WITH THE FIRST DATE OF NOVEMBER ############ 

Nov_mobAcct <-format(dmy(Nov_newdata$mob_actv1),"%Y-%m-%d")
Df_Nov = as.data.frame(Nov_mobAcct,stringsAsFactors = FALSE)

Df_Nov$Nov_mobAcct[is.na(Df_Nov$Nov_mobAcct)] <- "2014-11-01"

Nov_newdata$mob_actv1 <- Df_Nov$Nov_mobAcct

sapply(Nov_newdata, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(Nov_newdata, file = "Nov_newdata.rda")

##########################################################
###### COMBINING July Aug Sep Oct and Nov ################
##########################################################

load("JASO.rda")
load("Nov_newdata.rda")

system.time(JASON <- merge(JASO, Nov_newdata, by = "id", all = TRUE))

JASON$mob_actv1.x[is.na(JASON$mob_actv1.x)] <- JASON$mob_actv1[is.na(JASON$mob_actv1.x)]

sapply(JASON, function(x) sum(is.na(x)))

JASON$mob_actv1 <- NULL

################################################################################
########################## WRITING THE FILE  ############################

setwd("/media/hduser/Local_Disk_E/new")
system.time(save(JASON, file = "JASON.rda"))

names(JASON)

################# READ the 5 month Data  ###########

load("JASON.rda")
JASON <- as.data.frame(JASON)

#### DROPPING ROWS WITH ALL NA's ##################

JASON <- JASON[rowSums(is.na(JASON)) <= 195,]

############### IMPUTING 0 IN ALL THE COLUMNS WITH NA's ###############

JASON[is.na(JASON)] <- 0
sapply(JASON, function(x) sum(is.na(x)))

setwd("/media/hduser/Local_Disk_E/new")
save(JASON, file = "JASON.rda")

############## READING DISCON DATA #####################

Discon = fread("/media/hduser/Local_Disk_D/CHURN_PROJECT/Churn_Data/Discon1.csv")
Discon <- as.data.frame(Discon)

#### A column name target was added in the discon dataset #####
Discon$target <- 1
colnames(Discon)[1] <- "id"

#### MERGING THE DISCON WITH JASON ###################

load("JASON.rda")

JASON_Dis <- merge(JASON, Discon, by = "id", all = TRUE)

sapply(JASON_Dis, function(x) sum(is.na(x)))
JASON_Dis$target[is.na(JASON_Dis$target)] <- 0

######## Changing the date format and Imputing NA with the first date of the MONTHS #######
library(lubridate)

Dis_Date <-format(dmy(JASON_Dis$Date2),"%Y-%m-%d")
Df_Dis = as.data.frame(Dis_Date,stringsAsFactors = FALSE)

Df_Dis$Dis_Date[is.na(Df_Dis$Dis_Date)] <- "2014-11-30"

JASON_Dis$Date2 <- Df_Dis$Dis_Date

sapply(JASON_Dis, function(x) sum(is.na(x)))

finaldata = na.omit(JASON_Dis)  ###### REMOVING ROWS WITH NA's ########

#### CALCULATING THE NO OF DAYS ###############

finaldata$Date2 <- as.Date(finaldata$Date2)

finaldata$mob_actv1.x <- as.Date(finaldata$mob_actv1.x)
class(finaldata$mob_actv1.x)

finaldata$No_of_days <- as.Date(as.character(finaldata$Date2), format="%Y-%m-%d",origin = "1899-12-30") - as.Date(as.character(finaldata$mob_actv1.x), format="%Y-%m-%d",origin = "1899-12-30")

##### WRITING the finaldata #######

setwd("/media/hduser/Local_Disk_E/new")
save(finaldata, file = "finaldata.rda")

########################################################################################
############# APPLYING MACHINE LEARNING ALGORITHM LOGISTIC REGRESSION ##################
########################################################################################
#To read the final data

load("finaldata.rda")

df <- as.data.frame(finaldata[,-c(1,2,3)]) ### DELETING IRRELEVANT COLUMNS

#colnames(df)[201] <- "target"
sapply(df, function(x) sum(is.na(x)))

df$target = as.factor(df$target)
class(df$target)

class1=subset(df,target=="1")
class0=subset(df,target=="0")

nrow(class1)
nrow(class0)

s1=sample(nrow(class1))
s0=sample(nrow(class0))

train1=class1[s1[1:round(nrow(class1)*0.7)],]
test1=class1[s1[(round(nrow(class1)*0.7)+1):nrow(class1)],]

train0=class0[s0[1:round(nrow(class0)*.70)],]
test0=class0[s0[(round(nrow(class0)*.70)+1):nrow(class0)],]

train=rbind(train1,train0)
test=rbind(test1,test0)

#train_new$target <- as.factor(train_new$target)
class(train$target)
class(test$target)

############### For saving the train & Test dat################
setwd("/media/hduser/Local_Disk_E/new")
save(train, file = "train.rda")
save(test, file = "test.rda")

################################# #reading the train & test data ##########################
#setwd("/media/hduser/Local_Disk_E/new")

setwd("/home/hduser1/new/")

load("train.rda")
load("test.rda")

library(MASS)
library(car)
library(data.table)
library(dplyr)
library(lubridate)
library(caret)

sapply(test, function(x) sum(is.na(x)))
sapply(train, function(x) sum(is.na(x)))

test2 = test[-196] ###### removed the target of test data #########

fit <- glm(target~., family = binomial(link = "logit"), data = train)
summary(fit)

#system.time(stepAIC(fit))

pred <- predict(fit, test2, type = "response")
test2$final <- ifelse(pred >= 0.5, 1, 0)

confusionMatrix(data = test2$final, reference = test$target,positive = "1")

####################### For ROC Curve #############################

library(pROC)

g <- roc(as.numeric(test[,196]) ~ as.numeric(test2$final), test)
plot(g)

################### Area under the curve #########################

auc(g)*100

######## BAR GRAPH #########################

install.packages("plotly")
library(plotly)

B <- c(49260, 385906, 13337,61691)
#barplot(B, col="darkgreen")

barplot(B,col="darkgreen", main="CONFUSION MATRIX", xlab="MODEL PERFORMANCE", ylab="VALUES", names.arg=c("TP", "TN", "FP","FN"))
