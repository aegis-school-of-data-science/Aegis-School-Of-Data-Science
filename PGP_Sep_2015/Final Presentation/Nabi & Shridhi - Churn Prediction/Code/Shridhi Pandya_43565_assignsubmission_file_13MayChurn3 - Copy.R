#LIBRARY ----
library(data.table)
library(dplyr)
library(rhdfs)
library(modeest)
library(lubridate)

# Parallel Computing 
library(doParallel)
registerDoParallel() 
getDoParWorkers()
registerDoSEQ()  
getDoParWorkers()

registerDoParallel(cores=3)  
getDoParWorkers()

registerDoParallel(cores=300)  
getDoParWorkers()

cl <- makeCluster(3)  
registerDoParallel(cl)  
stopCluster(cl)


#LIBRARY END ----

# MERGING july,August,september,october,November [ Month.name_DATA1.csv | Month.name_PAY1.csv | Month.name_USG1.csv  ] 
# 1) JULY ----
#jul_data1 <- fread("july_data1.csv")# for Local System
jul_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_data1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System

jul_data1<-as.data.frame(jul_data1)# Changing to Data Frame
sort(sapply(jul_data1, function(x) sum(is.na(x))))#Checking Na's for every Column



#jul_pay1 <- fread("july_pay1.csv")# for Local System
jul_pay1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/July_pay1.csv")
                ,na.strings=c("NA","N/A",""))# for HDFS System
jul_pay1<-as.data.frame(jul_pay1)# Changing to Data Frame
sort(sapply(jul_pay1, function(x) sum(is.na(x))))#Checking Na's for every Column

#jul_usg1 <- fread("Jul_usg1.csv")# for Local System
jul_usg1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Jul_usg1.csv")
                ,na.strings=c("NA","N/A",""))# for HDFS System
jul_usg1<-as.data.frame(jul_usg1) # Changing to Data Frame
sort(sapply(jul_usg1, function(x) sum(is.na(x))))#Checking Na's for every Column


jul_data_pay<-merge(jul_data1,jul_pay1,by="id",all=TRUE)
jul_data_pay<-as.data.frame(jul_data_pay)# Changing to Data Frame

jul_PDU<-merge(jul_data_pay,jul_usg1,by="id",all=TRUE)
jul_PDU<-as.data.frame(jul_PDU)# Changing to Data Frame

rm(jul_data1,jul_pay1,jul_usg1,jul_data_pay)#Clearing the Not required Data Frame





jul_PDU$V1<-NULL
jul_PDU$`2G_IND_1`<-NULL
jul_PDU$`3G_IND_1`<-NULL
jul_PDU$Cust_Value_Seg_1<-NULL
jul_PDU$Cust_Group_1<-NULL
jul_PDU$Cust_Section_1<-NULL
jul_PDU$Mob_actv_date_1<-NULL


jul_PDU <- fread("Jul_PDU.csv")
jul_PDU$id<-as.factor(jul_PDU$id)
jul_by_id <- group_by(jul_PDU,id)

                                  
#----Writing Data in HDFS
#write.csv(jul_final, file = "jul_final.csv")# Writing to Local System
write.csv(jul_final, file = "jul_final.csv")# Writing Data in HDFS
hdfs.mkdir("/tmp/nabi_results") # Making Directory in HDFS
hdfs.put("jul_final.csv", "/tmp/nabi_results/") # Copying to Nabi_results directory
#JULY END ----

# 2) AUGUST ----
#aug_data1 <- fread("Aug_data1.csv")# for Local System
aug_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_data1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System

aug_data1<-as.data.frame(Aug_data1)# Changing to Data Frame
sort(sapply(aug_data1, function(x) sum(is.na(x))))#Checking Na's for every Column



#aug_pay1 <- fread("Aug_pay1.csv")# for Local System
aug_pay1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_pay1.csv")
                ,na.strings=c("NA","N/A",""))# for HDFS System
aug_pay1<-as.data.frame(aug_pay1)# Changing to Data Frame
sort(sapply(aug_pay1, function(x) sum(is.na(x))))#Checking Na's for every Column


aug_usg1 <- fread("Aug_usg1.csv")# for Local System
aug_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Aug_usg1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
aug_usg1<-as.data.frame(aug_usg1) # Changing to Data Frame
sort(sapply(aug_usg1, function(x) sum(is.na(x))))#Checking Na's for every Column

aug_data_pay<-merge(aug_data1,aug_pay1,by="id",all=TRUE)
aug_data_pay<-as.data.frame(aug_data_pay)# Changing to Data Frame

aug_PDU<-merge(aug_data_pay,aug_usg1,by="id",all=TRUE)
aug_PDU<-as.data.frame(aug_PDU)# Changing to Data Frame

rm(aug_data1,aug_pay1,aug_usg1,aug_data_pay)#Clearing the Not required Data Frame





aug_PDU$V1<-NULL
aug_PDU$`2G_IND_1`<-NULL
aug_PDU$`3G_IND_1`<-NULL
aug_PDU$Cust_Value_Seg_1<-NULL
aug_PDU$Cust_Group_1<-NULL
aug_PDU$Cust_Section_1<-NULL
aug_PDU$Mob_actv_date_1<-NULL



aug_PDU$id<-as.factor(aug_PDU$id)

#----Writing Data in HDFS
write.csv(aug_final, file = "aug_final.csv")# Writing to Local System
write.csv(aug_final, file = "aug_final.csv")# Writing Data in HDFS
hdfs.mkdir("/tmp/nabi_results") # Making Directory in HDFS
hdfs.put("aug_final.csv", "/tmp/nabi_results/") # Copying to Nabi_results directory
#AUGUST END ----

# 3) SEPTEMBER ----
sep_data1 <- fread("Sep_data1.csv")# for Local System
sep_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_data1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System

sep_data1<-as.data.frame(Sep_data1)# Changing to Data Frame
sort(sapply(sep_data1, function(x) sum(is.na(x))))#Checking Na's for every Column



sep_pay1 <- fread("Sep_pay1.csv")# for Local System
sep_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_pay1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
sep_pay1<-as.data.frame(sep_pay1)# Changing to Data Frame
sort(sapply(sep_pay1, function(x) sum(is.na(x))))#Checking Na's for every Column


sep_usg1 <- fread("Sep_usg1.csv")# for Local System
sep_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Sep_usg1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
sep_usg1<-as.data.frame(sep_usg1) # Changing to Data Frame
sort(sapply(sep_usg1, function(x) sum(is.na(x))))#Checking Na's for every Column


sep_data_pay<-merge(sep_data1,sep_pay1,by="id",all=TRUE)
sep_data_pay<-as.data.frame(sep_data_pay)# Changing to Data Frame

sep_PDU<-merge(sep_data_pay,sep_usg1,by="id",all=TRUE)
sep_PDU<-as.data.frame(sep_PDU)# Changing to Data Frame

rm(sep_data1,sep_pay1,sep_usg1,sep_data_pay)#Clearing the Not required Data Frame





sep_PDU$V1<-NULL
sep_PDU$`2G_IND_1`<-NULL
sep_PDU$`3G_IND_1`<-NULL
sep_PDU$Cust_Value_Seg_1<-NULL
sep_PDU$Cust_Group_1<-NULL
sep_PDU$Cust_Section_1<-NULL
sep_PDU$Mob_actv_date_1<-NULL




#----Writing Data in HDFS
write.csv(sep_final, file = "sep_final.csv")# Writing to Local System
write.csv(sep_final, file = "sep_final.csv")# Writing Data in HDFS
hdfs.mkdir("/tmp/nabi_results") # Making Directory in HDFS
hdfs.put("sep_final.csv", "/tmp/nabi_results/") # Copying to Nabi_results directory
#SEPTEMBER END ----

# 4) OCTOBER ----
oct_data1 <- fread("Oct_data1.csv")# for Local System
oct_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_data1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System

oct_data1<-as.data.frame(oct_data1)# Changing to Data Frame
sort(sapply(oct_data1, function(x) sum(is.na(x))))#Checking Na's for every Column



oct_pay1 <- fread("Oct_pay1.csv")# for Local System
oct_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_pay1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
oct_pay1<-as.data.frame(oct_pay1)# Changing to Data Frame
sort(sapply(oct_pay1, function(x) sum(is.na(x))))#Checking Na's for every Column


oct_usg1 <- fread("Oct_usg1.csv")# for Local System
oct_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Oct_usg1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
oct_usg1<-as.data.frame(oct_usg1) # Changing to Data Frame
sort(sapply(oct_usg1, function(x) sum(is.na(x))))#Checking Na's for every Column


oct_data_pay<-merge(oct_data1,oct_pay1,by="id",all=TRUE)
oct_data_pay<-as.data.frame(oct_data_pay)# Changing to Data Frame

oct_PDU<-merge(oct_data_pay,oct_usg1,by="id",all=TRUE)
oct_PDU<-as.data.frame(oct_PDU)# Changing to Data Frame

rm(oct_data1,oct_pay1,oct_usg1,oct_data_pay)#Clearing the Not required Data Frame





oct_PDU$V1<-NULL
oct_PDU$`2G_IND_1`<-NULL
oct_PDU$`3G_IND_1`<-NULL
oct_PDU$Cust_Value_Seg_1<-NULL
oct_PDU$Cust_Group_1<-NULL
oct_PDU$Cust_Section_1<-NULL
oct_PDU$Mob_actv_date_1<-NULL




#----Writing Data in HDFS
write.csv(oct_final, file = "oct_final.csv")# Writing to Local System
write.csv(oct_final, file = "oct_final.csv")# Writing Data in HDFS
hdfs.mkdir("/tmp/nabi_results") # Making Directory in HDFS
hdfs.put("oct_final.csv", "/tmp/nabi_results/") # Copying to Nabi_results directory
#OCTOBER END ----

# 5) NOVEMBER ----
nov_data1 <- fread("Nov_data1.csv")# for Local System
nov_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_data1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System

nov_data1<-as.data.frame(nov_data1)# Changing to Data Frame
sort(sapply(nov_data1, function(x) sum(is.na(x))))#Checking Na's for every Column

nov_pay1 <- fread("Nov_pay1.csv")# for Local System
nov_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_pay1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
nov_pay1<-as.data.frame(nov_pay1)# Changing to Data Frame
sort(sapply(nov_pay1, function(x) sum(is.na(x))))#Checking Na's for every Column


nov_usg1 <- fread("Nov_usg1.csv")# for Local System
nov_data1<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/projectdata/churndata/Nov_usg1.csv")
                 ,na.strings=c("NA","N/A",""))# for HDFS System
nov_usg1<-as.data.frame(nov_usg1) # Changing to Data Frame
sort(sapply(nov_usg1, function(x) sum(is.na(x))))#Checking Na's for every Column


nov_data_pay<-merge(nov_data1,nov_pay1,by="id",all=TRUE)
nov_data_pay<-as.data.frame(nov_data_pay)# Changing to Data Frame

nov_PDU<-merge(nov_data_pay,nov_usg1,by="id",all=TRUE)
nov_PDU<-as.data.frame(nov_PDU)# Changing to Data Frame

rm(nov_data1,nov_pay1,nov_usg1,nov_data_pay)#Clearing the Not required Data Frame





nov_PDU$V1<-NULL
nov_PDU$`2G_IND_1`<-NULL
nov_PDU$`3G_IND_1`<-NULL
nov_PDU$Cust_Value_Seg_1<-NULL
nov_PDU$Cust_Group_1<-NULL
nov_PDU$Cust_Section_1<-NULL
nov_PDU$Mob_actv_date_1<-NULL




#----Writing Data in HDFS
write.csv(nov_final, file = "nov_final.csv")# Writing to Local System
write.csv(nov_final, file = "nov_final.csv")# Writing Data in HDFS
hdfs.mkdir("/tmp/nabi_results") # Making Directory in HDFS
hdfs.put("nov_final.csv", "/tmp/nabi_results/") # Copying to Nabi_results directory
#NOVEMBER END ----

# Final Merging ----
jul_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/nabi_results/jul_final.csv"),
                na.strings=c("NA","N/A",""))
jul_data<-as.data.frame(jul_data)
aug_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/nabi_results/aug_final.csv"),
                na.strings=c("NA","N/A",""))
aug_data<-as.data.frame(aug_data)

#jul_data_unique <- unique(jul_data)
#aug_data_unique <- unique(aug_data)
jul_aug<-merge(aug_data_unique, jul_data_unique, by="id", all=TRUE)

#------------------------------------------------------------------------------------------

sep_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/nabi_results/sep_final.csv"),
                na.strings=c("NA","N/A",""))
sep_data<-as.data.frame(sep_data)
oct_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/nabi_results/oct_final.csv"),
                na.strings=c("NA","N/A",""))
oct_data<-as.data.frame(oct_data)

sep_data_unique <- unique(sep_data)
oct_data_unique <- unique(oct_data)
sep_oct<-merge(oct_data_unique, sep_data_unique, by="id", all=TRUE)

#------------------------------------------------------------------------------------------

nov_data<-fread(paste("/opt/hadoop/bin/hadoop", "fs -text /tmp/nabi_results/nov_final.csv"),
                na.strings=c("NA","N/A",""))
nov_data<-as.data.frame(nov_data)

nov_data_unique <- unique(nov_data)
JULAUG_data_unique<-unique(jul_aug)
SEPOCT<-unique(sep_oct)

JULAUG_nov<-merge(JULAUG_data_unique, nov_data_unique, by="id", all=TRUE)
Final_view<-merge(JULAUG_nov, SEPOCT, by="id", all=TRUE)

#write.csv(Final_view, file = "Final.csv")
write.csv(Final_view, file = "derived_variable.csv")
hdfs.mkdir("/tmp/nabi_results")
hdfs.put("Final.csv", "/tmp/nabi_results/")
#-----------------

newdata1 = fread("/home/hduser1/Nabi/derived_variable.csv")
newdata1 <- as.data.frame(newdata1)
sort(sapply(newdata1, function(x) sum(is.na(x))))#Checking Na's for every Column

# Changing Date format to "%Y/%m/%d"
newdata1$mob_actv1 <-format(dmy(newdata1$mob_actv1),"%Y/%m/%d")
newdata1$mob_act_x2 <-format(dmy(newdata1$mob_act_x2),"%Y/%m/%d")
newdata1$mob_act_y2 <-format(dmy(newdata1$mob_act_y2),"%Y/%m/%d")
newdata1$mob_actv1_x <-format(dmy(newdata1$mob_actv1_x),"%Y/%m/%d")
newdata1$mob_actv1_y <-format(dmy(newdata1$mob_actv1_y),"%Y/%m/%d")

# Changing Data type to as.date
newdata1$mob_actv1 <-as.Date(newdata1$mob_actv1)
newdata1$mob_actv1_x <-as.Date(newdata1$mob_actv1_x)
newdata1$mob_actv1_y <-as.Date(newdata1$mob_actv1_y)
newdata1$mob_act_x2 <-as.Date(newdata1$mob_act_x2)
newdata1$mob_act_y2 <-as.Date(newdata1$mob_act_y2)

# Changing Data type to as.date
newdata1$mob_actv1 <-as.Date(newdata1$mob_actv1)
newdata1$mob_actv1_x <-as.Date(newdata1$mob_actv1_x)
newdata1$mob_actv1_y <-as.Date(newdata1$mob_actv1_y)
newdata1$mob_act_x2 <-as.Date(newdata1$mob_act_x2)
newdata1$mob_act_y2 <-as.Date(newdata1$mob_act_y2)

# Replacing Na of activation date for particular Month with Starting date of Month
newdata1$mob_actv1[is.na(newdata1$mob_actv1)]<-"2014/7/01"
newdata1$mob_actv1_x[is.na(newdata1$mob_actv1_x)]<-"2014/08/01"
newdata1$mob_actv1_y[is.na(newdata1$mob_actv1_y)]<-"2014/09/01"
newdata1$mob_act_x2[is.na(newdata1$mob_act_x2)]<-"2014/10/01"
newdata1$mob_act_y2[is.na(newdata1$mob_act_y2)]<-"2014/11/01"

# Derived Variable

newdataa <- group_by(newdata1,id) %>%
  mutate(var2= VAR2_x+VAR2_y+VAR2_x+VAR2_y+VAR2) %>%
  mutate(var3 = VAR3_x+VAR3_y+VAR3_x+VAR3_y+VAR3) %>%
  mutate(Vol2g = vol2g_x+vol2g_y+vol2g_x+vol2g_y+vol2g) %>%
  mutate(Vol3g = vol3g_x+vol3g_y+vol3g_x+vol3g_y+vol3g) %>%
  mutate(Total_Bill = var15a_x_x+var15a_x_y+var15a_x_x+var15a_x_y+var15a_x) %>%
  mutate(RentalCharge = var16a_x_x+var16a_x_y+var16a_x_x+var16a_x_y+var16a_x) %>%
  mutate(NonRentalCharge = var17a_x_x+var17a_x_y+var17a_x_x+var17a_x_y+var17a_x) %>%
  mutate(Adj = var18a_x_x+var18a_x_y+var18a_x_x+var18a_x_y+var18a_x) %>%
  mutate(Usage = var19a_x_x+var19a_x_y+var19a_x_x+var19a_x_y+var19a_x) %>%
  mutate(LOC_OG_XYZ2XYZ_MOU = var2a_x+var2a_y+var2a_x+var2a_y+var2a) %>%
  mutate(STD_OG_XYZ2XYZ_MOU = var3a_x+var3a_y+var3a_x+var3a_y+var3a) %>%
  mutate(LOC_OG_XYZ2M_MOU = var4a_x+var4a_y+var4a_x+var4a_y+var4a) %>%
  mutate(STD_OG_XYZ2M_MOU = var5a_x+var5a_y+var5a_x+var5a_y+var5a) %>%
  mutate(STD_OG_MOU = var6a_x+var6a_y+var6a_x+var6a_y+var6a) %>%
  mutate(ISD_OG_MOU = var9a_x+var9a_y+var9a_x+var9a_y+var9a) %>%
  mutate(TOTAL_OG_MOU = var10a_x+var10a_y+var10a_x+var10a_y+var10a) %>%
  mutate(LOC_IC_XYZ2XYZ_MOU = var11a_x+var11a_y+var11a_x+var11a_y+var11a) %>%
  mutate(STD_IC_XYZ2XYZ_MOU = var12a_x+var12a_y+var12a_x+var12a_y+var12a) %>%
  mutate(LOC_IC_XYZ2M_MOU = var13a_x+var13a_y+var13a_x+var13a_y+var13a) %>%
  mutate(STD_IC_XYZ2M_MOU = var14a_x+var14a_y+var14a_x+var14a_y+var14a) %>%
  mutate(STD_IC_MOU = var15a_y_x+var15a_y_y+var15a_y_x+var15a_y_y+var15a_y) %>%
  mutate(ISD_IC_MOU = var16a_y_x+var16a_y_y+var16a_y_x+var16a_y_y+var16a_y) %>%
  mutate(ROAM_OG_MOU = var17a_y_x+var17a_y_y+var17a_y_x+var17a_y_y+var17a_y) %>%
  mutate(ROAM_IC_MOU = var18a_y_x+var18a_y_y+var18a_y_x+var18a_y_y+var18a_y) %>%
  mutate(TOTAL_IC_MOU = var19a_y_x+var19a_y_y+var19a_y_x+var19a_y_y+var19a_y)

#-----------------------------------------------------------------------------

# Importing Disconnection CSV
dis = fread("/home/nabi_s/Documents/Discon1.csv")
dis<-as.data.frame(dis)
names(dis)[1]<-"id"
sort(sapply(dis, function(x) sum(is.na(x))))#Checking Na's for every Column
str(dis)
dis$Date2 <-format(dmy(dis$Date2),"%Y/%m/%d")
dis$Date2 <- as.Date(dis$Date2)
dis$churn_status <-1

sort(sapply(newdataa, function(x) sum(is.na(x))))#Checking Na's for every Column
newdata2=newdataa
newdata3=newdataa


# Case-01 Omitting Na
newdata2<- na.omit(newdata1) # Omintting NA 
sort(sapply(newdata2, function(x) sum(is.na(x))))#Checking Na's for every Column

# Case-02 Subtituting Na with 0
newdata3[is.na(newdata1)] <- 0 # Replacing na with 0
sort(sapply(newdata3, function(x) sum(is.na(x))))#Checking Na's for every Column

library(lubridate)
# Merging 
fin2 = merge(newdata2, dis, by = "id",all=TRUE)
fin3 = merge(newdata3, dis, by = "id",all=TRUE)
table(fin$Date2,useNA = "ifany")
sort(sapply(fin, function(x) sum(is.na(x))))#Checking Na's for every Column
str(fin)

fin2$Date2[is.na(fin2$Date2)]<-"2014/11/30"
fin3$Date2[is.na(fin3$Date2)]<-"2014/11/30"

fin2$churn_status[is.na(fin2$churn_status)]<-0
fin3$churn_status[is.na(fin3$churn_status)]<-0

sort(sapply(fin2, function(x) sum(is.na(x))))# fin2 na is omit
sort(sapply(fin3, function(x) sum(is.na(x))))# fin3 0 is replacing

write.csv(fin2, "/home/hduser1/Nabi/fin2.csv")
write.csv(fin3, "/home/hduser1/Nabi/fin3.csv")

fin<- na.omit(fin)
fin3<- na.omit(fin3)

# FIN2 START ----
fin <- fin2
names(fin)
sort(sapply(fin, function(x) sum(is.na(x))))
str(fin)
glimpse(fin)
fin$churn_status<-as.factor(fin$churn_status)
class(fin$churn_status)
cls1=subset(fin,churn_status=="1")
cls0=subset(fin,churn_status=="0")
set.seed(123)
s1=sample(nrow(cls1))
s0=sample(nrow(cls0))

train1=cls1[s1[1:round(nrow(cls1)*0.70)],]
test1=cls1[s1[(round(nrow(cls1)*0.70)+1):nrow(cls1)],]

train0=cls0[s0[1:round(nrow(cls0)*0.70)],]
test0=cls0[s0[(round(nrow(cls0)*0.70)+1):nrow(cls0)],]

train=rbind(train1,train0)
sort(sapply(train, function(x) sum(is.na(x))))
test=rbind(test1,test0)
sort(sapply(test, function(x) sum(is.na(x))))

write.csv(train, "/home/hduser1/Nabi/train_CLEANED.csv")
write.csv(test, "/home/hduser1/Nabi/test_CLEANED.csv")

#############################
library(data.table)

train = fread("/home/hduser1/Nabi/train_CLEANED.csv")
test = fread("/home/hduser1/Nabi/test_CLEANED.csv")

# Copying Customer ID into Anouther Vector
train_ID<-train$id
test_ID<-test$id

#Removing Customer ID from Train & Test DATA SET
train$id <- NULL
test$id <- NULL

names(train)
names(test)
dim(train)
dim(test)

# Copying Churn Status Variable into Anouther Vector
train_churn_status<-train$churn_status
test_churn_status<-test$churn_status

#Removing Churn Status Variable from Test Data set
test$churn_status<-NULL

system.time(model.fit <- glm(churn_status~., family = binomial(link = "logit"), data = train))
summary(model.fit)

test1 <- test[,-c(153)]

pred <- predict(model.fit, test1, type = "response")
test1$final <- ifelse(pred >= 0.5, 1, 0)
confusionMatrix(data = test1$final, reference = test$churn_status, positive = "1")
# FIN2 END ----

# Data Loading ----
library(data.table)
setwd("/home/hduser1/Nabi")
fin3<-fread("fin3.csv")

fin$mob_actv1 <-as.Date(fin$mob_actv1)
fin$mob_actv1_x <-as.Date(fin$mob_actv1_x)
fin$mob_actv1_y <-as.Date(fin$mob_actv1_y)
fin$mob_act_x2 <-as.Date(fin$mob_act_x2)
fin$mob_act_y2 <-as.Date(fin$mob_act_y2)


# FIN3 START ----
fin <- fin3
names(fin)
sort(sapply(fin, function(x) sum(is.na(x))))
str(fin)
glimpse(fin)
fin$churn_status<-as.factor(fin$churn_status)
class(fin$churn_status)
cls1=subset(fin,churn_status=="1")
cls0=subset(fin,churn_status=="0")
set.seed(123)
s1=sample(nrow(cls1))
s0=sample(nrow(cls0))

train1=cls1[s1[1:round(nrow(cls1)*0.70)],]
test1=cls1[s1[(round(nrow(cls1)*0.70)+1):nrow(cls1)],]

train0=cls0[s0[1:round(nrow(cls0)*0.70)],]
test0=cls0[s0[(round(nrow(cls0)*0.70)+1):nrow(cls0)],]

train=rbind(train1,train0)
sort(sapply(train, function(x) sum(is.na(x))))
test=rbind(test1,test0)
sort(sapply(test, function(x) sum(is.na(x))))

write.csv(train, "/home/hduser1/Nabi/train_CLEANED.csv")
write.csv(test, "/home/hduser1/Nabi/test_CLEANED.csv")

#############################
library(data.table)
library(caret)
train = fread("/home/hduser1/Nabi/train_CLEANED.csv")
test = fread("/home/hduser1/Nabi/test_CLEANED.csv")


# Copying Customer ID into Anouther Vector
train_ID<-train$id
test_ID<-test$id

#Removing Customer ID from Train & Test DATA SET
train$id <- NULL
test$id <- NULL

names(train)
names(test)
dim(train)
dim(test)

# Copying Churn Status Variable into Anouther Vector
train_churn_status<-train$churn_status
test_churn_status<-test$churn_status

#Removing Churn Status Variable from Test Data set
#test$churn_status<-NULL
train$V1<-NULL
test$V1<-NULL
system.time(model.fit <- glm(churn_status~., family = binomial(link = "logit"), data = train))
summary(model.fit)

test1 <- test[,-c(153)]

pred <- predict(model.fit, test1, type = "response")
test1$final <- ifelse(pred >= 0.5, 1, 0)
confusionMatrix(data = test1$final, reference = test$churn_status, positive = "1")


library(pROC)

g <- roc(as.numeric(test[,153]) ~ as.numeric(test1$final), test)
#g <- roc(response = as.numeric(test[,196]) ~ predictor = as.numeric(test2$final), test)

plot(g)
auc(g)*100
# FIN3 END ----
