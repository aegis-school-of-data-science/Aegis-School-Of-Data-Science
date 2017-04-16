#****************************************************************************************#
#VALUEMETRICS:
#Submitted By: Harshad Madhamshettiwar
#Created On:18/04/2016
#Course Name: EPGP BABD -Jan2015
#
#This is towards submission for the capstones project
#
#Software and Package Requirement:
# This script needs the R version 3.2.3 or more and Rstudio version 0.99.879 or more
#
#This is V2 of the code and created as the extenstion to the existing project prsented while first presentation
#****************************************************************************************#

getwd()
setwd("D:\\Business Analytics and Big Data-PGP_AEGIS SCHOOL OF BUSINESS\\Capstones Project\\HM\\R directory")


#load required packages
library(rpart)
library(data.table)

#read data
tree_data = read.csv("insurance_data_aegis_harshadm.csv")

#Divide dataset into 70:30 as training and testing data
tree_ran <- sample(2, nrow(tree_data), replace=TRUE, prob=c(0.7, 0.3))
tree_data.train <- tree_data[tree_ran==1,]
tree_data.test <- tree_data[tree_ran==2,]


# train a decision tree
hmformula <-Losses~Age+Years.of.Driving.Experience+Number.of.Vehicles+gender_dummy+married_dummy+Vehicle.Age+fuel_dummy

insurance_rpart <- rpart(hmformula, data = tree_data.train,method = "anova",control = rpart.control(minsplit = 30,cp=0.01))

#attributes(insurance_rpart)
print(insurance_rpart$cptable)
print(insurance_rpart)

plotcp(insurance_rpart)
rsq.rpart(insurance_rpart)
summary(residuals(insurance_rpart))
plot(predict(insurance_rpart),residuals(insurance_rpart))


plot(insurance_rpart)
text(insurance_rpart, use.n=T)

#this is taking out the selection with minimum error, ie. maximum variance explained
opt <- which.min(insurance_rpart$cptable[,"xerror"])

#for above selection take out that cp value entry
cp <- insurance_rpart$cptable[opt, "CP"]

#for prune backwards degradation or feature reduction use above cp value
insurance_prune <- prune(insurance_rpart, cp = cp)
print(insurance_prune)
plot(insurance_prune)
prp(insurance_prune)
text(insurance_prune, use.n=T)

#once the prune model is ready now use that to predic the values
inslosses_pred <- predict(insurance_prune, newdata=tree_data.test)

xlim <- range(tree_data$Losses)
plot(inslosses_pred ~ Losses, data=tree_data.test, xlab="Observed", 
     ylab="Predicted", ylim=xlim, xlim=xlim)
abline(a=0, b=1)


