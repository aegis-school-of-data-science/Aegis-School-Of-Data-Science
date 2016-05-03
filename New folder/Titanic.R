setwd("F:/Data for Project/5 Project/Titanic")
train <- read.csv("F:/Data for Project/5 Project/Titanic/train.csv", stringsAsFactors = FALSE, header = T)
mean(train$Fare)
mode(train$Age)
actual_mode <- table(train$Age)
actual_mode
names(actual_mode)[actual_mode == max(actual_mode)]
median(train$Fare)
boxplot(train$Age ~ train$Pclass, xlab = "Class", ylab = "Age", col = c("red"))

