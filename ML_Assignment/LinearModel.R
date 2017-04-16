library(UsingR)
library(ggplot2)
library(reshape2)
library(plyr)
head(father.son)
library(QuantPsyc)
library(car)

### Data Visualization ########
ggplot(father.son, aes(x = fheight, y = sheight)) + geom_point() + 
geom_smooth(method = "lm" ) + labs(x = "Fathers", y = "Sons")

######################################################################

heightsLM <- lm(sheight ~ fheight, data = father.son)

summary(heightsLM)


############### ANOVA ##########
head(tips) ## information about tip a waiter recieved over a period

### tip in dollars
### bill in dollars
### sex of the bill payer
### day of the week
### time of the day
### size of the party
### whether there were smokers in the party

tipsAnova <- aov(tip ~ day - 1, data = tips)
tipsLM <- lm(tip ~ day - 1, data = tips)

summary(tipsAnova)
summary(tipsLM)

tipsByDay <- ddply(tips, "day", summarize, tip.mean = mean(tip), tip.sd = sd(tip), Length = NROW(tip), tfrac = qt(p = 0.90, df = Length - 1),
                   Lower = tip.mean - tfrac*tip.sd/sqrt(Length),
                   Upper = tip.mean + tfrac * tip.sd/sqrt(Length))


tipsInfo <- summary(tipsLM)
tipsCoef <- as.data.frame(tipsInfo$coefficients[, 1:2])
################################################################################

housing <- read.csv("housing.csv", header = TRUE, stringsAsFactors = FALSE)
colnames(housing)

names(housing) <- c("Neighborhood", "Class", "Units", "YearBuilt", "SqFt",
                    "Income", "IncomePerSFt", "Expense", "ExpensePerSqFt",
                    "NetIncome", "Value", "ValuePerSqFt", "Boro")

ggplot(housing, aes(x = ValuePerSqFt)) + geom_histogram(binwidth = 10) + labs(x = "Value Per Square Foot")


ggplot(housing, aes(x = ValuePerSqFt, fill = Boro)) + 
  geom_histogram(binwidth = 10) + labs(x = "Value per Square Foot")


ggplot(housing, aes(x = ValuePerSqFt, fill = Boro)) + 
  geom_histogram(binwidth = 10) + labs(x = "Value per Square Foot") + facet_wrap(~Boro)



ggplot(housing, aes(x = SqFt)) + geom_histogram()
ggplot(housing, aes(x = Units)) + geom_histogram()

ggplot(housing[housing$Units < 1000, ], aes(x = SqFt)) + geom_histogram()

ggplot(housing[housing$Units < 1000, ], aes(x = Units)) + geom_histogram()



ggplot(housing, aes(x = SqFt, y = ValuePerSqFt)) + geom_point()
ggplot(housing, aes(x = Units, y = ValuePerSqFt)) + geom_point()
ggplot(housing[housing$Units < 1000, ], aes(x = SqFt, y = ValuePerSqFt)) + geom_point()
ggplot(housing[housing$Units < 1000, ], aes(x = Units, y = ValuePerSqFt)) + geom_point()

sum(housing$Units > 1000)


### removing these outliers 
housing <- housing[housing$Units < 1000, ]
ggplot(housing, aes(x = SqFt, y = ValuePerSqFt)) + geom_point()
ggplot(housing, aes(x = log(SqFt), y = ValuePerSqFt)) + geom_point()
ggplot(housing, aes(x = SqFt, y = log(ValuePerSqFt))) + geom_point()
ggplot(housing, aes(x = log(SqFt), y = log(ValuePerSqFt))) + geom_point()

#################################################################
ggplot(housing, aes(x = Units, y = ValuePerSqFt)) + geom_point()
ggplot(housing, aes(x = log(Units), y = ValuePerSqFt)) + geom_point()


ggplot(housing, aes(x = Units, y = log(ValuePerSqFt))) + geom_point()
ggplot(housing, aes(x = log(Units), y = log(ValuePerSqFt))) + geom_point()
####################################################################

housing_model1 <- lm(ValuePerSqFt ~ Units + SqFt + Boro, data = housing)

summary(housing_model1)

nrow(housing) - length(coef(housing_model1))

housing_model1$coefficients
coef(housing_model1)
coefficients(housing_model1)

#############################################################

housing_model2 <- lm(ValuePerSqFt ~ Units * SqFt + Boro, data = housing)
housing_model2$coefficients
housing_model3 <- lm(ValuePerSqFt ~ Units:SqFt + Boro, data = housing)
housing_model3$coefficients

housing_model4 <- lm(ValuePerSqFt ~ SqFt * Units * Income, data = housing)
housing_model4$coefficients

housing_model5 <- lm(ValuePerSqFt ~ Class * Boro, data = housing)
housing_model5$coefficient

housing_model6 <- lm(ValuePerSqFt ~ I(SqFt/Units) + Boro, data = housing)


###################################
housingNew <- read.csv("housingNew.csv", header = TRUE, stringsAsFactors = FALSE)
head(housingNew)
## making prediction with newdata and 95% confidence bounds

housing_predict <- predict(housing_model1, newdata = housingNew, se.fit = TRUE, interval = "prediction", level = 0.95 )


head(housing_predict$fit)

### Standard error Error for prediction

head(housing_predict$se.fit)

###########################################################

summary(housing_model1)
##Parsimony-adjusted measure of fit

### AIC = n*ln(SSE/n) + 2*p

album1 <- read.delim("AlbumSales1.dat", header = TRUE)
albumSales1_model <- lm(sales ~ adverts, data = album1)
summary(albumSales1_model)

album2 <- read.delim("AlbumSales2.dat", header = TRUE )
albumSales2_model <- lm(sales ~ adverts, data = album2 )
albumSales3_model <- lm(sales ~ adverts + airplay + attract, data = album2 )
albumSales3_model <- update(albumSales2_model, .~. + airplay + attract)


Rsquare = 0.6647
n = 200
k = 3

adjusted_Rquare <- 1 - ((n-1)/(n-k-1) * (n-2)/(n-k-2) * (n+1)/(n))*(1- Rsquare)
print(adjusted_Rquare)

F = (n-k - 1)*Rsquare/ (k*(1- Rsquare))

durbinWatsonTest(albumSales3_model)
dwt(housing_model1)

vif(housing_model1)

album2$fitted <- albumSales3_model$fitted.values
album2$standardized.residuals <- rstandard(albumSales3_model)
album2$studentized.residuals <- rstudent(albumSales3_model)
album2$cooks.distance <- cooks.distance(albumSales3_model)
album2$dfbeta <- dfbeta(albumSales3_model)
album2$dffit <- dffits(albumSales3_model)
album2$leverage <- hatvalues(albumSales3_model)
album2$covariance.ratios <- covratio(albumSales3_model)

head(album2)


histogram <- ggplot(album2, aes(standardized.residuals)) +
  geom_histogram(aes(y = ..density..), colour = "black", fill = "white") +
  labs(x = "Studentized Residual", y = "Density")

histogram + stat_function(fun = dnorm, args = list(mean = mean(album2$studentized.residuals, na.rm = TRUE)), colour = "red", size = 1)

### Q-Q plot
qqplot.resid <- qplot(sample = album2$studentized.residuals, stat = "qq") + labs(x = "Theoretical Value", y = "Observed Values")

scatter <- ggplot(album2, aes(fitted, studentized.residuals))
scatter + geom_point() + geom_smooth(method = "lm", colour = "Blue") + labs(x = "Fitted Values", y = "Studentized Residual")


hist(album2$studentized.residuals)

hist(rstudent(albumSales3_model))


########### Logistic Regression ##############
library(car)
library(mlogit)

eelData <- read.delim("eel.dat", header = TRUE)
head(eelData)

eelData$Cured <- relevel(eelData$Cured, "Not Cured")
eelData$Intervention <- relevel(eelData$Intervention, "No Treatment")

eelModel.1 <- glm(Cured ~ Intervention, data = eelData, family = binomial() )
eelModel.2 <- glm(Cured ~ Intervention + Duration, data = eelData, family = binomial() )
summary(eelModel.1)
summary(eelModel.2)

modelChi <- eelModel.1$null.deviance - eelModel.1$deviance

modelChi

chidf <- eelModel.1$df.null - eelModel.1$df.residual
chidf

chisq.prob <- 1 - pchisq(modelChi, chidf)
chisq.prob


R2.h1 <- modelChi/eelModel.1$null.deviance

R2.h1

R.cs <- 1 - exp((eelModel.1$deviance - eelModel.1$null.deviance)/ 113)

R.cs

R.n <- R.cs / (1 - exp(-(eelModel.1$null.deviance/113)))
R.n

logisticPseudoR2s <- function(logModel) {
  dev <- logModel$deiance
  nullDev <- logModel$null.diviance
  modelN <- length(logModel$fitted.values)
  R.l <- 1 - dev / nullDev
  R.cs <- 1 - exp(-(nullDev - dev) / modelN)
  R.n <- R.cs / (1 - (exp (-(nullDev/ modelN))))
  cat("Pseudo R^2 for logistic regression\n")
  cat("Hosmer and Lemeshow R^2 ", round(R.l, 3), "\n")
  cat("Cox and Snell R^2 ", round(R.cs, 3), "\n")
  cat("Nagelkerke R^2", round(R.n, 3), "\n")
}

logisticPseudoR2s(eelModel.1)

#######################################################

acs <- read.csv("acs_ny.csv", header = TRUE, stringsAsFactors = FALSE)
acs$Income <- with(acs,  FamilyIncome >= 150000)

ggplot(acs, aes(x = FamilyIncome)) + 
  geom_density(fill = "grey", color = "grey") +
  geom_vline(xintercept = 150000) +
  scale_x_continuous(label = multiple.dollar, limits = c(0, 1000000) )

head(acs)
Model_Income <- glm(Income ~ HouseCosts + NumWorkers + OwnRent + NumBedrooms + FamilyType , data = acs, family = binomial(link = "logit"))
summary(Model_Income)

invlogit <- function(x) {
  1/(1 + exp(-x))
}

invlogit(Model_Income$coefficients)


ggplot(acs, aes(NumChildren)) + geom_histogram(binwidth = 1)

Model_Children <- glm(NumChildren ~ FamilyIncome + FamilyType + OwnRent, data = acs, family = poisson(link = "log"))
summary(Model_Children)


z <- (acs$NumChildren - Model_Children$fitted.values)/sqrt(Model_Children$fitted.values)
## Over dispersion factor

sum(z^2)/Model_Children$df.residual

pchisq(sum(z^2),Model_Children$df.residual )

Model_Children_1 <- glm(NumChildren ~ FamilyIncome + FamilyType + OwnRent, data = acs, family = quasipoisson(link = "log"))


#### Model Diagnostics ####

housing <- read.table("housing.csv", sep = ",", header = TRUE, stringsAsFactors = FALSE)

colnames(housing)
names(housing) <- c("Neighborhood", "Class", "Units", "YearBuilt", "SqFt", "Income", "IncomePerSqFt", "Expense", "ExpensePerSqFt", "NetIncome", "Value", "ValuePerSqFt", "Boro")
housing <- housing[housing$Units < 1000, ]
head(housing)

model_house <- lm(ValuePerSqFt ~ Units + SqFt  + Boro, data = housing)
summary(model_house)
head(fortify(model_house))

h1 <- ggplot(aes(x = .fitted, y = .resid), data = model_house) + 
  geom_point() +
  geom_hline(yintercept = 0) +
  geom_smooth(se = FALSE) + 
  labs(x = "Fitted Values", y = "Residuals")
h1

h1 + geom_point(aes(color = Boro))

plot(model_house, which = 1)

plot(model_house, which = 1, col = as.numeric(factor(model_house$model$Boro)))

legend("topright", legend = levels(factor(model_house$model$Boro)), pch = 1,
       col = as.numeric(factor(model_house$model$Boro)),
       text.col = as.numeric(factor(levels(factor(model_house$model$Boro)))), title = "Boro")


plot(model_house, which = 2)

ggplot(model_house, aes(sample = .stdresid)) + stat_qq() + geom_abline()

ggplot(model_house, aes(x = .resid)) + geom_histogram()
### Model Comparison #######
model_house_1 <- lm(ValuePerSqFt ~ Units + SqFt  + Boro, data = housing)
model_house_2 <- lm(ValuePerSqFt~ Units * SqFt + Boro, data = housing)
model_house_3 <- lm(ValuePerSqFt~ Units  + SqFt * Boro + Class, data = housing)
model_house_4 <- lm(ValuePerSqFt~ Units + SqFt * Boro + SqFt * Class, data = housing)
model_house_5 <- lm(ValuePerSqFt ~Boro + Class, data = housing)
###multiplot(model_house_1, model_house_2, model_house_3, model_house_4, model_house_5)
anova(model_house_1, model_house_2, model_house_3, model_house_4, model_house_5)

AIC(model_house_1, model_house_2, model_house_3, model_house_4, model_house_5)

BIC(model_house_1, model_house_2, model_house_3, model_house_4, model_house_5)
### Logistic Regression #####
housing$HighValue <-  housing$ValuePerSqFt > 150

model_high1 <- glm(HighValue ~ Units + SqFt + Boro, data = housing, family = binomial(link = "logit"))
model_high2 <- glm(HighValue ~ Units * SqFt + Boro, data = housing, family = binomial(link = "logit"))
model_high3 <- glm(HighValue ~ Units + SqFt * Boro + Class, data = housing, family = binomial(link = "logit"))
model_high4 <- glm(HighValue ~ Units + SqFt * Boro + SqFt * Class, data = housing, family = binomial(link = "logit"))
model_high5 <- glm(HighValue ~ Boro + Class, data = housing, family = binomial(link = "logit"))

anova(model_high1, model_high2, model_high3, model_high4, model_high5)
AIC(model_high1, model_high2, model_high3, model_high4, model_high5)
BIC(model_high1, model_high2, model_high3, model_high4, model_high5)


####### Cross Validation ########
library(boot)
model_house_GLM <- glm(ValuePerSqFt ~ Units + SqFt  + Boro, data = housing,  family = gaussian(link = "identity") )
model_house_LM <- lm(ValuePerSqFt ~ Units + SqFt  + Boro, data = housing )

identical(coef(model_house_LM),coef(model_house_GLM) )

model_house_cv1 <- cv.glm(housing,model_house_GLM, K = 5 ) 
model_house_cv$delta

model_house_GLM2 <- glm(ValuePerSqFt ~ Units * SqFt + Boro, data = housing, family = gaussian(link = "identity"))
model_house_GLM3 <- glm(ValuePerSqFt ~ Units + SqFt * Boro + Class, data = housing, family = gaussian(link = "identity"))
model_house_GLM4 <- glm(ValuePerSqFt ~ Units + SqFt * Boro + SqFt * Class, data = housing, family = gaussian(link = "identity"))
model_house_GLM5 <- glm(ValuePerSqFt ~ Boro + Class, data = housing, family = gaussian(link = "identity"))

model_house_cv2 <- cv.glm(housing,model_house_GLM2, K = 5 )
model_house_cv3 <- cv.glm(housing,model_house_GLM3, K = 5 )
model_house_cv4 <- cv.glm(housing,model_house_GLM4, K = 5 )
model_house_cv5 <- cv.glm(housing,model_house_GLM5, K = 5 )



cvResult <- as.data.frame(rbind(model_house_cv$delta,model_house_cv2$delta, model_house_cv3$delta, model_house_cv4$delta, model_house_cv5$delta))

names(cvResult) <- c("Error", "AdjustedError")
cvResult$Model <- sprintf("model_house_cv%s",1:5 )
cvResult


## Testing with ANOVA  #####
library(reshape2)
cvANOVA <- anova(model_house_GLM, model_house_GLM2, model_house_GLM3, model_house_GLM4, model_house_GLM5)
cvResult$ANOVA <- cvANOVA$`Resid. Df`
cvResult$AIC <- AIC(model_house_GLM, model_house_GLM2, model_house_GLM3, model_house_GLM4, model_house_GLM5)$AIC

cvMelt <- melt(cvResult, id.vars = "Model", variable.name = "Measure", value.name = "Value")
cvMelt

ggplot(cvMelt, aes(x = Model, y = Value)) +
  geom_line(aes(group = Measure, color = Measure)) +
  facet_wrap(~ Measure, scales = "free_y") +
  theme(axis.text.x = element_text(angle = 90, vjust = .5))+
  guides(color = FALSE)


### Step wise Selection #######

null_model <- lm(ValuePerSqFt ~ 1, data = housing)
full_model <- lm(ValuePerSqFt ~ Units + SqFt * Boro + Boro * Class, data = housing)

house_step <- step(null_model, scope = list(lower = null_model, upper = full_model), direction = "both")
house_step


