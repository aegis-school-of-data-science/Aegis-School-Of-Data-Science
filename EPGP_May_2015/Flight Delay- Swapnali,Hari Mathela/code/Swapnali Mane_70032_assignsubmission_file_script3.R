DaysToHoliday <- function(month, day)
{ 
year <- 2014
if (month > 10)
{
    year <- 2013
}
currDate <- as.Date(paste(year,month,day,sep = '-')) 
numDays <- as.numeric(min(abs(currDate-holidayDates))) 
return(numDays)                                        
}
data=DaysToHoliday(1,7)
datesOfYear <- unique(flightsDB[,1:2]) 
View(datesOfYear)
datesOfYear$HDAYS <- mapply(DaysToHoliday, datesOfYear$MONTH, datesOfYear$DAY_OF_MONTH) 
cbind(datesOfYear)
head(datesOfYear)
InputDays <- function(month,day)
  {
  finalDays <- datesOfYear$HDAYS[datesOfYear$MONTH == month & datesOfYear$DAY_OF_MONTH == day] # Find which row to get
  return(finalDays)
}
flightsDB$HDAYS <- mapply(InputDays, flightsDB$MONTH, flightsDB$DAY_OF_MONTH)
head(flightsDB)
flightsDB$ARR_HOUR <- trunc(flightsDB$CRS_ARR_TIME/100) 
flightsDB$DEP_HOUR <- trunc(flightsDB$CRS_DEP_TIME/100)
library(dplyr)
head(airportsDB)
subset(airportsDB, grepl('Chicago', Description))
MaxFlightsCode <- function(code)
  {
  
  codeFrame <- subset(flightsDB, ORIGIN_AIRPORT_ID == code)
  numFlights <- dim(codeFrame)[1]
  
  return(numFlights)
}
AirportCode <- function(city)
  {
  codes <- subset(airportsDB, grepl(city, Description))
  
  codes$NumFlights <- sapply(codes$Code, MaxFlightsCode)
  
  codes <- subset(codes, NumFlights == max(NumFlights))$Code
  
  return(codes)
}
AirportCode('Chicago')
GrouperFunc <- function(df, ...) df %>% regroup(list(...))
library(ggplot2)
AirPlot <- function(departure, arrival, groupon)
  {
  
  departCode <- AirportCode(departure)
  arriveCode <- AirportCode(arrival) 
  
  tempDB <- subset(flightsDB, ORIGIN_AIRPORT_ID == departCode & DEST_AIRPORT_ID == arriveCode) 
  grouped <- GrouperFunc(tempDB, groupon)                                                       
  summaryDF <- summarize(grouped, mean = mean(ARR_DELAY)) 
  
  
  
  finalBarPlot <- ggplot(summaryDF, aes_string(x=groupon, y='mean')) +
    geom_bar(color="black", width = 0.2, stat = 'identity') +
    guides(fill=FALSE)+
    xlab(groupon) + 
    ylab('Average Delay (minutes)')+
    ggtitle((paste('Flights from', departure, 'to', arrival)))
  
  return(finalBarPlot)
}

AirPlot('Dallas', 'Chicago', 'UNIQUE_CARRIER')
subset(carriersDB, grepl('^AA$|^OO$|^UA$|^YV$', Code))
AirPlot('Dallas', 'Chicago', 'MONTH')
AirPlot('Chicago', 'Dallas', 'MONTH')
AirPlot('Boston', 'Atlanta', 'DAY_OF_WEEK')
AirPlot('Boston', 'Atlanta', 'HDAYS')
AirPlot('Boston', 'Atlanta', 'DEP_HOUR')
AirPlot('Boston', 'Atlanta', 'ARR_HOUR')
flightsDB$CARRIER_CODE <- as.numeric(as.factor(flightsDB$UNIQUE_CARRIER))
head(flightsDB)
View(flightsDB)
numericDB <- select(flightsDB, -c(CRS_DEP_TIME, CRS_ARR_TIME))
head(numericDB)
write.csv(numericDB, 'FinalFlightsNumeric.csv')
getwd()
airport_lookupDF=read.csv(file="F:/BA_BD/capstone project/Airline Data/DataSource/Airport_Lookup.csv",
                          header = TRUE, stringsAsFactors = FALSE)
trainDF=read.csv('FinalFlightsNumeric.csv', header = TRUE)
head(trainDF)
View(trainDF)
carrierDF=trainDF[c("UNIQUE_CARRIER","CARRIER_CODE")]
carrierDF=unique(carrierDF)
head(carrierDF)
trainDF=subset(trainDF,select=-UNIQUE_CARRIER)
HdaysDF=trainDF[c("MONTH","DAY_OF_MONTH","HDAYS")]
HdaysDF=unique(HdaysDF)
class(trainDF$DISTANCE)
class(trainDF$HDAYS)
scalingDF =trainDF[c("DISTANCE","HDAYS")]
scalingDF=scale(scalingDF)
categDF = trainDF[c("MONTH", "DAY_OF_MONTH", "ORIGIN_AIRPORT_ID", 
                   "DEST_AIRPORT_ID", "ARR_HOUR","DEP_HOUR", 
                   "CARRIER_CODE", "DAY_OF_WEEK")] 
scalingDF = as.factor(trainDF[c("DISTANCE","HDAYS")])
class(scalingDF)
class(categDF)
head(scalingDF)
head(categDF)
str(categDF)
str(scalingDF)


