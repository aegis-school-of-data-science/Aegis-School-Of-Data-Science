airportsDB <- read.csv(file="F:/BA_BD/capstone project/Airline Data/DataSource/Airport_Lookup.csv",
                       header = TRUE, stringsAsFactors = FALSE)
carriersDB <- read.csv(file="F:/BA_BD/capstone project/Airline Data/DataSource/Carrier_Lookup.csv",
                       header = TRUE, stringsAsFactors = FALSE)
flightssDB <- read.csv(file="F:/BA_BD/capstone project/Airline Data/DataSource/CombinedFlights.csv",
                       header = TRUE, stringsAsFactors = FALSE)
head(flightssDB)
flightsDB <- subset(flightssDB, select = -c(X, YEAR, X.1))
summary(flightsDB$ARR_DELAY)
dim(flightsDB)
flightsDB <- na.omit(flightsDB)
summary(flightsDB)
holidays <- c('2014-01-01', '2014-01-20', '2014-02-17', '2014-05-26',
              '2014-07-04', '2014-09-01', '2014-10-13', '2013-11-11',
              '2013-11-28', '2013-12-25')
class(holidays)
holidayDates <- as.Date(holidays)
holidayDates
class(holidayDates)
datesOfYear <- unique(flightsDB[,1:2]) 
datesOfYear$HDAYS <- mapply(DaysToHoliday, datesOfYear$MONTH, datesOfYear$DAY_OF_MONTH) 

DaysToHoliday <- function(month, day)
  { 
  year <- 2014
  if (month > 10)
    {
    year <- 2013
  }
  # Paste on a 2013 for November and December dates.
  
  currDate <- as.Date(paste(YEAR,month,day,sep = '-')) # Create a DATE object we can use to calculate the time difference
  
  
  numDays <- as.numeric(min(abs(currDate-holidayDates))) # Now find the minimum difference between the date and our holidays
  return(numDays)                                        # We can vectorize this to automatically find the minimum closest
  # holiday by subtracting all holidays at once
  
}


