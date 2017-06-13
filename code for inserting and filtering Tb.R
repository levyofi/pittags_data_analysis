# R code for Tb data filtration 
# 
# Author: Ofir Levy
# Email:  levyofi@gmail.com
###############################################################################


#######################################################
## This function creates the necessary database tables         
#######################################################
build.tables<-function(){
	#create a table that will hold individuals data including the Tb calibration curves
	dbGetQuery(database, "CREATE TABLE  `example`.`INDIVIDUALS` (
					`IN_ID` tinyint(4) NOT NULL, 
					`IN_GENDER` varchar(7) NOT NULL,
					`IN_WEIGHT` tinyint(4) default '0',
					`IN_HZ` float default NULL,
					`IN_A` float default NULL,
					`IN_B` float default NULL,
					`IN_ENCLOSURE` tinyint(1) NOT NULL,
					PRIMARY KEY  (`IN_ID`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1")
	
	#create a table that will hold the Tb raw data
	dbGetQuery(database, "CREATE TABLE  `example`.`TEMPERATURE` (
					`TM_ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
					`TM_DATE` date default NULL,
					`TM_TIME` time default NULL,
					`TM_HZ` float default NULL,
					`TM_PULSE` int(10) unsigned default NULL,
					`TM_TEMPERATURE` float default NULL,
					`TM_POWER` int(11) default NULL,
					`TM_ANTENNA` varchar(4) default NULL,
					PRIMARY KEY  (`TM_ID`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1")
	
	#create a table that will hold the Tb data after filtration
	dbGetQuery(database, "CREATE TABLE  `example`.`CLEAN_TABLE` (
					`CT_ID` int(11) NOT NULL AUTO_INCREMENT,
					`CT_DATE` date NOT NULL,
					`CT_TIME` time NOT NULL,
					`CT_HZ` float NOT NULL,
					`CT_TEMPERATURE` float NOT NULL,
					`CT_POWER` int(11) NOT NULL,
					`CT_ANTENNA` varchar(5) NOT NULL,
					PRIMARY KEY  (`CT_ID`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1")
}

##########################################################################################################################################
## This function enters the individuals data into the database. 
## The data is read from a text file.
## For example:
## 			"IN_ID","IN_GENDER","IN_WEIGHT","IN_HZ","IN_A","IN_B","IN_ENCLOSURE"
##  		33,"female",40,150.478,-0.01,50.63,1
##  		34,"female",38,150.37,-0.0081,46.58,1
##  		4,"female",44,150.35,-0.0086,46.93,1
## "IN_HZ" column is the frequency of the implanted transmitter. "IN_A" and "IN_B" columns are the slope and intercept of the calibration 
##	curve for the specific transmitter that converts the pulse interval to temperature
##########################################################################################################################################
input.individuals<-function(){
	ind.data<-read.table("INDIVIDUALS.csv", header=T, sep=",")
	dbWriteTable(database, "INDIVIDUALS", ind.data,  row.names = F, append=T)
}

##########################################################################################################################################
## This function enters the raw Tb data into the database.
## The data is read from a text file.
## For example:
##		080126 22:27,55 150.858   1069  -132  L
##		080126 22:27,57 150.858   2061  -136  R
##		080126 22:28,08 150.858   1074  -133  L
##		080126 22:28,10 150.858   1500  -133  R
##		080126 22:28,45 150.858   1077  -133  L
##		080126 22:28,46 150.858   1286  -133  R
##		080126 22:35,52 150.350   1454  -126  L
##		080126 22:35,54 150.350   2260  -137  R
##		080126 22:36,06 150.350   1453  -127  L
##		080126 22:36,08 150.350   2178  -136  R
##		080126 22:36,20 150.350   1454  -126  L
## Where the first column is the date in 'ymd' format, and the rest of the columns are time, transmitter frequency, pulse interval, signal
## strength and antenna location (right or left). 
## If several log files exist, the function should be called for each file or all files should be unified to one file.
##########################################################################################################################################
input.Tb<-function(log.file.name){
	tb.data<-read.table(log.file.name, header=F)	
	#give meaningful names to columns
	names(tb.data)<-c("TM_DATE","TM_TIME", "TM_HZ","TM_PULSE", "TM_POWER", "TM_ANTENNA")
	
	#convert date and time text fields to date and time class objects
	library(chron)
	#add a leading zero to the date so the year will be 08 instead of just 8
	tb.data$TM_DATE<-paste("0", tb.data$TM_DATE, sep="")
	#convert from text to date class
	tb.data$TM_DATE<-dates(tb.data$TM_DATE, format="ymd")
	#convert from text to time class
	tb.data$TM_TIME<-sub(",", ":", tb.data$TM_TIME)
	tb.data$TM_TIME<-times(tb.data$TM_TIME)
	
	#enter the data to the database - to avoid using a mysql INSERT command on each row we first insert all data in one command into a temporary table and use a mysql INSERT INTO...SELECT command on this temporary table to create the TEMPERATURE table
	#insert the data into a temporary table
	dbWriteTable(database, "TEMP_TEMPERATURE", tb.data,  row.names = F, append=F, overwrite=T)
	#calculate the actual Tb and enter Tb data to the TEMPERATURE table
	dbGetQuery(database, "INSERT INTO TEMPERATURE (TM_DATE, TM_TIME, TM_HZ, TM_PULSE, TM_POWER, TM_ANTENNA, TM_TEMPERATURE) SELECT STR_TO_DATE(TM_DATE, '%m/%d/%y') AS TM_DATE, TIME(TM_TIME) AS TM_TIME, TM_HZ, TM_PULSE, TM_POWER, TM_ANTENNA, (IN_A*TM_PULSE+IN_B) AS TM_TEMPERATURE FROM example.TEMP_TEMPERATURE, example.INDIVIDUALS WHERE ABS(TM_HZ-IN_HZ)<0.005")
}

##########################################################################################################################################
## This function reads the raw Tb data starting from a certain row index. 
## For each 60 seconds of recording from the same individual (transmitter), this function returns the Tb log with the highest signal strength  
##########################################################################################################################################
getNextRecordsSession<-function(start.index, tb.raw){ 
	index<-start.index
	record.time<-tb.raw[index,]$time
	record.hz<-tb.raw[index,]$TM_HZ
	selected.row<-tb.raw[index,]
	index<-index+1		
	#scan all records from the same individual and the same minute and look for the record with the highest signal strength
	while ((index<nrow(tb.raw)) & (difftime(tb.raw[index,]$time,record.time,units="secs")<60) & (record.hz==tb.raw[index,]$TM_HZ)){
		if (selected.row$TM_POWER<tb.raw[index,]$TM_POWER){
			selected.row<-tb.raw[index,]
		}		
		index<-index+1
	}	
	#return the row with the last index from that individual
	selected.row$last.index<-index
	
	#add filtering parameters 
	selected.row$prev.tb.ok<-TRUE
	selected.row$next.tb.ok<-TRUE
	
	#return the selected row
	selected.row
}

#################################################################################
## This function filters the data and return the clean data without Tb logs that 
## are differ from their previous and subsequent logs by more than 7°C
#################################################################################
filter.Tb<-function(){
	#get the raw Tb data sorted by individual, day, and time
	tb.raw<-dbGetQuery(database, "SELECT * FROM TEMPERATURE ORDER BY TM_HZ,TM_DATE,TM_TIME")
	tb.raw$time<-chron(tb.raw$TM_DATE, tb.raw$TM_TIME,format = c(dates = "Y-m-d", times = "h:m:s"))
	#remove rows with unspecified time which may occur when there are errors in the log file
	tb.raw<-tb.raw[!is.na(tb.raw$time),]
	#start filtering the data and enter the clean data into `cleaned.logs` dataset	
	previous.log<-getNextRecordsSession(1, tb.raw)#this Tb log will be compared to its subsequent log
	cleaned.logs<-previous.log #enter the first log to the cleaned.logs dataset
	current.log<-getNextRecordsSession(previous.log$last.index, tb.raw)
	#scan all logs, and compare each log to its previous and subsequent logs 
	while (current.log$last.index<nrow(tb.raw)){
		subsequent.log<-getNextRecordsSession(current.log$last.index, tb.raw)
		if (abs(previous.log$TM_TEMPERATURE-current.log$TM_TEMPERATURE)>7){
			current.log$prev.tb.ok<-FALSE
		}
		if (abs(subsequent.log$TM_TEMPERATURE-current.log$TM_TEMPERATURE)>7){
			current.log$next.tb.ok<-FALSE
		}	
		if ((current.log$prev.tb.ok)|(current.log$next.tb.ok)){
			cleaned.logs<-rbind(cleaned.logs,current.log)
		}
		previous.log<-current.log
		current.log<-subsequent.log
	}
	cleaned.logs
}

##### MAIN CODE #######

#connect to the database
library(RMySQL)
database<- dbConnect(MySQL(), user="example", password="example",  dbname="example", host="localhost")

#use 'chron' library to work with date and time values
library(chron)

#create the tables and enter the individuals data and the raw Tb data into the database
build.tables()
input.individuals()
input.Tb("example Tb.log")

#get the filtered Tb data
cleaned.logs<-filter.Tb()

#enter the data to the database - to avoid using a mysql INSERT command on each row we first insert all data in one command into a temporary table and use a mysql INSERT INTO...SELECT command on this temporary table to create the CLEAN_TABLE table
#insert the filtered Tb data into a temporary database table
dbWriteTable(database, "TEMP_TEMPERATURE", cleaned.logs,  row.names = F, append=F, overwrite=T)
#insert the relevant fields into the CLEAN_TABLE database table
dbGetQuery(database, "INSERT INTO CLEAN_TABLE (CT_DATE, CT_TIME, CT_HZ, CT_POWER, CT_ANTENNA, CT_TEMPERATURE) SELECT TM_DATE, TM_TIME, TM_HZ, TM_POWER, TM_ANTENNA, TM_TEMPERATURE FROM example.TEMP_TEMPERATURE T")
