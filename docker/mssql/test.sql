/*create database*/
CREATE DATABASE mydb;
GO

/*use a database*/
USE mydb;
GO

/*create table*/

CREATE TABLE airlines (MonthDayYear varchar(255), DayOfWeek int, DepTime varchar(255), ArrTime varchar(255), Carrier varchar(255), FlightNum int, TailNum varchar(255), AirTime varchar(255), ArrDelay varchar(255), DepDelay varchar(255), Origin varchar(255), Destination varchar(255), Distance int, TaxiIn int, TaxiOut int);
GO

/*run bulk import to import data from csv file from netstore*/
BULK INSERT mydb.dbo.airlines FROM '/netstore/datasets/XcalarTraining/airlines.csv' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a' )
GO


