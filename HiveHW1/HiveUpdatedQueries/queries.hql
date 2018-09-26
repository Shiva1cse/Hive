/*To connect to the beeline*/
!connect jdbc:hive2://localhost:10000 root Ni76yd@933

/*Drop the database if already existed.*/
drop database IF EXISTS HiveHW1;

/*Creating the database HiveHW1*/
Create database HiveHW1;

/*Using the currently created database*/
use HiveHW1;

/*Drop the tables if they exist*/
drop table IF EXISTS airports;
drop table IF EXISTS carriers;
drop table IF EXISTS flights;
drop table IF EXISTS airports_temp;
drop table IF EXISTS carriers_temp;
drop table IF EXISTS flights_temp;

/*Creating the temporary table for the airports. This table will have the details of the  iata, airport, city, state, country, lat, long
airports Loading the airports.csv to the table. This can be joined with the flights and carriers tables*/
CREATE TABLE airports_temp (
  iata string,
  airport string,
  city string,
  state string,
  country string,
  lat string,
  long string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)STORED as TEXTFILE
 tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH '/user/shiva/HiveHW1/airports.csv' overwrite into table airports_temp;

/*Creating the table for the airports. Moving table data from airports_temp to airports*/
create table airports stored as orc
LOCATION '/user/hiveHW1/airports' as 
SELECT
  *
FROM airports_temp;

/*Drop table airports_temp*/
drop table airports_temp;



/*Creating the temporary table for the carriers. The fields are Code ,
  Description.  Loading the carriers.csv to the table. This can be joined with the flights and airports tables*/
CREATE TABLE carriers_temp (
  Code string,
  Description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH '/user/shiva/HiveHW1/carriers.csv' overwrite into table carriers_temp;

/*Creating the table for the carriers. Moving table data from carriers_temp to carriers*/
create table carriers stored as orc
LOCATION '/user/hiveHW1/carriers' as 
SELECT
  *
FROM carriers_temp;

/*Drop table carriers_temp*/
drop table carriers_temp;



/*Creating the temporary table for the carriers.   Year int,  Month ,  DayofMonth ,DayOfWeek ,DepTime ,CRSDepTime ,ArrTime ,CRSArrTime ,UniqueCarrier ,FlightNum ,TailNum , ActualElapsedTime ,CRSElapsedTime ,AirTime ,ArrDelay ,
  DepDelay , Origin ,Dest ,Distance ,TaxiIn ,TaxiOut ,Cancelled ,CancellationCode ,Diverted ,CarrierDelay ,WeatherDelay ,NASDelay ,SecurityDelay ,LateAircraftDelay. Loading the 2007.csv to the table.
  This table will have all details of the flights. This can be joined with the carriers and airports tables*/
CREATE TABLE flights_temp (
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode string,
  Diverted int,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH '/user/shiva/HiveHW1/2007.csv' overwrite into table flights_temp;

/*Creating the table for the flights. Moving table data from flights_temp to carriers*/
create table flights stored as orc
LOCATION '/user/hiveHW1/flights' as 
SELECT
  *
FROM flights_temp;

/*Drop table carriers.*/
drop table flights_temp;



/*Using the created HiveHW1 database*/
use HiveHW1;

/*Counting total number of flights per carrier in 2007.
 Searching with year 2007 and grouping by UniqueCarrier*/
SELECT
  carrier.description,
  flight.flight_count
FROM (SELECT
  UniqueCarrier,
  COUNT(*) AS flight_count
FROM flights
WHERE year = 2007
AND cancelled != 1
GROUP BY UniqueCarrier) flight
JOIN carriers carrier
  ON carrier.code = flight.UniqueCarrier;


/*The total number of flights served in Jun 2007 by NYC. finding the flights origin
  and destination for NYC during the year 2007 and 6 th month*/
SELECT
  COUNT(*) count
FROM (SELECT
  *
FROM flights
WHERE cancelled != 1
AND year = 2007
AND month = 6) flight
JOIN airports airport1
  ON airport1.iata = flight.origin
JOIN airports airport2
  ON airport2.iata = flight.dest
WHERE (airport1.city = 'New York'
OR airport2.city = 'New York');



/*Five most busy airports in US during Jun 01 - Aug 31. Finding the
 top 5 busy airports in US between the period of month 6 and 8*/
SELECT
  airport1.airport,
  SUM(flight1.count) total
FROM (SELECT
  airport.iata,
  COUNT(*) count
FROM flights flight
JOIN airports airport
  ON airport.iata = flight.origin
WHERE (cancelled != 1
AND flight.year = 2007
AND flight.month BETWEEN 6 AND 8)
GROUP BY airport.iata
UNION ALL
SELECT
  airport.iata,
  COUNT(*) cnt
FROM flights flight
JOIN airports airport
  ON airport.iata = flight.dest
WHERE (cancelled != 1
AND flight.year = 2007
AND flight.month BETWEEN 6 AND 8)
GROUP BY airport.iata) flight1
JOIN airports airport1
  ON airport1.iata = flight1.iata
GROUP BY airport1.airport
ORDER BY total
 desc limit 5;
 
 
 
 
/*Carrier who served the biggest number of flights. grouping by the
 UniqueCarrier finding count and then printing the top 1st one.*/

 SELECT
  carrier.description,
  flight.flight_count
FROM (SELECT
  UniqueCarrier,
  COUNT(*) AS flight_count
FROM flights
WHERE cancelled != 1
GROUP BY UniqueCarrier) flight
JOIN carriers carrier
  ON carrier.code = flight.UniqueCarrier
ORDER BY flight.flight_count DESC limit 1;