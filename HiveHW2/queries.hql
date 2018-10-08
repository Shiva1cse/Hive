/*To connect to the beeline*/
!connect jdbc:hive2://localhost:10000 root Ni76yd@933

/*Drop the database if already existed.*/
drop database IF EXISTS HiveHW2;

/*Creating the database HiveHW2*/
Create database HiveHW2;

/*Using the currently created database*/
use HiveHW2;

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
LOAD DATA INPATH '/user/shiva/HiveHW2/airports.csv' overwrite into table airports_temp;

/*Creating the table for the airports. Moving table data from airports_temp to airports*/
create table airports stored as orc
LOCATION '/user/hiveHW2/airports' as 
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
LOAD DATA INPATH '/user/shiva/HiveHW2/carriers.csv' overwrite into table carriers_temp;

/*Creating the table for the carriers. Moving table data from carriers_temp to carriers*/
create table carriers stored as orc
LOCATION '/user/hiveHW2/carriers' as 
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
LOAD DATA INPATH '/user/shiva/HiveHW2/2007.csv' overwrite into table flights_temp;

/*Creating the table for the flights. Moving table data from flights_temp to carriers*/
create table flights stored as orc
LOCATION '/user/hiveHW2/flights' as 
SELECT
  *
FROM flights_temp;

/*Drop table carriers.*/
drop table flights_temp;



/*Using the currently created database*/
use HiveHW2;


/*carriers who cancelled more than 1 flights during 2007, order them from biggest to lowest by number 
of cancelled flights and list in each record all departure cities where cancellation happened. Append all the cities as a set.*/
SELECT
  carrier.description AS description,
  Canceled.Canceled_flights AS Canceled_Count,
  Canceled.Cities AS cities
FROM (SELECT
  f.UniqueCarrier AS uniqueCarrier,
  COUNT(*) AS Canceled_flights,
  concat_ws(',', collect_set(a.airport)) AS Cities
FROM flights f
JOIN airports a
  ON (f.dest = a.iata
  AND f.cancelled = 1)
GROUP BY f.UniqueCarrier
HAVING COUNT(*) > 1
ORDER BY Canceled_flights DESC) Canceled
JOIN carriers carrier
  ON Canceled.uniqueCarrier = carrier.code;
