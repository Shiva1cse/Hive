!connect jdbc:hive2://localhost:10000 root Ni76yd@933
set hive.execution.engine=tez;
/*Using the database HiveHW3*/
use HiveHW3;

/*To create the table for the reading the data from  the files and then creating the table.
To read the data for parsing the userAgent.*/
CREATE TABLE bids(
	BidID STRING,
	Time STRING, 
	LogType STRING,
	iPinYouID STRING, 
	Useragent STRING, 
	IP STRING, 
	RegionID STRING,
	CityID INT,
	AdExchange STRING,
	Domain STRING, 
	URL STRING, 
	AnonymousURL STRING, 
	AdSlotID STRING, 
	AdSlotWidth STRING,
	AdSlotHeight STRING, 
	AdSlotVisibility STRING, 
	AdSlotFormat STRING, 
	AdSlotFloorPrice STRING,
	CreativeID STRING, 
	BiddingPrice STRING,
	PayingPrice STRING,
	LandingPageURL STRING, 
	AdvertiserID STRING,
	UserProfileIDs STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
);
LOAD DATA INPATH '/user/shiva/imp' OVERWRITE INTO TABLE bids;

/*To map with the bids table for getting th cityname.*/
CREATE TABLE city_names (
	city_id INT,
	city_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
);
LOAD DATA INPATH '/user/shiva/city/city.en.txt' OVERWRITE INTO TABLE city_names;

/*Adding the jar for the parsing the userAgent.*/
ADD JAR /home/hive/Hive3-1.0-SNAPSHOT-jar.jar;
/*UDF for parsing the user data*/

/*Creating a function*/
CREATE TEMPORARY FUNCTION ParseUA as 'com.epam.bigdata.UserAgentDetail.UserAgentUDF';

DROP TABLE city_user_agents;
/* creating a table and get the userAgent different properties like ostype, browser,devices from the function created.
 Joining tables with the cityid in common.*/
CREATE TABLE city_user_agents
STORED AS ORC tblproperties ("orc.compress"="NONE") AS
SELECT
  prep_bids.city_name,
  p.*
FROM (
  SELECT
    CASE WHEN c.city_name IS NULL THEN 'unknown' ELSE c.city_name END AS city_name,
    b.useragent AS user_agent
  FROM bids b
  LEFT JOIN city_names c ON (c.city_id = b.cityid)
  WHERE b.useragent != null OR b.useragent != 'null'
) prep_bids
LATERAL VIEW parseUA(prep_bids.user_agent) p;


/*Get the devices based on the rank for the each city.*/
CREATE VIEW rank_wise_devices AS
  SELECT
    city_name,
    device,
    count(*) AS amount,
    RANK() OVER (PARTITION BY city_name ORDER BY COUNT(*) DESC) AS rank
  FROM city_user_agents
  GROUP BY city_name, device;


/*Get the os_types based on the rank for the each city.*/
CREATE VIEW rank_wise_os_types AS
  SELECT
    city_name,
    os_name,
    count(*) AS amount,
    RANK() OVER (PARTITION BY city_name ORDER BY COUNT(*) DESC) AS rank
  FROM city_user_agents
  GROUP BY city_name, os_name;


/*Get the browsers based on the rank for the each city.*/
CREATE VIEW rank_wise_browsers  AS
  SELECT
    city_name,
    browser,
    count(*) AS amount,
    RANK() OVER (PARTITION BY city_name ORDER BY COUNT(*) DESC) AS rank
  FROM city_user_agents
  GROUP BY city_name, browser;


/*Give the output which has the rank=1 for the devices, os, browsers for the each city*/
SELECT
  rank_wise_devices.city_name,
  rank_wise_devices.device,
  rank_wise_os_types.os_name,
  rank_wise_browsers.browser
FROM rank_wise_devices, rank_wise_os_types, rank_wise_browsers
WHERE
  rank_wise_devices.rank = 1 
  AND rank_wise_os_types.rank = 1 
  AND rank_wise_browsers.rank = 1 
  AND rank_wise_os_types.city_name = rank_wise_devices.city_name 
  AND rank_wise_browsers.city_name = rank_wise_devices.city_name;