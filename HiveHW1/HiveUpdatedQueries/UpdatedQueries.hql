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