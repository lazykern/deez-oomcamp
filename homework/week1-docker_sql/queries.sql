--How many taxi trips were totally made on January 15?
SELECT COUNT(*) FROM trip WHERE DATE_PART('doy', lpep_pickup_datetime) = 15 AND DATE_PART('doy', lpep_dropoff_datetime) = 15

--Which was the day with the largest trip distance?
SELECT lpep_pickup_datetime AS pickup_datetime, SUM(trip_distance) AS total_trip_distance FROM trip GROUP BY lpep_pickup_datetime ORDER BY total_trip_distance DESC LIMIT 5;

--In 2019-01-01 how many trips had 2 and 3 passengers?
SELECT sq.passenger_count, COUNT(*) FROM (SELECT passenger_count FROM trip WHERE lpep_pickup_datetime >= '2019-01-01' AND lpep_pickup_datetime < '2019-01-02') as sq GROUP BY sq.passenger_count;

-- For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
SELECT do_zone.zone, SUM(tip_amount) AS total_tip FROM trip INNER JOIN taxi_zone pu_zone ON trip.pu_location_id = pu_zone.location_id INNER JOIN taxi_zone do_zone ON trip.do_location_id = do_zone.location_id WHERE pu_zone.zone = 'Astoria' GROUP BY do_zone.zone ORDER BY total_tip DESC LIMIT 5;
