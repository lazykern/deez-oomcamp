CREATE TABLE IF NOT EXISTS taxi_zone (
  location_id INT PRIMARY KEY,
  borough VARCHAR(255),
  Zone VARCHAR(255),
  service_zone VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS trip (
  vendor_id SMALLINT,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag CHAR(1),
  ratecode_id SMALLINT,
  pu_location_id INT,
  do_location_id INT,
  passenger_count INT,
  trip_distance FLOAT(2),
  fare_amount FLOAT(2),
  extra FLOAT(2),
  mta_tax FLOAT(2),
  tip_amount FLOAT(2),
  tolls_amount FLOAT(2),
  ehail_fee FLOAT(2),
  improvement_surcharge FLOAT(2),
  total_amount FLOAT(2),
  payment_type SMALLINT,
  trip_type SMALLINT,
  congestion_surcharge FLOAT(2),
  CONSTRAINT fk_pu_location_id FOREIGN KEY (pu_location_id) REFERENCES taxi_zone(location_id),
  CONSTRAINT fk_do_location_id FOREIGN KEY (do_location_id) REFERENCES taxi_zone(location_id)
);
