-- TODO: I'm not sure if there's a pythonic way to do migrations, but this
-- should serve as a record of what I expect the database to look like:

CREATE TABLE weather_readings (
  light INT NOT NULL
  , humidity INT NOT NULL
  , temp_celcius INT NOT NULL
  , heat_index NUMERIC(5, 3) NOT NULL
  , capture_dttm INT NOT NULL
  , device varchar(200) NOT NULL
);

CREATE UNIQUE INDEX inx_capture_dttm_and_device
  ON weather_readings(capture_dttm, device);

ALTER TABLE weather_readings ADD COLUMN pressure_pa INT NOT NULL DEFAULT -99;
ALTER TABLE weather_readings ADD COLUMN baro_temp_celcius INT NOT NULL DEFAULT -99;
ALTER TABLE weather_readings ALTER COLUMN heat_index TYPE NUMERIC(8, 5);

-- 2017-07-30
ALTER TABLE weather_readings ADD COLUMN mq135 INT NOT NULL DEFAULT -99;
ALTER TABLE weather_readings ADD COLUMN mq5 INT NOT NULL DEFAULT -99;
ALTER TABLE weather_readings ADD COLUMN mq6 INT NOT NULL DEFAULT -99;
ALTER TABLE weather_readings ADD COLUMN mq9 INT NOT NULL DEFAULT -99;
