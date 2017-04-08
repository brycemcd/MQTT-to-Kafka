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
