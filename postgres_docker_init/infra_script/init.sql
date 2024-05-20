-- create a  schema
CREATE SCHEMA IF NOT EXISTS OLYMPIC;

-- create and populate table
CREATE TABLE IF NOT EXISTS OLYMPIC.ATHLETE_EVENTS
(
    id INT, --Unique number for each athlete
	name VARCHAR,
	sex VARCHAR,
	age VARCHAR,
	height VARCHAR,
	weight VARCHAR,
	team VARCHAR,
	noc VARCHAR,
	games VARCHAR,
	year INT,
	season VARCHAR,
	city VARCHAR,
	sport VARCHAR,
	event VARCHAR,
	medal VARCHAR
);

COPY OLYMPIC.ATHLETE_EVENTS (id, name, sex, age, height, weight, team, noc, games, year, season, city, sport, event, medal)
FROM '/data/athlete_events.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS OLYMPIC.NOC_REGIONS
(
	noc VARCHAR,
	region VARCHAR,
	notes VARCHAR
);

COPY OLYMPIC.NOC_REGIONS (noc, region, notes)
FROM '/data/noc_regions.csv' DELIMITER ',' CSV HEADER;