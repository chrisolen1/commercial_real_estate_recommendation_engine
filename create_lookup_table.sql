USE zillow_oltp;

#Create crime-zone lookup table
CREATE TABLE `crime_zone_lkup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `results_key` int(51) NOT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `zone` int,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Create school-zone lookup table
CREATE TABLE `school_zone_lkup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `School_ID` int(51) NOT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `zone` int,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Create property-zone lookup table
CREATE TABLE `property_zone_lkup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `results_key` int(51) NOT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `zone` int,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Insert values into crime-zone lookup
INSERT INTO zillow_oltp.crime_zone_lkup(
    results_key,
    latitude,
    longitude,
    zone)
(SELECT 
	c.results_key,
    c.latitude,
    c.longitude,
    lookup.zone
FROM
    crime AS c
        LEFT JOIN
    (SELECT 
        c.results_key, z.zone
    FROM
        zone AS z, crime AS c
    WHERE
			c.latitude <= z.n_bound 
			AND c.latitude > z.s_bound
            AND c.longitude >= z.w_bound
            AND c.longitude < z.e_bound) AS lookup ON c.results_key = lookup.results_key
ORDER BY c.results_key);

#Insert values into school-zone lookup
INSERT INTO zillow_oltp.school_zone_lkup(
    School_ID,
    latitude,
    longitude,
    zone)
(SELECT 
	s.School_ID,
    s.School_Latitude,
    s.School_Longitude,
    lookup.zone
FROM
    school AS s
        LEFT JOIN
    (SELECT 
        s.School_ID, z.zone
    FROM
        zone AS z, school AS s
    WHERE
			 s.School_Latitude <= z.n_bound 
			AND  s.School_Latitude > z.s_bound
            AND s.School_Longitude >= z.w_bound
            AND s.School_Longitude < z.e_bound) AS lookup ON s.School_ID = lookup.School_ID
ORDER BY s.School_ID);

#Insert values into property-zone lookup
INSERT INTO zillow_oltp.property_zone_lkup(
    results_key,
    latitude,
    longitude,
    zone)
(SELECT 
	p.results_key,
    p.latitude,
    p.longitude,
    lookup.zone
FROM
    property AS p
        LEFT JOIN
    (SELECT 
        p.results_key, z.zone
    FROM
        zone AS z, property AS p
    WHERE
			p.latitude <= z.n_bound 
			AND p.latitude > z.s_bound
            AND p.longitude >= z.w_bound
            AND p.longitude < z.e_bound) AS lookup ON p.results_key = lookup.results_key
ORDER BY p.results_key);

#When values are added to property table, triggers line to be added to property-zone lookup table
DELIMITER $$
DROP TRIGGER IF EXISTS `new_property_added` $$
CREATE TRIGGER new_property_added 
AFTER INSERT ON property
FOR EACH ROW 
BEGIN
  INSERT INTO property_zone_lkup (results_key, latitude, longitude, zone)
	SELECT 
		NEW.results_key,
		NEW.latitude,
		NEW.longitude,
		lookup.zone
	FROM
		property AS p
			LEFT JOIN
		(SELECT 
			p.results_key, z.zone
		FROM
			zone AS z, property AS p
		WHERE
				p.latitude <= z.n_bound 
				AND p.latitude > z.s_bound
				AND p.longitude >= z.w_bound
				AND p.longitude < z.e_bound) AS lookup ON p.results_key = lookup.results_key
	WHERE p.results_key = new.results_key
    ORDER BY NEW.results_key;
END $$
DELIMITER ;

#Create transit-zone lookup table
CREATE TABLE `transit_zone_lkup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `results_key` int(51) NOT NULL,
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  `zone` int,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Insert values into transit-zone lookup
INSERT INTO zillow_oltp.transit_zone_lkup(
    results_key,
    latitude,
    longitude,
    zone)
(SELECT 
	c.results_key,
    c.latitude,
    c.longitude,
    lookup.zone
FROM
    transit AS c
        LEFT JOIN
    (SELECT 
        c.results_key, z.zone
    FROM
        zone AS z, transit AS c
    WHERE
			c.latitude <= z.n_bound 
			AND c.latitude > z.s_bound
            AND c.longitude >= z.w_bound
            AND c.longitude < z.e_bound) AS lookup ON c.results_key = lookup.results_key
ORDER BY c.results_key);