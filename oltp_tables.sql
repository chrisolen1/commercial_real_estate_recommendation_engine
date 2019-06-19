-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema zillow_oltp
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema zillow_oltp
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `zillow_oltp` DEFAULT CHARACTER SET utf8mb4 ;
USE `zillow_oltp` ;

-- -----------------------------------------------------
-- Table `zillow_oltp`.`business_test_2`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`business_test_2` (
  `zone_sic_key_test` INT(10) NOT NULL AUTO_INCREMENT,
  `zone_sic` VARCHAR(20) NOT NULL,
  `zone` INT(11) NOT NULL,
  `sic` VARCHAR(8) NOT NULL,
  `sic_description` VARCHAR(150) NOT NULL,
  `avg_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `n_bus` INT(10) NOT NULL,
  PRIMARY KEY (`zone_sic_key_test`))
ENGINE = InnoDB
AUTO_INCREMENT = 16558
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `zillow_oltp`.`business_train_2`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`business_train_2` (
  `zone_sic_key_train` INT(10) NOT NULL AUTO_INCREMENT,
  `zone_sic` VARCHAR(20) NOT NULL,
  `zone` INT(11) NOT NULL,
  `sic` VARCHAR(8) NOT NULL,
  `sic_description` VARCHAR(150) NOT NULL,
  `avg_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_revenue` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_emp_total` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_emp_here` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_age` DECIMAL(20,2) NULL DEFAULT NULL,
  `avg_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `median_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `sd_sqft` DECIMAL(20,2) NULL DEFAULT NULL,
  `n_bus` INT(10) NOT NULL,
  PRIMARY KEY (`zone_sic_key_train`))
ENGINE = InnoDB
AUTO_INCREMENT = 70344
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `zillow_oltp`.`crime`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`crime` (
  `results_key` INT(11) NOT NULL AUTO_INCREMENT,
  `id` INT(11) NULL DEFAULT NULL,
  `case_number` VARCHAR(15) NULL DEFAULT NULL,
  `date` DATETIME NULL DEFAULT NULL,
  `year` INT(4) NULL DEFAULT NULL,
  `primary_type` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(150) NULL DEFAULT NULL,
  `domestic` TINYINT(1) NULL DEFAULT NULL,
  `arrest` TINYINT(1) NULL DEFAULT NULL,
  `beat` INT(6) NULL DEFAULT NULL,
  `block` VARCHAR(45) NULL DEFAULT NULL,
  `community_area` INT(3) NULL DEFAULT NULL,
  `ward` INT(3) NULL DEFAULT NULL,
  `district` INT(5) NULL DEFAULT NULL,
  `latitude` FLOAT(14,10) NULL DEFAULT NULL,
  `longitude` FLOAT(14,10) NULL DEFAULT NULL,
  `location_description` VARCHAR(150) NULL DEFAULT NULL,
  `fbi_code` VARCHAR(5) NULL DEFAULT NULL,
  `iucr` VARCHAR(5) NULL DEFAULT NULL,
  `entered_on` DATETIME NULL DEFAULT NULL,
  `x_coordinate` INT(9) NULL DEFAULT NULL,
  `y_coordinate` INT(9) NULL DEFAULT NULL,
  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`results_key`))
ENGINE = InnoDB
AUTO_INCREMENT = 257122
DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------
-- Table `zillow_oltp`.`grid_census_final_2`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`grid_census_final_2` (
  `zone` INT(11) NULL DEFAULT NULL,
  `n_bound` DOUBLE NULL DEFAULT NULL,
  `s_bound` DOUBLE NULL DEFAULT NULL,
  `w_bound` DOUBLE NULL DEFAULT NULL,
  `e_bound` DOUBLE NULL DEFAULT NULL,
  `NW ZIP` TEXT NULL DEFAULT NULL,
  `NE ZIP` TEXT NULL DEFAULT NULL,
  `SW ZIP` TEXT NULL DEFAULT NULL,
  `SE ZIP` TEXT NULL DEFAULT NULL,
  `median_total` DOUBLE NULL DEFAULT NULL,
  `ave_household_size_total` DOUBLE NULL DEFAULT NULL,
  `median_male_age` DOUBLE NULL DEFAULT NULL,
  `median_female_age` DOUBLE NULL DEFAULT NULL,
  `white_total` DOUBLE NULL DEFAULT NULL,
  `black_total` DOUBLE NULL DEFAULT NULL,
  `native_total` DOUBLE NULL DEFAULT NULL,
  `asian_total` DOUBLE NULL DEFAULT NULL,
  `hawaiian_total` DOUBLE NULL DEFAULT NULL,
  `other_alone_total` DOUBLE NULL DEFAULT NULL,
  `two_or_more_total` DOUBLE NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `zillow_oltp`.`property`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`property` (
  `results_key` INT(11) NOT NULL AUTO_INCREMENT,
  `zpid` INT(15) NOT NULL,
  `street` VARCHAR(50) NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `zipcode` VARCHAR(15) NULL DEFAULT NULL,
  `latitude` FLOAT(11,8) NULL DEFAULT NULL,
  `longitude` FLOAT(11,8) NULL DEFAULT NULL,
  `zestimate` FLOAT(11,2) NULL DEFAULT NULL,
  `valueChange` FLOAT(11,2) NULL DEFAULT NULL,
  `low_estimate` FLOAT(11,2) NULL DEFAULT NULL,
  `high_estimate` FLOAT(11,2) NULL DEFAULT NULL,
  `neighborhood` VARCHAR(100) NULL DEFAULT NULL,
  `neighborhood_zindex_value` FLOAT(11,2) NULL DEFAULT NULL,
  `hometype` VARCHAR(50) NULL DEFAULT NULL,
  `assessmentYear` INT(4) NULL DEFAULT NULL,
  `assessment` FLOAT(11,2) NULL DEFAULT NULL,
  `yearBuilt` INT(4) NULL DEFAULT NULL,
  `lotSize` INT(7) NULL DEFAULT NULL,
  `houseSize` INT(7) NULL DEFAULT NULL,
  `bathrooms` FLOAT(5,2) NULL DEFAULT NULL,
  `bedrooms` INT(3) NULL DEFAULT NULL,
  `lastSold` DATE NULL DEFAULT NULL,
  `lastSoldPrice` FLOAT(11,2) NULL DEFAULT NULL,
  `grid_num` INT(11) NULL DEFAULT NULL,
  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`results_key`))
ENGINE = InnoDB
AUTO_INCREMENT = 255617
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `zillow_oltp`.`school`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`school` (
  `School_ID` INT(10) NOT NULL,
  `Long_Name` VARCHAR(100) NOT NULL,
  `School_Type` VARCHAR(30) NOT NULL,
  `Primary_Category` VARCHAR(3) NOT NULL,
  `Address` VARCHAR(50) NOT NULL,
  `City` VARCHAR(20) NOT NULL,
  `State` VARCHAR(20) NOT NULL,
  `Zip` INT(10) NOT NULL,
  `Blue_Ribbon_Award_Year` INT(10) NULL DEFAULT NULL,
  `Excelerate_Award_Gold_Year` INT(10) NULL DEFAULT NULL,
  `Spot_Light_Award_Year` INT(10) NULL DEFAULT NULL,
  `Improvement_Award_Year` INT(10) NULL DEFAULT NULL,
  `Excellence_Award_Year` INT(10) NULL DEFAULT NULL,
  `Student_Growth_Rating` VARCHAR(50) NULL DEFAULT NULL,
  `Growth_Reading_Grades_Tested_Pct_ES` INT(10) NULL DEFAULT NULL,
  `Growth_Math_Grades_Tested_Pct_ES` INT(10) NULL DEFAULT NULL,
  `Student_Attainment_Rating` VARCHAR(50) NULL DEFAULT NULL,
  `Attainment_Reading_Pct_ES` INT(10) NULL DEFAULT NULL,
  `Attainment_Math_Pct_ES` INT(10) NULL DEFAULT NULL,
  `Culture_Climate_Rating` VARCHAR(50) NULL DEFAULT NULL,
  `Creative_School_Certification` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Involved_Families` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Supportive_Environment` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Ambitious_Instruction` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Effective_Leaders` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Collaborative_Teachers` VARCHAR(50) NULL DEFAULT NULL,
  `School_Survey_Safety` VARCHAR(50) NULL DEFAULT NULL,
  `Suspensions_Per_100_Students_Year_2_Pct` DECIMAL(10,2) NULL DEFAULT NULL,
  `Misconducts_To_Suspensions_Year_2_Pct` DECIMAL(10,2) NULL DEFAULT NULL,
  `Student_Attendance_Year_2_Pct` DECIMAL(10,2) NULL DEFAULT NULL,
  `Graduation_4_Year_School_Pct_Year_2` DECIMAL(10,2) NULL DEFAULT NULL,
  `College_Enrollment_School_Pct_Year_2` DECIMAL(10,2) NULL DEFAULT NULL,
  `College_Persistence_School_Pct_Year_2` DECIMAL(10,2) NULL DEFAULT NULL,
  `School_Latitude` DECIMAL(30,6) NULL DEFAULT NULL,
  `School_Longitude` DECIMAL(30,6) NULL DEFAULT NULL,
  PRIMARY KEY (`School_ID`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

-- -----------------------------------------------------
-- Table `zillow_oltp`.`transit`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`transit` (
  `results_key` INT(11) NOT NULL AUTO_INCREMENT,
  `stop_id` INT(11) NULL DEFAULT NULL,
  `direction_id` VARCHAR(12) NULL DEFAULT NULL,
  `stop_name` VARCHAR(100) NULL DEFAULT NULL,
  `blue` TINYINT(1) NULL DEFAULT NULL,
  `brn` TINYINT(1) NULL DEFAULT NULL,
  `g` TINYINT(1) NULL DEFAULT NULL,
  `o` TINYINT(1) NULL DEFAULT NULL,
  `p` TINYINT(1) NULL DEFAULT NULL,
  `pexp` TINYINT(1) NULL DEFAULT NULL,
  `pnk` TINYINT(1) NULL DEFAULT NULL,
  `red` TINYINT(1) NULL DEFAULT NULL,
  `y` TINYINT(1) NULL DEFAULT NULL,
  `bus` TINYINT(1) NULL DEFAULT NULL,
  `latitude` FLOAT(14,10) NULL DEFAULT NULL,
  `longitude` FLOAT(14,10) NULL DEFAULT NULL,
  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`results_key`))
ENGINE = InnoDB
AUTO_INCREMENT = 11375
DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------
-- Table `zillow_oltp`.`zone`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `zillow_oltp`.`zone` (
  `zone` INT(11) NOT NULL,
  `n_bound` FLOAT NOT NULL,
  `s_bound` FLOAT NOT NULL,
  `w_bound` FLOAT NOT NULL,
  `e_bound` FLOAT NOT NULL,
  PRIMARY KEY (`zone`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
USE `zillow_oltp`;

DELIMITER $$
USE `zillow_oltp`$$
CREATE
DEFINER=`root`@`%`
TRIGGER `zillow_oltp`.`new_property_added`
AFTER INSERT ON `zillow_oltp`.`property`
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
END$$


DELIMITER ;
