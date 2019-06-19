set @old_unique_checks=@@unique_checks, unique_checks=0;
set @old_foreign_key_checks=@@foreign_key_checks, foreign_key_checks=0;
set @old_sql_mode=@@sql_mode, sql_mode='allow_invalid_dates';
set sql_safe_updates=0; 

use `zillow_oltp`;

CREATE TABLE IF NOT EXISTS `zillow_oltp`.`crime` (
  `results_key` int(11) not null auto_increment,
  `id` int(11) null default null,
  `case_number` VARCHAR(15) null default null,
  `date` datetime null default null,
  `year` int(4) null default null,
  `primary_type` varchar(45) null default null,
  `description` varchar(150) null default null,
  `domestic` boolean null default null,
  `arrest` boolean null default null,
  `beat` int(6) null default null,
  `block` varchar(45) null default null,
  `community_area` int(3) null default null, 
  `ward` int(3) null default null,
  `district` int(5) null default null,
  `latitude` float(14,10) null default null,
  `longitude` float(14,10) null default null,
  `location_description` varchar(150) null default null,
  `fbi_code` varchar(5) null default null,
  `iucr` varchar(5) null default null,
  `entered_on` datetime null default null,
  `x_coordinate` int(9) null default null,
  `y_coordinate` int(9) null default null,
  `updated` timestamp not null default current_timestamp on update current_timestamp, 
  primary key (`results_key`))
engine = innodb
default character set = utf8mb4;

set sql_mode=@old_sql_mode;
set foreign_key_checks=@old_foreign_key_checks;
set unique_checks=@old_unique_checks;
