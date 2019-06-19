set @old_unique_checks=@@unique_checks, unique_checks=0;
set @old_foreign_key_checks=@@foreign_key_checks, foreign_key_checks=0;
set @old_sql_mode=@@sql_mode, sql_mode='allow_invalid_dates';
set sql_safe_updates=0; 

drop schema if exists `zillow_oltp`;
create schema `zillow_oltp` default character set utf8mb4;
use `zillow_oltp`;

-- -----------------------------------------------------
-- table `zillow_oltp`.`deep_search`
-- -----------------------------------------------------
drop table if exists `zillow_oltp`.`deep_search`;

create table if not exists `zillow_oltp`.`deep_search` (
  `results_key` int(11) not null auto_increment,
  `zpid` int(15) not null,
  `street` varchar(50) null default null,
  `city` varchar(50) null default null,
  `zipcode` varchar(15) null default null,
  `latitude` float(11,8) null default null,
  `longitude` float(11,8) null default null,
  `zestimate` float(11,2) null default null,
  `valueChange` float(11,2) null default null,
  `low_estimate` float(11,2) null default null,
  `high_estimate` float(11,2) null default null,
  `neighborhood` varchar(100) null default null,
  `neighborhood_zindex_value` float(11,2) null default null,
  `hometype` varchar(50) null default null,
  `assessmentYear` int(4) null default null,
  `assessment` float(11,2) null default null,
  `yearBuilt` int(4) null default null,
  `lotSize` int(7) null default null,
  `houseSize` int(7) null default null,
  `bathrooms` float(5,2) null default null,
  `bedrooms` int(3) null default null,
  `lastSold` date null default null,
  `lastSoldPrice` float(11,2) null default null,
  `updated` timestamp not null default current_timestamp on update current_timestamp, 
  primary key (`results_key`))
engine = innodb
default character set = utf8mb4;

set sql_mode=@old_sql_mode;
set foreign_key_checks=@old_foreign_key_checks;
set unique_checks=@old_unique_checks;
