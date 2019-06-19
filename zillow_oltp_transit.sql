set @old_unique_checks=@@unique_checks, unique_checks=0;
set @old_foreign_key_checks=@@foreign_key_checks, foreign_key_checks=0;
set @old_sql_mode=@@sql_mode, sql_mode='allow_invalid_dates';
set sql_safe_updates=0; 

use `zillow_oltp`;

CREATE TABLE IF NOT EXISTS `zillow_oltp`.`transit` (
  `results_key` int(11) not null auto_increment,
  `stop_id` int(11) null default null,
  `direction_id` VARCHAR(12) null default null,
  `stop_name` VARCHAR(100) null default null,
  `blue` boolean null default null,
  `brn` boolean null default null,
  `g` boolean null default null,
  `o` boolean null default null,
  `p` boolean null default null,
  `pexp` boolean null default null,
  `pnk` boolean null default null,
  `red` boolean null default null,
  `y` boolean null default null,
  `bus` boolean null default null,
  `latitude` float(14,10) null default null,
  `longitude` float(14,10) null default null,
  `updated` timestamp not null default current_timestamp on update current_timestamp, 
  primary key (`results_key`))
engine = innodb
default character set = utf8mb4;

set sql_mode=@old_sql_mode;
set foreign_key_checks=@old_foreign_key_checks;
set unique_checks=@old_unique_checks;
