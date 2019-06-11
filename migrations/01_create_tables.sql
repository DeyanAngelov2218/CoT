CREATE TABLE IF NOT EXISTS  `symbols` (
  `symbol_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`symbol_id`),
  UNIQUE KEY `idSecurities_UNIQUE` (`symbol_id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `symbols_data` (
  `data_id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol_id` int(11) NOT NULL,
  `week` date DEFAULT NULL,
  `open_interest` double DEFAULT NULL,
  `comm_netto` double DEFAULT NULL,
  `comm_long` double DEFAULT NULL,
  `comm_long_oi` double DEFAULT NULL,
  `comm_short` double DEFAULT NULL,
  `comm_short_oi` double DEFAULT NULL,
  `large_netto` double DEFAULT NULL,
  `large_long` double DEFAULT NULL,
  `large_long_oi` double DEFAULT NULL,
  `large_short` double DEFAULT NULL,
  `large_short_oi` double DEFAULT NULL,
  `small_netto` double DEFAULT NULL,
  `small_long` double DEFAULT NULL,
  `small_long_oi` double DEFAULT NULL,
  `small_short` double DEFAULT NULL,
  `small_short_oi` double DEFAULT NULL,
  UNIQUE KEY `data_id_UNIQUE` (`data_id`),
  KEY `symbol_id_idx` (`symbol_id`),
  CONSTRAINT `symbol_id` FOREIGN KEY (`symbol_id`) REFERENCES `symbols` (`symbol_id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;