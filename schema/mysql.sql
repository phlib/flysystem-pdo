# noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE `flysystem_path` (
  `path_id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT, -- 16,777,215
  `type` ENUM('dir', 'file') NOT NULL,
  `path` VARCHAR(255) NOT NULL,
  `mimetype` VARCHAR(255) CHARACTER SET ascii DEFAULT NULL,
  `visibility` VARCHAR(25) DEFAULT '',
  `size` int(10) UNSIGNED DEFAULT NULL,
  `is_compressed` BOOL NOT NULL DEFAULT 1,
  `update_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`path_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `flysystem_chunk` (
  `path_id` MEDIUMINT UNSIGNED NOT NULL,
  `chunk_no` SMALLINT(5) UNSIGNED NOT NULL,
  `content` longblob NOT NULL,
  PRIMARY KEY (`path_id`,`chunk_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

