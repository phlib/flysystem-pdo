
CREATE TABLE flysystem_path (
  path_id INTEGER PRIMARY KEY,
  type TEXT NOT NULL,
  path TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  contents BLOB,
  size INTEGER NOT NULL DEFAULT 0,
  mimetype TEXT,
  timestamp INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (path_id)
);

CREATE TABLE flysystem_chunk (
  path_id MEDIUMINT UNSIGNED NOT NULL,
  chunk_no SMALLINT(5) UNSIGNED NOT NULL,
  content longblob NOT NULL,
  PRIMARY KEY (path_id, chunk_no)
);
