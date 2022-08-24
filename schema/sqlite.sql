CREATE TABLE flysystem_path (
  path_id INTEGER NOT NULL,
  type TEXT NOT NULL,
  path TEXT NOT NULL,
  mimetype TEXT,
  visibility TEXT,
  size INTEGER DEFAULT NULL,
  is_compressed INTEGER DEFAULT 1,
  expiry TEXT DEFAULT NULL,
  update_ts TEXT NOT NULL DEFAULT 0,
  PRIMARY KEY (path_id)
);

CREATE TABLE flysystem_chunk (
  path_id INTEGER NOT NULL,
  chunk_no INTEGER NOT NULL,
  content BLOB NOT NULL,
  PRIMARY KEY (path_id, chunk_no)
);
