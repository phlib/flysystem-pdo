CREATE TABLE flysystem_path (
  path_id INTEGER NOT NULL,
  type TEXT NOT NULL,
  path TEXT NOT NULL,
  mimetype TEXT,
  visibility TEXT,
  size INTEGER DEFAULT NULL,
  is_compressed INTEGER DEFAULT 1,
  expiry TEXT DEFAULT NULL,
  meta TEXT DEFAULT NULL,
  update_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (path_id)
);

CREATE UNIQUE INDEX idx_path ON flysystem_path (path);

CREATE INDEX idx_expiry ON flysystem_path (expiry);

CREATE TABLE flysystem_chunk (
  path_id INTEGER NOT NULL,
  chunk_no INTEGER NOT NULL,
  content BLOB NOT NULL,
  PRIMARY KEY (path_id, chunk_no)
);
