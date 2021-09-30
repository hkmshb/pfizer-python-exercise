-- Table: uploads
-- stores all processed csv records
CREATE TABLE uploads (
    [batch]   VARCHAR(20) NOT NULL
  , [start]   DATETIME NOT NULL
  , [end]     DATETIME NOT NULL
  , [records] INTEGER NOT NULL
  , [pass]    BOOLEAN NOT NULL
  , [message] VARCHAR(1000) NOT NULL
);
