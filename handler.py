"""Defines handler for processing CSV file uploaded to S3 into an SQLite database.
"""
import sys
import logging
import sqlite3

from pathlib import Path
from typing import Any, Iterator, List

from base import AttrDict, AttrDictReader, RowValidator


logging.basicConfig(format='%(levelname)s - %(module)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__file__)

BASE_DIR = Path(__file__).parent
row_validator = RowValidator()


class DB:
    _DML_INSERT = 'INSERT INTO uploads VALUES (:batch, :start, :end, :records, :pass, :message)'


    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def fetchall(self):
        """Returns all rows within the 'uploads' table.
        """
        return self.conn.fetchall('SELECT * FROM uploads')

    def insert(self, rows: List[AttrDict]):
        """Adds provided rows to the 'uploads' table.

        :param rows: records to be added to the database.
        :type rows: List[AttrDict]
        """
        self.conn.executemany(self._DML_INSERT, rows)
        self.conn.commit()

    @classmethod
    def create_database(cls, filepath: Path) -> sqlite3.Connection:
        """Creates a new SQLite database and the data tables.

        :param filepath: destination path for the created database file
        :type filepath: Path
        :return: connection to the created database.
        :rtype: sqlite3.Connection
        """
        schema_filepath = BASE_DIR / 'schema.sql'
        assert schema_filepath.exists(), 'Database schema definition file not found!'

        conn = sqlite3.connect(filepath)
        with schema_filepath.open('r') as schema:
            conn.executescript(schema.read())

        return conn

    @classmethod
    def connect(cls, filepath: Path) -> 'DB':
        """Returns a connection to the SQLite database at the provided path if it exists otherwise
        a new database is created.
        """
        conn = sqlite3.connect(filepath) if filepath.exists() else cls.create_database(filepath)
        return DB(conn)


def process_upload(filepath: Path, db: DB, batch_size: int = 50):
    def iterate_records() -> Iterator[List[AttrDict]]:
        with filepath.open('r') as fp:
            reader = AttrDictReader(fp)

            rows = []
            for row in reader:
                if not row_validator.is_valid(row):
                    logger.debug(f'Invalid row encountered. Error: {row_validator.err}.')
                    continue

                rows.append(row)
                if len(rows) == batch_size:
                    yield rows
                    rows = []

            if rows:
                yield rows
                rows = []

    # process file records and insert to database
    for rows in iterate_records():
        db.insert(rows)


def handler():
    # get uploaded csv file
    argv = sys.argv[1:]
    if not argv:
        raise ValueError('Target file path not provided')

    uploaded_file = Path(argv[0])

    # get database
    db = DB.connect(Path('./bin/uploads.db3'))
    process_upload(uploaded_file, db)
    logger.info('done')


if __name__ == '__main__':
    handler()
