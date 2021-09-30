"""Defines handler for processing CSV file uploaded to S3 into an SQLite database.
"""
import json
import logging
import mimetypes
import sqlite3
from pathlib import Path
from typing import Any, Iterator, List

from base import AttrDict, AttrDictReader, RowValidator, S3ObjInfo

logging.basicConfig(format='%(levelname)s - %(module)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__file__)

DB_KEY = 'store/uploads.db3'
MIMETYPE_CSV = 'text/csv'

BASE_DIR = Path(__file__).parent
TEMP_DIR = Path('/tmp')

row_validator = RowValidator()


class DB:
    _DML_INSERT = 'INSERT INTO uploads VALUES (:batch, :start, :end, :records, :pass, :message)'

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def count(self):
        """Returns the total number of records within the 'uploads' table."""
        cur = self.conn.execute('SELECT COUNT(*) FROM uploads')
        return cur.fetchone()[0]

    def fetchall(self):
        """Returns all rows within the 'uploads' table."""
        cur = self.conn.execute('SELECT * FROM uploads')
        return cur.fetchall()

    def fetch_by_batch(self, batch):
        """Returns all rows with batch matching specified value."""
        cur = self.conn.execute('SELECT * FROM uploads WHERE batch = ?', (batch,))
        return cur.fetchall()

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

        conn = sqlite3.connect(str(filepath))
        with schema_filepath.open('r') as schema:
            conn.executescript(schema.read())

        return conn

    @classmethod
    def connect(cls, info: S3ObjInfo) -> 'DB':
        """Returns a connection to the SQLite database at the provided path if it exists otherwise
        a new database is created.
        """
        info.download(TEMP_DIR)
        if info.local_exists:
            conn = sqlite3.connect(info.local_filepath)
        else:
            conn = cls.create_database(info.local_filepath)

        return DB(conn)


def cleanup_resources(infos: List[S3ObjInfo]):
    count = 0
    logger.info('Cleaning up downloaded resources ...')
    for info in [i for i in infos or [] if i.local_exists]:
        try:
            info.local_filepath.unlink()
            count += 1
        except Exception:
            logger.debug(f'Unable to delete: {info.local_filepath}')

    logger.info(f'{count}/{len(infos)} deleted.')


def process_records(filepath: Path, db: DB, batch_size: int = 50):
    def iterate_records() -> Iterator[List[AttrDict]]:
        with filepath.open('r') as fp:
            reader = AttrDictReader(fp)

            rows: List[AttrDict] = []
            for row in reader:
                if not row_validator.is_valid(row):  # type: ignore
                    logger.debug(f'Invalid row encountered. Error: {row_validator.err}.')
                    continue

                rows.append(row)  # type: ignore
                if len(rows) == batch_size:
                    yield rows
                    rows = []

            if rows:
                yield rows
                rows = []

    # check that file is csv
    file_type, _ = mimetypes.guess_type(filepath)
    if file_type is None or file_type != MIMETYPE_CSV:
        return

    # process file records and insert to database
    for rows in iterate_records():
        db.insert(rows)


def download_s3_objects(event: AttrDict) -> Iterator[S3ObjInfo]:
    """Download files uploaded to S3 and return valid files for further processing.

    :param event: event data.
    :type event: AttrDict
    :return: list of S3ObjInfo that are to be processed further.
    :rtype: S3ObjInfo
    """
    for record in event.Records:
        info = S3ObjInfo(record.s3.bucket.name, record.s3.object.key)
        info.download(TEMP_DIR)
        yield info


def handler(event: dict, context: Any):
    # download uploaded file and identify csv files
    infos, skipped = [], []

    logger.info('Downloading file(s) uploaded to s3 ...')
    for info in download_s3_objects(AttrDict(event)):
        if info.content_type != 'text/csv':
            logger.info(f'Skipping non csv file: {info.key}')
            skipped.append(info)
            continue

        infos.append(info)

    logger.info(f'{len(infos)} file(s) downloaded ...')
    if not infos:
        logger.info('Terminating operation. No csv files for processing.')
        return {'status_code': 200, 'message': '0 csv file(s) processed.'}

    # retrieve or create sqlite database to be written to
    db_info = S3ObjInfo(infos[0].bucket, DB_KEY)
    db = DB.connect(db_info)

    # process and add csv records into database
    for info in infos:
        try:
            process_records(info.local_filepath, db)
            logger.info(f'Records processed and written to database for: {info.key}')
        except Exception as ex:
            logger.error(f'Error processing csv records. File: {info.key}. Error: {ex}')

    # upload updated database back to s3
    try:
        db_info.upload()
        logger.info('Operation completed successfully!')
    except Exception as ex:
        logger.error(f'Database upload back to S3 failed. Error: {ex}')

    try:
        cleanup_resources(infos + skipped + [db_info])
    except Exception:
        pass


if __name__ == '__main__':
    with Path('./bin/config.json').open('r') as f:
        event = json.load(f).get('event')
        handler(event, None)
