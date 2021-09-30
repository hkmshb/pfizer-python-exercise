import sqlite3
from pathlib import Path

import pytest

from handler import process_records, DB


@pytest.fixture
def db():
    with Path('./schema.sql').open('r') as schema:
        conn = sqlite3.connect(':memory:')
        conn.executescript(schema.read())
        yield DB(conn)

        conn.close()


class TestUploadProcessing:
    @pytest.mark.parametrize(
        'upload, last_batch',
        [
            (Path('./fixtures/file1.csv'), 'pMQaQdvxVbimtnsHRAds'),
            (Path('./fixtures/file2.csv'), 'tmoThtJPZsossGWXjUdz'),
        ],
    )
    def test_process_records_skips_bad_records_to_process_other_records(
        self, db, upload, last_batch
    ):
        assert db.count() == 0
        assert upload.exists()

        try:
            process_records(upload, db)
        except Exception as ex:
            pytest.fail(f'Unexpected error: {ex}')

        assert db.count() > 0

        records = db.fetch_by_batch(last_batch)
        assert records is not None
        assert len(records) == 1
        assert records[0][0] == last_batch

    def test_process_records_skips_all_records_for_a_bad_file(self, db):
        assert db.count() == 0

        upload = Path('./fixtures/file3.csv')
        assert upload.exists()

        try:
            process_records(upload, db)
        except Exception as ex:
            pytest.fail(f'Unexpected error. Error: {ex}')

        assert db.count() == 0

    def test_process_records_skips_bad_file_to_process_other_files(self, db):
        uploads = (
            Path('./fixtures/file3.csv'),  # bad file, all records skipped
            Path('./fixtures/file1.csv'),  # valid file with invalid records
        )

        assert db.count() == 0
        try:
            for upload in uploads:
                assert upload.exists()
                process_records(upload, db)
        except Exception as ex:
            pytest.fail(f'Unexpected error. Error: {ex}')

        assert db.count() > 0

    def test_process_records_skips_invalid_files_to_process_other_files(self, db):
        uploads = (
            Path('./fixtures/image.png'),  # bad file, all records skipped
            Path('./fixtures/file1.csv'),  # valid file with invalid records
        )

        assert db.count() == 0
        try:
            for upload in uploads:
                assert upload.exists()
                process_records(upload, db)
        except Exception as ex:
            pytest.fail(f'Unexpected error. Error: {ex}')

        assert db.count() > 0
