import csv
import logging
import re
import uuid

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import unquote

import boto3

logger = logging.getLogger(__file__)
s3 = boto3.client('s3')


# Exceptions
# ==============================================================


class ValidationError(Exception):
    """Error raised for invalid data."""

    pass


# Types
# ==============================================================


class AttrDict(dict):
    """Represents a dict that allows attribute-style access."""

    def __getattr__(self, name: str) -> Any:
        value = self.get(name)
        is_dict = lambda x: isinstance(x, dict) and not isinstance(x, AttrDict)  # noqa

        if is_dict(value):
            value = self[name] = AttrDict(value)  # type: ignore
        elif isinstance(value, (list, tuple)):
            value = [AttrDict(item) if is_dict(item) else item for item in value]
            self[name] = value

        return value

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class AttrDictReader(csv.DictReader):
    """A CSV reader that returns an AttrDict for each row."""

    def __next__(self) -> AttrDict:
        row = super().__next__()
        return AttrDict(row)


class S3ObjInfo:
    """Provides properties and methods for working with objects on S3."""

    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self.__local_filepath = None
        self.__content_type = None

    @property
    def content_type(self):
        return self.__content_type

    @property
    def local_filepath(self):
        return self.__local_filepath

    @property
    def local_exists(self):
        return (self.__local_filepath is not None) and self.__local_filepath.exists()

    def _generate_local_name(self):
        """Returns a randomly generated name that the S3 object can go by locally."""
        local_key = unquote(self.key).replace('/', '')
        return f'{uuid.uuid4()}_{local_key}'

    def download(self, dest_dir: Path):
        """Downloads S3 object.

        :param dest_path: destination path for downloaded object.
        :type dest_path: str
        :return: Path to the file representing downloaded s3 object.
        :rtype: str
        """
        logger.info(f'Downloading S3 object: {self.key} ...')
        self.__local_filepath = dest_dir / self._generate_local_name()

        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.key)
            self.__content_type = response['ContentType']

            logger.debug('Saving s3 object locally...')
            data = response['Body'].read()
            with self.local_filepath.open('wb') as f:
                f.write(data)
                f.flush()
        except Exception as ex:
            logger.error(f'Object download failed. {ex}')

    def upload(self):
        """Upload local file to S3."""
        if not self.local_exists:
            return

        with self.local_filepath.open('rb') as f:
            s3.put_object(Bucket=self.bucket, Key=self.key, Body=f)


# Validators
# ==============================================================


class BatchValidator:
    """Ensures that batch values are 20 characters long and characters only."""

    length: int

    def __init__(self, length: int = 20):
        self.length = length
        self.pattern = re.compile(f'[a-zA-Z]{{{length}}}')

    def __call__(self, value: str):
        match = self.pattern.match(value)
        if not match or len(value) != self.length:
            raise ValidationError(f'Invalid batch provided: {value}')


class BoolValidator:
    """Validates boolean string values."""

    def __call__(self, value: str):
        if (value or '').lower() not in ('false', 'true'):
            raise ValidationError(f'Invalid boolean value: {value}')


class DateValidator:
    def __init__(self, fmt: str = '%Y-%m-%d'):
        self.format = fmt

    def __call__(self, value: str):
        try:
            datetime.strptime(value, self.format)
        except ValueError:
            raise ValidationError(
                f'Invalid date value: {value}. Expected format: {self.format}'
            )


class FuncValidator:
    """Validator which relegated validation to a callable."""

    def __init__(self, func: Callable, label: str = None):
        self.label = label
        self.func = func

    def __call__(self, value: str):
        try:
            self.func(value)
        except Exception as ex:
            raise ValidationError(
                f"Invalid {self.label or 'value'} provided: {value}. Error: {ex}"
            )


def _notempty(value: str):
    if value in (None, '') or len(value.strip()) == 0:
        raise ValueError('Value cannot be empty or whitespaces only')


NotEmptyValidator = FuncValidator(_notempty)


class RowValidator:
    """Defines rules for validating csv records to ensure column data are of expected type/format."""  # noqa

    err: Optional[ValidationError] = None
    datetime_validator = DateValidator('%Y-%m-%dT%H:%M:%S')

    columns = AttrDict(
        {
            'batch': BatchValidator(),
            'start': datetime_validator,
            'end': datetime_validator,
            'records': FuncValidator(int),
            'pass': BoolValidator(),
            'message': NotEmptyValidator,
        }
    )

    def __call__(self, row: AttrDict):
        for name, validator in self.columns.items():
            value = row.get(name)
            if not value:
                raise ValidationError(f'Missing value for {name}')

            validator(value)

    def is_valid(self, row: AttrDict) -> bool:
        self.err = None

        try:
            self(row)
            return True
        except ValidationError as ex:
            self.err = ex
            return False
