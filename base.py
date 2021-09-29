import csv
import re
from datetime import datetime
from typing import Any, Callable


# Exceptions
# ==============================================================

class ValidationError(Exception):
    """Error raised for invalid data.
    """
    pass


# Types
# ==============================================================

class AttrDict(dict):
    """Represents a dict that allows attribute-style access.
    """

    def __getattr__(self, name: str) -> Any:
        value = self.get(name)
        if isinstance(value, dict) and not isinstance(value, AttrDict):
            value = self[name] = AttrDict(value)
        return value

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class AttrDictReader(csv.DictReader):
    """A CSV reader that returns an AttrDict for each row.
    """

    def __next__(self) -> AttrDict:
        row = super().__next__()
        return AttrDict(row)


# Validators
# ==============================================================

class BatchValidator:
    """Ensures that batch values are 20 characters long and characters only.
    """
    length: int

    def __init__(self, length: int = 20):
        self.length = length
        self.pattern = re.compile(f'[a-zA-Z]{{{length}}}')

    def __call__(self, value: str):
        match = self.pattern.match(value)
        if not match or len(value) != self.length:
            raise ValidationError(f'Invalid batch provided: {value}')


class BoolValidator:
    """Validates boolean string values.
    """
    def __call__(self, value: str):
        if (value or '').lower() not in ('false', 'true'):
            raise ValidationError(f'Invalid boolean value: {value}')


class DateValidator:
    def __init__(self, format: str = '%Y-%m-%d'):
        self.format = format

    def __call__(self, value: str):
        try:
            datetime.strptime(value, self.format)
        except ValueError:
            raise ValidationError(f'Invalid date value: {value}. Expected format: {self.format}')


class FuncValidator:
    """Validator which relegated validation to a callable.
    """

    def __init__(self, func: Callable, label: str = None):
        self.label = label
        self.func = func

    def __call__(self, value: str):
        try:
            self.func(value)
        except Exception as ex:
            raise ValidationError(f"Invalid {self.label or 'value'} provided: {value}. Error: {ex}")


class RowValidator:
    """Defines rules for validating csv records to ensure column data are of expected type/format.
    """

    err: ValidationError = None
    datetime_validator = DateValidator('%Y-%m-%dT%H:%M:%S')
    columns = AttrDict({
        'batch': BatchValidator(),
        'start': datetime_validator,
        'end': datetime_validator,
        'records': FuncValidator(int),
        'pass': BoolValidator(),
    })

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
