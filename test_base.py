import pytest
from base import (
    AttrDict, BatchValidator, BoolValidator, DateValidator, FuncValidator, ValidationError,
    RowValidator
)


class TestBatchValidator:
    validator = BatchValidator()

    def test_fails_for_batch_length_not_matching_validator_defined_length(self):
        # validator expecting default length of 20
        expected_length = 20
        assert self.validator.length == expected_length
        for batch_value in ['qwerty', 'ASikVvAGnNIqCwFJKresXYZ']:
            assert len(batch_value) != expected_length
            pytest.raises(ValidationError, self.validator, batch_value)

        # validator expecting length of 5
        expected_length = 5
        validator = BatchValidator(expected_length)
        for batch_value in ['qwerty', 'asdf', 'xyz']:
            assert len(batch_value) != expected_length
            pytest.raises(ValidationError, validator, batch_value)

    def test_fails_for_non_all_char_batch_value(self):
        expected_length = 20
        assert self.validator.length == expected_length
        for batch_value in [
            '9kzuKXkRCjkvdgvgpLsC', # starts with digit
            'dkzuKXkRCjkvdgvgpLs7', # ends with digit
            'dkzuKXkRC7kvdgvgpLsC', # has embedded digit
        ]:
            assert len(batch_value) == expected_length
            pytest.raises(ValidationError, self.validator, batch_value)

    def test_passes_for_all_char_batch_value_matching_validator_defined_length(self):
        # validator expecting default length of 20
        expected_length = 20
        assert self.validator.length == expected_length

        for batch_value in ['fUBImUCTzQUsbRVVNSoM', 'ASikVvAGnNIqCwFJKres', 'TJZbvyCgkNBDLrJeiaOX']:
            assert len(batch_value) == expected_length
            try:
                self.validator(batch_value)
            except (Exception):
                pytest.fail(f'Unexpected error. Value: {batch_value}')

        # validator expect length of 5
        expected_length = 5
        validator = BatchValidator(expected_length)
        for batch_value in ['query', 'asDFg', 'xyUwa']:
            assert len(batch_value) == expected_length
            try:
                validator(batch_value)
            except (Exception):
                pytest.fail(f'Unexpected error. Value: {batch_value}')


class TestBoolValidator:
    validator = BoolValidator()

    def test_passes_for_true_values_in_different_case(self):
        true_values = ['True', 'true', 'tRue', 'TRUE']
        for value in true_values:
            try:
                self.validator(value)
            except Exception:
                pytest.fail('Unexpected error')

    def test_passes_for_false_values_in_different_case(self):
        false_values = ['False', 'false', 'FALSE', 'faLSe']
        for value in false_values:
            try:
                self.validator(value)
            except Exception:
                pytest.fail('Unexpected error')

    def test_fails_for_invalid_values(self):
        values = ['yes', 'YES', 'no', 'NO', 'QWERTY', '0', '1']
        for value in values:
            pytest.raises(ValidationError, self.validator, value)


class TestDateValidator:
    def test_fails_for__valid_date_value__invalid_format(self):
        validator = DateValidator(format='%Y-%m-%d')
        values = ['2021-01', '2021-01-01T11:01:01']
        for value in values:
            pytest.raises(ValidationError, validator, value)

    def test_fails_for__invalid_date_value__valid_format(self):
        validator = DateValidator(format='%Y-%m-%d')
        values = ['2021-01-32', '2021-02-29', '2021-13-01']
        for value in values:
            pytest.raises(ValidationError, validator, value)

        validator = DateValidator(format='%Y-%m-%dT%H:%M:S')
        for value in [
            '2020-11-11T24:58:40', # invalid hour
            '2020-11-11T22:68:40', # invalid minute
            '2020-11-11T22:58:90', # invalid second
            '2020-13-11T22:58:40', # invalid month
            '2020-11-13T22:58:40', # invalid day
        ]:
            pytest.raises(ValidationError, validator, value)

    def test_passes_for__valid_date_value__valid_format(self):
        validator = DateValidator(format='%Y-%m-%d')
        values = ['2021-01-01', '2020-02-29']
        for value in values:
            try:
                validator(value)
            except ValueError:
                pytest.fail('Unexpected error')


        validator = DateValidator(format='%Y-%m-%dT%H:%M:%S')
        for value in [
            '2020-11-11T23:58:40',
            '1979-11-11T22:58:40'
        ]:
            try:
                validator(value)
            except ValueError:
                pytest.fail('Unexpected error')


class TestFuncValidator:
    int_validator = FuncValidator(int)
    float_validator = FuncValidator(float)

    def test__int_validator__fails_for__char__alphanum__decimal__value(self):
        values = ['one', '6e', '5b', '3.14']
        for value in values:
            pytest.raises(ValidationError, self.int_validator, value)


    def test_int_validator__passes_for_int_string_value(self):
        values = ['0', '1', '1234', '192873']
        for value in values:
            try:
                self.int_validator(value)
            except Exception as ex:
                pytest.fail(f'Unexpected error: {ex}')

    def test__float_validator__fails_for__char__alphanum__decimal__value(self):
        values = ['one', '6e', '5b', '3.14b']
        for value in values:
            pytest.raises(ValidationError, self.float_validator, value)

    def test_float_validator__passes_for__int__float__string_value(self):
        values = ['0', '1', '1234', '1e3']
        for value in values:
            try:
                self.float_validator(value)
            except Exception as ex:
                pytest.fail(f'Unexpected error: {ex}')


class TestRowValidator:
    validator = RowValidator()

    @pytest.mark.parametrize('data', [
        {
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-09-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '100'
        },
        {
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-09-12T03:32:48',
            'records': '100', 'pass': 'true'
        },
        {
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-09-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '100', 'pass': None
        }
    ])
    def test_fails_for__missing_column__missing_data(self, data):
        row = AttrDict(data)
        pytest.raises(ValidationError, self.validator, row)
        assert self.validator.is_valid(row) == False


    @pytest.mark.parametrize('data', [
        {
            # invalid batch length
            'batch': 'oIcACSLYuEWKXVHNeXdKZYZ', 'start': '2003-09-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '100', 'pass': 'true'
        },
        {
            # invalid start date
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-19-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '100', 'pass': 'true'
        },
        {
            # invalid records value
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-09-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '10.6', 'pass': 'false'
        }
    ])
    def test_fails_for__invalid_column_values(self, data):
        row = AttrDict(data)
        pytest.raises(ValidationError, self.validator, row)
        assert self.validator.is_valid(row) == False


    def test_passes_for_valid_row_data(self):
        row = AttrDict(        {
            'batch': 'oIcACSLYuEWKXVHNeXdK', 'start': '2003-09-12T03:32:48',
            'end': '2019-05-29T11:51:43', 'records': '100', 'pass': 'true'
        })

        try:
            self.validator(row)
            assert self.validator.is_valid(row) == True
        except ValidationError:
            pytest.fail('Unexpected error')
