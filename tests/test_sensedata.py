from unittest import mock
from unittest.mock import Mock

import pytest
from requests import HTTPError

from sensedata import SensedataAPI


class TestSensedataAPI:
    @mock.patch('requests.get', return_value=Mock(status_code=200, json=lambda: {"data": {"id": 1}}))
    def test_get_entity_data(self, mock_request):
        api = SensedataAPI()
        data = api.get_entity_data(entity_name='customers', page=1)
        mock_request.assert_called_once()
        actual_id = 1
        assert actual_id == data['data']['id']

    @mock.patch('requests.get')
    def test_get_entity_data_not_ok(self, mock_request):
        api = SensedataAPI()
        exception = HTTPError(mock.Mock(status=404), "not found")
        mock_request(mock.ANY).raise_for_status.side_effect = exception

        with pytest.raises(HTTPError) as error_info:
            api.get_entity_data(entity_name='customers', page=1)
            assert error_info == exception
