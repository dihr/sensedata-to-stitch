import json
from unittest import mock
from unittest.mock import Mock

import pytest

from stitch_api import StitchApi


class TestStitchApi:
    @mock.patch('requests.request', return_value=Mock(status_code=201))
    def test_push_data_to_stitch(self, mock_request):
        api = StitchApi()
        data = json.dumps([{}])
        api.push_data_to_stitch(data=data)
        mock_request.assert_called_once()

    @pytest.mark.parametrize(
        ("obj", "row"),
        [
            (
                    {'data': {'id': 1}},
                    {"custom_fields": {
                        "etapa": {
                            "value": "Ongoing"
                        }, "trial": {
                            "value": "test"
                        }
                    }}
            )
        ]
    )
    def test_parse_custom_fields(self, obj, row):
        api = StitchApi()
        api._parse_custom_fields(obj=obj, row=row)
        assert obj['data']['custom_fields_etapa'] == row['custom_fields']['etapa']['value']
        assert obj['data']['custom_fields_trial'] == row['custom_fields']['trial']['value']

    @mock.patch('stitch_api.StitchApi._parse_nps_to_stitch_standard', return_value='')
    def test_paser_entity_data_to_stitch_standard(self, mock_stitch_parser):
        api = StitchApi()
        api.paser_entity_data_to_stitch_standard(data=[], entity_name='nps')
        mock_stitch_parser.assert_called_once()

    @pytest.mark.parametrize("data", [[{
        "id": 1,
        "id_legacy": "internal-tes",
        "id_customer": 277,
        "ref_date": "2019-10-28T00:00:00",
        "survey_date": "2019-10-28T00:00:00",
        "medium": "tes@gmail.com",
        "respondent": "test",
        "score": 7,
        "role": "SUPER_ADMIN",
        "stage": "",
        "group": "",
        "category": "",
        "nps_status": "neutral",
        "comments": "",
        "tags": "",
        "created_at": "2020-09-26T01:00:02.371497",
        "updated_at": "",
    }]])
    def test_parse_nps_to_stitch_standard(self, data):
        api = StitchApi()
        resp = api._parse_nps_to_stitch_standard(data=data)

        # Checking some fields
        assert json.loads(resp)[0]['data']['id'] == data[0]['id']
        assert json.loads(resp)[0]['data']['role'] == data[0]['role']
        assert json.loads(resp)[0]['data']['medium'] == data[0]['medium']

        # Checking non-optional fields
        table_name = 'nps'
        assert table_name == json.loads(resp)[0]['table_name']
        assert data[0] == json.loads(resp)[0]['data']

        action = 'upsert'
        assert action == json.loads(resp)[0]['action']

    @pytest.mark.parametrize("data", [[{
        "id": 20,
        "id_legacy": "ATIV-0001",
        "id_customer": 2,
        "id_parent": 19,
        "id_contact": 435,
        "group": "G02",
        "description": "Ligar para cliente",
        "notes": "Ligar para o telefone +55123456789",
        "start_date": "2021-03-14",
        "due_date": "2021-03-21",
        "end_date": "2021-03-18T10:00:00Z",
        "type": {
            "id": 2545,
            "description": "meeting",
            "caption": "Reunião",
            "enabled": True,
            "is_default": True
        },
        "status": {
            "id": 2545,
            "description": "Concluída"
        },
        "priority": {
            "id": 2545,
            "description": "Alta"
        },
        "owner": {
            "id": 3,
            "name": "Hariosvaldo",
            "username": "hariosvaldo.empresa",
            "email": "hariosvaldo@empresa.com",
            "profile": {
                "name": "Viewer",
                "role": "viewer"
            },
            "active": True,
            "registered_on": "2021-02-26T10:03:00Z",
            "created_at": "2021-02-24T00:00:00Z",
            "updated_at": "2021-02-27T10:35:00Z"
        },
        "created_by": {
            "id": 3,
            "name": "Hariosvaldo",
            "username": "hariosvaldo.empresa",
            "email": "hariosvaldo@empresa.com",
            "profile": {
                "name": "Viewer",
                "role": "viewer"
            },
            "active": True,
            "registered_on": "2021-02-26T10:03:00Z",
            "created_at": "2021-02-24T00:00:00Z",
            "updated_at": "2021-02-27T10:35:00Z"
        },
        "hours_spent": 2,
        "hours_planned": 3,
        "progress": 100,
        "id_playbook": 2,
        "id_rule": 15,
        "tags": "contato",
        "created_at": "2020-03-19T00:00:00Z",
        "system_end_date": "2021-03-18T13:00:00Z",
        "updated_at": "2020-03-19T00:00:00Z",
        "custom_value": 15,
        "favorite": True
    }]])
    def test_parse_tasks_to_stitch_standard(self, data):
        api = StitchApi()
        resp = api._parse_tasks_to_stitch_standard(data=data)

        # Checking some fields
        assert json.loads(resp)[0]['data']['id'] == data[0]['id']
        assert json.loads(resp)[0]['data']['hours_planned'] == data[0]['hours_planned']
        assert json.loads(resp)[0]['data']['updated_at'] == data[0]['updated_at']

        # Checking non-optional fields
        table_name = 'tasks'
        assert table_name == json.loads(resp)[0]['table_name']

        action = 'upsert'
        assert action == json.loads(resp)[0]['action']

    @pytest.mark.parametrize("data", [[{
        "id": 2,
        "id_legacy": "L0001",
        "group": "Grupo ABC",
        "name_contract": "Ardidas SA",
        "name": "Ardidas",
        "cnpj": "23.435.123/0001-23",
        "status": {
            "id": 1,
            "description": "Em vigência",
            "enabled": True
        },
        "sponsor": "",
        "sponsor_phone": "",
        "sponsor_email": "",
        "state": "BA",
        "city": "Itú",
        "size": "Grande",
        "stage": "Adoção",
        "dt_stage": "2020-02-14",
        "dt_register": "2020-03-01",
        "industry": "Tecidos",
        "salesperson": "Juliana",
        "cs": {
            "id": 3,
            "name": "Hariosvaldo",
            "username": "hariosvaldo.empresa",
            "email": "hariosvaldo@empresa.com",
            "profile": {
                "name": "Viewer",
                "role": "viewer"
            },
            "active": True,
            "registered_on": "2021-02-26T10:03:00Z",
            "created_at": "2021-02-24T00:00:00Z",
            "updated_at": "2021-02-27T10:35:00Z"
        },
        "csm": {
            "id": 3,
            "name": "Hariosvaldo",
            "username": "hariosvaldo.empresa",
            "email": "hariosvaldo@empresa.com",
            "profile": {
                "name": "Viewer",
                "role": "viewer"
            },
            "active": True,
            "registered_on": "2021-02-26T10:03:00Z",
            "created_at": "2021-02-24T00:00:00Z",
            "updated_at": "2021-02-27T10:35:00Z"
        },
        "dt_cancel": "2021-03-01",
        "cancel_tag": "Pausa,Bom relacionamento",
        "cancel_description": "Pausa no projeto",
        "badge": {

        },
        "created_at": "2020-03-02",
        "updated_at": "2020-01-01T00:00:00Z",
        "custom_fields": {
            "vertical": {
                "value": "Eletrônicos"
            }
        }
    }]])
    def test_parse_customer_to_stitch_standard(self, data):
        api = StitchApi()
        resp = api._parse_customer_to_stitch_standard(data=data)

        # Checking some fields
        assert json.loads(resp)[0]['data']['id'] == data[0]['id']
        assert json.loads(resp)[0]['data']['name'] == data[0]['name']
        assert json.loads(resp)[0]['data']['created_at'] == data[0]['created_at']

        # Checking non-optional fields
        table_name = 'customers'
        assert table_name == json.loads(resp)[0]['table_name']

        action = 'upsert'
        assert action == json.loads(resp)[0]['action']

    @pytest.mark.parametrize("data", [[{
        "id": 16,
        "id_legacy": "0006",
        "customer": {
            "id": 2,
            "id_legacy": "L0001",
            "group": "Grupo ABC",
            "name_contract": "Ardidas SA",
            "name": "Ardidas",
            "cnpj": "23.435.123/0001-23"
        },
        "is_main_sponsor": True,
        "is_active": True,
        "name": "John",
        "nickname": "John John",
        "email": "contato@mymail.com",
        "occupation": "Gerente",
        "types": [
            {
                "id": 1,
                "name": "Viewer"
            }
        ],
        "phone": "+551154329877",
        "phone2": "+5511984543234",
        "address": "Rua Jurubatuba, 1043",
        "skype": "john.john@skype.com",
        "email_unsubscribe": False,
        "unsubscribe_reason": "Não quer receber e-mails",
        "is_favorite": False,
        "obs_info": "Contatar após as 14h",
        "custom_fields": {
            "setor": {
                "value": "TI"
            }
        },
        "updated_at": "2020-01-01T00:00:00Z"
    }]])
    def test_parse_contact_to_stitch_standard(self, data):
        api = StitchApi()
        resp = api._parse_contact_to_stitch_standard(data=data)
        # Checking some fields
        assert json.loads(resp)[0]['data']['id'] == data[0]['id']
        assert json.loads(resp)[0]['data']['is_favorite'] == data[0]['is_favorite']
        assert json.loads(resp)[0]['data']['skype'] == data[0]['skype']

        # Checking non-optional fields
        table_name = 'contacts'
        assert table_name == json.loads(resp)[0]['table_name']

        action = 'upsert'
        assert action == json.loads(resp)[0]['action']
