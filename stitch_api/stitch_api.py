import abc
import json
import os
from datetime import datetime

import requests


class StitchApi:
    def __init__(self):
        self.base_url = 'https://api.stitchdata.com'
        self.api_token = os.getenv('STITCH_INTEGRATION_TOKEN')
        self.client_id = os.getenv('STITCH_CLIENT_ID')

    def push_data_to_stitch(self, data):
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        endpoint = f"{self.base_url}/v2/import/push"
        response = requests.request("POST", url=endpoint, headers=headers, data=data)
        print(response.text)
        response.raise_for_status()

    def paser_entity_data_to_stitch_standard(self, data: list, entity_name: str) -> str:
        match entity_name:
            case 'nps':
                return self._parse_nps_to_stitch_standard(data=data)
            case 'customers':
                return self._parse_customer_to_stitch_standard(data=data)
            case 'contacts':
                return self._parse_contact_to_stitch_standard(data=data)
            case 'tasks':
                return self._parse_tasks_to_stitch_standard(data=data)

    def _parse_contact_to_stitch_standard(self, data: list) -> str:
        data_list = []
        for row in data:
            obj = {
                'client_id': self.client_id,
                'action': 'upsert',
                'sequence': int(round(datetime.now().timestamp())),
                'table_name': 'contacts',
                'data': {
                    'id': row['id'],
                    'id_legacy': row['id_legacy'],
                    'customer_id': row['customer']['id'],
                    'customer_id_legacy': row['customer']['id_legacy'],
                    'customer_group': row['customer']['group'],
                    'customer_name_contract': row['customer']['name_contract'],
                    'customer_name': row['customer']['name'],
                    'customer_cnpj': row['customer']['cnpj'],
                    'is_main_sponsor': row['is_main_sponsor'],
                    'is_active': row['is_active'],
                    'name': row['name'],
                    'nickname': row['nickname'],
                    'email': row['email'],
                    'occupation': row['occupation'],
                    'phone': row['phone'],
                    'phone2': row['phone2'],
                    'address': row['address'],
                    'skype': row['skype'],
                    'email_unsubscribe': row['email_unsubscribe'],
                    'unsubscribe_reason': row['unsubscribe_reason'],
                    'is_favorite': row['is_favorite'],
                    'obs_info': row['obs_info'],
                },
                'key_names': [
                    'id'
                ]
            }

            # Check field types
            if row['types']:
                obj['data']['types_id'] = row['types'][0]['id']
                obj['data']['types_name'] = row['types'][0]['name']

            data_list.append(obj)
        return json.dumps(data_list)

    def _parse_customer_to_stitch_standard(self, data: list) -> str:
        data_list = []
        for row in data:
            obj = {
                'client_id': self.client_id,
                'action': 'upsert',
                'sequence': int(round(datetime.now().timestamp())),
                'table_name': 'customers',
                'data': {
                    'id': row['id'],
                    'id_legacy': row['id_legacy'],
                    'group': row['group'],
                    'name_contract': row['name_contract'],
                    'name': row['name'],
                    'cnpj': row['cnpj'],
                    'state': row['state'],
                    'city': row['city'],
                    'size': row['size'],
                    'stage': row['stage'],
                    'dt_stage': row['dt_stage'],
                    'dt_register': row['dt_register'],
                    'industry': row['industry'],
                    'salesperson': row['salesperson'],
                    'sponsor': row['sponsor'],
                    'sponsor_phone': row['sponsor_phone'],
                    'sponsor_email': row['sponsor_email'],
                    'dt_cancel': row['dt_cancel'],
                    'cancel_tag': row['cancel_tag'],
                    'cancel_description': row['cancel_description'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                    'status.id': row['status']['id'],
                    'status.description': row['status']['description'],
                    'status.enabled': row['status']['enabled'],
                    'cs.id': row['cs']['id'],
                    'cs.name': row['cs']['name'],
                    'cs.username': row['cs']['username'],
                    'cs.email': row['cs']['email'],
                    'cs.profile.name': row['cs']['profile']['name'],
                    'cs.profile.role': row['cs']['profile']['role'],
                    'cs.active': row['cs']['active'],
                    'cs.registered_on': row['cs']['registered_on'],
                    'cs.created_at': row['cs']['created_at'],
                    'cs.updated_at': row['cs']['updated_at'],
                    'csm.id': row['csm']['id'],
                    'csm.name': row['csm']['name'],
                    'csm.username': row['csm']['username'],
                    'csm.email': row['csm']['email'],
                    'csm.profile.name': row['csm']['profile']['name'],
                    'csm.profile.role': row['csm']['profile']['role'],
                    'csm.active': row['csm']['active'],
                    'csm.registered_on': row['csm']['registered_on'],
                    'csm.created_at': row['csm']['created_at'],
                    'csm.updated_at': row['csm']['updated_at'],
                },
                'key_names': [
                    'id'
                ]
            }
            self._parse_custom_fields(obj=obj, row=row)
            data_list.append(obj)
        return json.dumps(data_list)

    def _parse_nps_to_stitch_standard(self, data: list) -> str:
        data_list = []
        for row in data:
            data_list.append({
                'client_id': self.client_id,
                'action': 'upsert',
                'sequence': int(round(datetime.now().timestamp())),
                'table_name': 'nps',
                'data': {
                    'id': row['id'],
                    'id_legacy': row['id_legacy'],
                    'id_customer': row['id_customer'],
                    'ref_date': row['ref_date'],
                    'survey_date': row['survey_date'],
                    'medium': row['medium'],
                    'respondent': row['respondent'],
                    'score': row['score'],
                    'role': row['role'],
                    'stage': row['stage'],
                    'group': row['group'],
                    'category': row['category'],
                    'nps_status': row['nps_status'],
                    'comments': row['comments'],
                    'tags': row['tags'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                },
                'key_names': [
                    'id'
                ]
            })
        return json.dumps(data_list)

    def _parse_tasks_to_stitch_standard(self, data: list) -> str:
        data_list = []
        for row in data:
            data_list.append({
                'client_id': self.client_id,
                'action': 'upsert',
                'sequence': int(round(datetime.now().timestamp())),
                'table_name': 'tasks',
                'data': {
                    'id': row['id'],
                    'id_legacy': row['id_legacy'],
                    'id_customer': row['id_customer'],
                    'id_parent': row['id_parent'],
                    'id_contact': row['id_contact'],
                    'group': row['group'],
                    'description': row['description'],
                    'notes': row['notes'],
                    'start_date': row['start_date'],
                    'due_date': row['due_date'],
                    'end_date': row['end_date'],
                    'type_id': row['type']['id'],
                    'type_description': row['type']['description'],
                    'type_caption': row['type']['caption'],
                    'type_enabled': row['type']['enabled'],
                    'type_is_default': row['type']['is_default'],
                    'status_id': row['status']['id'],
                    'status_description': row['status']['description'],
                    'priority_id': row['priority']['id'],
                    'priority_description': row['priority']['description'],
                    'owner_id': row['owner']['id'],
                    'owner_name': row['owner']['name'],
                    'owner_username': row['owner']['username'],
                    'owner_email': row['owner']['email'],
                    'owner_profile_name': row['owner']['profile']['name'],
                    'owner_profile_role': row['owner']['profile']['role'],
                    'owner_active': row['owner']['active'],
                    'owner_registered_on': row['owner']['registered_on'],
                    'owner_created_at': row['owner']['created_at'],
                    'owner_updated_at': row['owner']['updated_at'],
                    'created_by_id': row['created_by']['id'],
                    'created_by_name': row['created_by']['name'],
                    'created_by_username': row['created_by']['username'],
                    'created_by_email': row['created_by']['email'],
                    'created_by_profile_name': row['created_by']['profile']['name'],
                    'created_by_profile_role': row['created_by']['profile']['role'],
                    'created_by_active': row['created_by']['active'],
                    'created_by_registered_on': row['created_by']['registered_on'],
                    'created_by_created_at': row['created_by']['created_at'],
                    'created_by_updated_at': row['created_by']['updated_at'],
                    'hours_spent': row['hours_spent'],
                    'hours_planned': row['hours_planned'],
                    'progress': row['progress'],
                    'id_playbook': row['id_playbook'],
                    'id_rule': row['id_rule'],
                    'tags': row['tags'],
                    'created_at': row['created_at'],
                    'system_end_date': row['system_end_date'],
                    'updated_at': row['updated_at'],
                    'custom_value': row['custom_value'],
                    'favorite': row['favorite'],
                },
                'key_names': [
                    'id'
                ]
            })
        return json.dumps(data_list)

    @abc.abstractmethod
    def _parse_custom_fields(self, obj: dict, row: dict):
        for field in row['custom_fields']:
            new_key = f'custom_fields_{field}'
            obj['data'][new_key] = row['custom_fields'][field]['value']
