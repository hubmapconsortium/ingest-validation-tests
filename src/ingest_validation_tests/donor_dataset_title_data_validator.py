from typing import List
from pathlib import Path
from uuid import UUID
import requests
from ingest_validation_tools.plugin_validator import Validator
# get_auth_tok is in ingest-pipeline/src/ingest-pipeline/airflow/dags/utils.py
from cryptography.fernet import Fernet
from airflow.configuration import conf as airflow_conf


def decrypt_tok(crypt_tok: bytes) -> str:
    key = airflow_conf.as_dict(display_sensitive=True)['core']['fernet_key']
    fernet = Fernet(key.encode())
    return fernet.decrypt(crypt_tok).decode()


# get_auth_tok is in ingest-pipeline/src/ingest-pipeline/airflow/dags/utils.py
def get_auth_tok(**kwargs) -> str:
    """
    Recover the authorization token from the environment, and
    decrpyt it.
    """
    crypt_auth_tok = (kwargs['crypt_auth_tok'] if 'crypt_auth_tok' in kwargs
                      else kwargs['dag_run'].conf['crypt_auth_tok'])
    auth_tok = ''.join(e for e in decrypt_tok(crypt_auth_tok.encode())
                       if e.isalnum())  # strip out non-alnum characters
    return auth_tok


def verify_dataset_title_info(dataset_uuid, entity_api_scheme_host, auth_tok) -> List[str]:
    auth_header = {
        'Authorization': f"Bearer {auth_tok}"
    }
    response = requests.get(
        url=f"{entity_api_scheme_host}/datasets/{dataset_uuid}/verifytitleinfo",
        headers=auth_header,
        verify=False
    )
    if response.status_code != 200:
        raise requests.exceptions.RequestException(response.text)

    return response.json()


class DonorDatasetTitleDataValidator(Validator):
    description = "Validate that Donor data required to make a title exists"
    cost = 1.0

    def dataset_uuid_from_path(self) -> str:
        candidate_dataset_uuid = str(Path(self.path).name)
        UUID(candidate_dataset_uuid)  # otherwise throw ValueError
        return candidate_dataset_uuid

    def collect_errors(self, **kwargs) -> List[str]:
        try:
            dataset_uuid = self.dataset_uuid_from_path()
        except ValueError:
            return [f'Dataset path {self.path} does not end in a UUID']

        auth_tok = get_auth_tok(**kwargs)
        entity_api_scheme_host = 'NEEDS TO BE FIGURED OUT'
        return verify_dataset_title_info(dataset_uuid, entity_api_scheme_host, auth_tok)
