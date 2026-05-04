from csv import DictReader
from pathlib import Path

import requests


class GetParentData:
    def __init__(self, hubmap_id, globus_token, app_context):
        self.hubmap_id = hubmap_id
        self.token = globus_token
        self.app_context = app_context

    def __get_uuid(self) -> None:
        url = self.app_context.get("uuid_url") + self.hubmap_id
        headers = self.app_context.get("request_headers", {})
        headers.update({"Authorization": "Bearer " + self.token})
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            self.uuid = response.json().get("uuid")
        except requests.exceptions.HTTPError as err:
            self.uuid = None
            print(f"Error: {err}")

    def get_path(self) -> str:
        self.__get_uuid()
        if self.uuid is not None:
            url = (
                self.app_context.get("ingest_url")
                + "datasets/"
                + self.uuid
                + "/file-system-abs-path"
            )
            headers = self.app_context.get("request_headers", {})
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return response.json().get("path")
            except requests.exceptions.HTTPError as err:
                print(f"Error: {err}")
        return ""


def get_non_global_paths_by_row(rows: list[dict]) -> dict[int, str]:
    """
    Create dict of non-global paths by row for a shared upload.
    {0: [<path_1>, <path_2>], 1: [<path_3>, <path_4>]}
    """
    files_by_row = {}
    for i, row in enumerate(rows):
        if non_global_files := row.get("non_global_files"):
            files_by_row[i] = [file.strip() for file in non_global_files.split(";")]
    return files_by_row


def read_tsv(path: Path, encoding: str = "utf-8") -> list[dict]:
    with open(path, encoding=encoding) as f:
        rows = list(DictReader(f, dialect="excel-tab"))
        f.close()
    return rows
