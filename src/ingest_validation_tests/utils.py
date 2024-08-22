import requests


class GetParentData:
    def __init__(self, hubmap_id, globus_token, app_context):
        self.hubmap_id = hubmap_id
        self.token = globus_token
        self.app_context = app_context

    def __get_uuid(self) -> None:
        url = self.app_context.get("uuid_url") + self.hubmap_id
        headers = self.app_context.get("request_headers", {})
        headers({"Authorization": "Bearer " + self.token})
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            self.uuid = response.json().get("uuid")
        except requests.exceptions.HTTPError as err:
            self.uuid = None
            print(f"Error: {err}")

    def get_path(self) -> str:
        self.__get_uuid()
        if self.uuid is None:
            url = self.app_context.get("ingest_url") + "datasets/" + self.uuid + "/file-system-abs-path"
            headers = self.app_context.get("request_headers", {})
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return response.json().get("path")
            except requests.exceptions.HTTPError as err:
                print(f"Error: {err}")
        return ""
