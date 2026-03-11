import os
from functools import cached_property
from urllib.parse import urljoin

import requests
from validator import Validator


class PublicationMetadataValidator(Validator):
    """
    Test for some common errors in the metadata for publications.
    """

    description = "Test for common problems found in publication metadata."
    cost = 1.0
    version = "1.0"
    required = ["publication"]

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.description += f"Correct any errors by updating {self.ingest_ui_link}"
        self.errors = []
        self.source_ids = self.entity_data.get("source_ids")
        self.publication_url = self.entity_data.get("publication_url")
        self.publication_doi = self.entity_data.get("publication_doi")
        self.omap_doi = self.entity_data.get("omap_doi")

    @property
    def ingest_ui_link(self) -> str:
        return f"https://ingest.{self.project}consortium.org/publication/{self.entity_data.get('uuid')}"

    @property
    def project(self) -> str:
        # Defaults to HuBMAP
        project_url = self.app_context.get("ingest_url", "")
        if "sennet" in project_url:
            proj = "sennet"
        else:
            proj = "hubmap"
        return proj

    def _collect_errors(self) -> list[str | None]:
        self._check_required()
        self._check_ancestors()
        self._check_urls()
        self._check_dois()
        return self._return_result(self.errors, self.assay_type == "publication")

    @cached_property
    def entity_data(self) -> dict:
        headers = {
            "authorization": f"Bearer {self.token}",
            "content-type": "application/json",
            f"X-{self.project}-Application": "ingest-pipeline",
        }
        url = urljoin(
            self.app_context["entities_url"],
            f"{self.uuid}?exclude=direct_ancestors.files",
        )
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def _check_required(self):
        required_fields = {
            "source_ids": self.source_ids,
            "publication_url": self.publication_url,
            "title": self.entity_data.get("title"),
            "publication_venue": self.entity_data.get("publication_venue"),
            "publication_date": self.entity_data.get("publication_date"),
            "publication_status": self.entity_data.get("publication_status"),
            "abstract": self.entity_data.get("abstract"),
        }
        try:
            assert all(required_fields.values())
        except AssertionError:
            missing = ", ".join([key for key, val in required_fields.items() if not val])
            self.errors.append(f"Missing required fields: {missing}")

    def _check_ancestors(self):
        # check status is published
        # check against constraints endpoint
        pass

    def _check_urls(self):
        pass

    def _check_dois(self):
        pass

    @property
    def uuid(self) -> str:
        for elt in reversed(str(self.paths[0]).split(os.sep)):
            if len(elt) == 32 and all([c in "0123456789abcdef" for c in list(elt)]):
                return elt
        raise RuntimeError("no uuid was found in the path to the current" " working directory")
