import os
from functools import cached_property
from urllib.parse import urljoin

import requests
from validator import Validator


class PublicationMetadataValidator(Validator):
    """
    Test for some common errors in the metadata for publications.
    """

    description = "Test for common problems found in publication metadata"
    cost = 1.0
    version = "1.0"
    required = ["publication"]

    def __init__(self, base_paths, assay_type, *args, **kwargs):
        super().__init__(base_paths, assay_type, *args, **kwargs)
        self.errors: list = []
        self.publication_url = self.entity_data.get("publication_url", "")
        self.publication_doi = self.entity_data.get("publication_doi", "")
        self.omap_doi = self.entity_data.get("omap_doi", "")

    def _collect_errors(self) -> list[str | None]:
        self.check_required()
        self.check_ancestors()
        self.check_urls()
        if self.errors:
            self.errors.append(f"Correct any errors by updating {self.ingest_ui_link}")
        return self._return_result(self.errors, self.assay_type == "publication")

    def check_required(self):
        required_fields = {
            "publication_url": self.publication_url,
            "title": self.entity_data.get("title"),
            "publication_venue": self.entity_data.get("publication_venue"),
            "publication_date": self.entity_data.get("publication_date"),
            "publication_status": self.entity_data.get("publication_status"),
            "abstract": self.entity_data.get("description"),
        }
        try:
            assert all(required_fields.values())
        except AssertionError:
            missing = ", ".join([key for key, val in required_fields.items() if not val])
            self.errors.append(f"Missing required fields: {missing}")

    def check_ancestors(self):
        ancestors = self.entity_data.get("direct_ancestors", [])
        # check required source_ids are present
        if len(ancestors) == 0:
            self.errors.append("Publication has no Source IDs (required).")
            return
        for ancestor in ancestors:
            # make sure ancestor is published
            if ancestor.get("status", "").lower() != "published":
                self.errors.append(
                    f"Source ID '{ancestor.get(f'{self.project}_id')}' is not published."
                )
            # make sure ancestor is dataset (should be replaced with
            # constraints endpoint when implemented)
            if ancestor.get("entity_type", "").lower() != "dataset":
                self.errors.append(
                    f"Source ID '{ancestor.get(f'{self.project}_id')}' is not a dataset."
                )

    def check_urls(self):
        try:
            self._make_request(self.publication_url)
        except Exception:
            self.errors.append(f"Bad Publication URL '{self.publication_url}'.")
        for doi_data in [
            [self.publication_doi, "Publication DOI", "https://api.crossref.org/works/"],
            [self.omap_doi, "OMAP DOI", "https://purl.humanatlas.io/omap/"],
        ]:
            self._check_doi(*doi_data)

    def _check_doi(self, doi: str, doi_type: str, url: str):
        if not doi:
            return
        try:
            if doi.startswith("http"):
                self._make_request(doi)
            else:
                self._make_request(f"{url}{doi}")
        except Exception:
            self.errors.append(f"Bad {doi_type} '{doi}'.")

    def _make_request(self, url: str):
        response = requests.get(url)
        response.raise_for_status()

    @property
    def ingest_ui_link(self) -> str:
        # prod only
        return f"https://ingest.{self.project}consortium.org/publication/{self.entity_data.get('uuid')}"

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

    @property
    def uuid(self) -> str:
        for elt in reversed(str(self.paths[0]).split(os.sep)):
            if len(elt) == 32 and all([c in "0123456789abcdef" for c in list(elt)]):
                return elt
        raise Exception("no uuid was found in the path to the current working directory")

    @property
    def project(self) -> str:
        # Defaults to HuBMAP
        project_url = self.app_context.get("ingest_url", "")
        if "sennet" in project_url:
            proj = "sennet"
        else:
            proj = "hubmap"
        return proj
