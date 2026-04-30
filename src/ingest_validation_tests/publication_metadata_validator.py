import os
import re
from functools import cached_property
from urllib.parse import urljoin, urlsplit

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

    def _collect_errors(self) -> list[str | None]:
        self.check_required()
        self.check_ancestors()
        self.check_urls()
        if self.errors:
            self.errors.append(f"Correct any errors by updating {self.ingest_ui_link}")
        return self._return_result(self.errors, self.assay_type == "publication")

    def check_required(self):
        required_fields = {
            "publication_url": self.entity_data.get("publication_url"),
            "title": self.entity_data.get("title"),
            "publication_venue": self.entity_data.get("publication_venue"),
            "publication_date": self.entity_data.get("publication_date"),
            "publication_status": self.entity_data.get("publication_status"),
            "abstract": self.entity_data.get("description"),
        }
        try:
            assert all([value not in [None, ""] for value in required_fields.values()])
        except AssertionError:
            missing = ", ".join([key for key, val in required_fields.items() if val in ["", None]])
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
        self._check_publication_url()
        self._check_doi()

    def _check_publication_url(self):
        publication_url = self.entity_data.get("publication_url", "")
        try:
            response = self._make_request(publication_url)
            if response.status_code == 403:
                self._check_rxiv_url(publication_url)
            response.raise_for_status()
        except Exception:
            self.errors.append(f"Bad Publication URL '{publication_url}'.")

    def _check_rxiv_url(self, url):
        """
        Automated requests are blocked by biorxiv; parse URL
        and try to check using biorxiv API.
        """
        split_url = urlsplit(url)
        for rxiv in ["biorxiv", "medrxiv"]:
            if rxiv in split_url.netloc:
                url_prefix = f"https://api.biorxiv.org/details/{rxiv}/"
                # theoretically a DOI could start with a digit other than 10 but not yet
                # https://www.doi.org/doi-handbook/html/index.html as of 04/2026
                doi_regex = r"10.+\/.*"
                if match := re.search(doi_regex, split_url.path):
                    response = self._make_request(urljoin(url_prefix, match[0]))
                    if messages := response.json().get("messages"):
                        if not messages[0].get("status") == "ok":
                            self.errors.append(f"Failed {rxiv} API search: {url}.")

    def _check_doi(self):
        """
        Check provided DOI string against doi.org, fallback to crossref API.
        """
        for doi_type, doi in {
            "Publication DOI": self.entity_data.get("publication_doi"),
            "OMAP DOI": self.entity_data.get("omap_doi"),
        }.items():
            if not doi:
                return
            try:
                response = self._make_request(f"https://www.doi.org/{doi}")
                if response.status_code == 403:
                    # automated requests may be blocked, try crossref API
                    # (widely adopted but not universal, which is why we try
                    # doi.org first); mailto included for "polite mode"
                    response = self._make_request(
                        f"https://api.crossref.org/works/doi/{doi}?mailto=help@hubmapconsortium.org"
                    )
                response.raise_for_status()
            except Exception:
                self.errors.append(f"Bad {doi_type} '{doi}'.")

    def _make_request(self, url: str) -> requests.Response:
        return requests.get(url)

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
