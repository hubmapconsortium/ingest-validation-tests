import pytest
import requests
from publication_metadata_validator import PublicationMetadataValidator


class MockConstraintsResponseGood:

    @staticmethod
    def json():
        return {
            "code": 200,
            "description": [
                {
                    "code": 200,
                    "description": [
                        {"entity_type": "dataset", "sub_type": None, "sub_type_val": None}
                    ],
                }
            ],
        }

    @staticmethod
    def raise_for_status():
        response = MockConstraintsResponseGood.json()
        if response["code"] == 200:
            return
        raise Exception


class MockConstraintsResponseBad:

    @staticmethod
    def json():
        return {
            "code": 400,
            "description": [
                {
                    "code": 200,
                    "description": [
                        {"entity_type": "dataset", "sub_type": None, "sub_type_val": None}
                    ],
                },
                {
                    "code": 404,
                    "description": [
                        {"entity_type": "sample", "sub_type": ["Organ"], "sub_type_val": None}
                    ],
                },
            ],
        }

    @staticmethod
    def raise_for_status():
        response = MockConstraintsResponseBad.json()
        if response["code"] == 200:
            return
        raise Exception


class TestPublicationMetadataValidator:

    required_fields = {
        "entity_type": "Publication",
        "direct_ancestors": [
            {"hubmap_id": "test_id1", "entity_type": "Dataset", "status": "Published"}
        ],
        "publication_url": "pub_url_value",
        "publication_doi": "pub_doi_value",
        "title": "title_value",
        "publication_venue": "pub_venue_value",
        "publication_date": "pub_date_value",
        "publication_status": "pub_status_value",
        "description": "abstract_value",
        "uuid": "test_uuid",
        "omap_doi": "omap_doi_value",
    }
    default_args = [
        "tmp_path",
        "required_type",
    ]
    default_kwargs = {
        "app_context": {
            "ingest_url": "https://ingest_url",
            "constraints_url": "https://test_constraints_url",
            "entities_url": "https://entities",
        }
    }

    @pytest.fixture
    def _mock_validator_good(self, monkeypatch):

        def mock_post(*args, **kwargs):
            return MockConstraintsResponseGood()

        def mock_raise(url):
            if "bad" in url:
                raise Exception

        monkeypatch.setattr(PublicationMetadataValidator, "entity_data", self.required_fields)
        monkeypatch.setattr(PublicationMetadataValidator, "uuid", "test_uuid")
        monkeypatch.setattr(
            PublicationMetadataValidator, "_make_request", lambda a, url: mock_raise(url)
        )
        monkeypatch.setattr(requests, "post", mock_post)

    def test_required_present(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_required()
        assert v.errors == []

    def test_required_missing(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields | {"title": None, "description": ""},
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_required()
            assert v.errors == ["Missing required fields: title, abstract"]

    def test_check_ancestors_good(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_ancestors()
        assert v.errors == []

    def test_check_ancestors_no_constraints_url(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args)
        v.check_ancestors()
        assert v.errors == ["Constraints URL is missing from app_context, can't check Source IDs."]

    def test_check_ancestors_none(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields | {"direct_ancestors": []},
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_ancestors()
            assert v.errors == ["Publication has no Source IDs (required)."]

    def test_check_ancestors_ancestor_not_published(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields
                | {"direct_ancestors": [{"status": "New", "hubmap_id": "test_id"}]},
            )
            m.setattr(
                PublicationMetadataValidator,
                "_make_constraints_check",
                lambda a, b: None,
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_ancestors()
            assert v.errors == ["Source ID 'test_id' is not published."]

    def test_constraints_check_good(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v._make_constraints_check({})
        assert v.errors == []

    @pytest.mark.parametrize(
        ("errors", "req_fields_update"),
        (
            (
                ["Invalid ancestor(s) for Publication. Sample: ['test_id2']"],
                {
                    "direct_ancestors": [
                        {"hubmap_id": "test_id1", "entity_type": "Dataset", "status": "Published"},
                        {"hubmap_id": "test_id2", "entity_type": "Sample", "status": "Published"},
                    ]
                },
            ),
            # multiple different types of bad ancestors
            (
                ["Invalid ancestor(s) for Publication. Sample: ['test_id2']"],
                {
                    "direct_ancestors": [
                        {"hubmap_id": "test_id1", "entity_type": "Dataset", "status": "Published"},
                        {"hubmap_id": "test_id2", "entity_type": "Sample", "status": "Published"},
                    ]
                },
            ),
        ),
    )
    def test_constraints_check_bad(self, req_fields_update, errors, monkeypatch):

        def mock_post(*args, **kwargs):
            return MockConstraintsResponseBad()

        monkeypatch.setattr(
            PublicationMetadataValidator, "entity_data", self.required_fields | req_fields_update
        )
        monkeypatch.setattr(requests, "post", mock_post)

        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v._make_constraints_check({})
        assert v.errors == errors

    def test_get_project(self, _mock_validator_good):
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.sennetconsortium.org"}
        )
        assert v.project == "sennet"
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"}
        )
        assert v.project == "hubmap"

    def test_ingest_ui_link(self, _mock_validator_good):
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"}
        )
        assert v.ingest_ui_link == "https://ingest.hubmapconsortium.org/publication/test_uuid"

    def test_check_urls_no_errors(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_urls()
        assert v.errors == []

    def test_check_urls_error_formatting(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.publication_url = "pub_url_bad_value"
        v.publication_doi = "pub_doi_bad_value"
        v.omap_doi = "omap_doi_bad_value"
        v.check_urls()
        assert v.errors == [
            "Bad Publication URL: pub_url_bad_value.",
            "Bad Publication DOI: pub_doi_bad_value.",
            "Bad OMAP DOI: omap_doi_bad_value.",
        ]
