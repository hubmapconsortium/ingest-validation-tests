import pytest
from publication_metadata_validator import PublicationMetadataValidator


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
        "publication_status": True,
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

    @staticmethod
    def mock_raise(url):
        if "bad" in url:
            raise Exception

    @pytest.fixture
    def _mock_validator_good(self, monkeypatch):

        monkeypatch.setattr(PublicationMetadataValidator, "entity_data", self.required_fields)
        monkeypatch.setattr(PublicationMetadataValidator, "uuid", "test_uuid")
        monkeypatch.setattr(
            PublicationMetadataValidator, "_make_request", lambda a, url: self.mock_raise(url)
        )

    def test_collect_errors(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator, "_make_request", lambda a, url: self.mock_raise(url)
            )
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields
                | {
                    "title": None,
                    "description": "",
                    "direct_ancestors": [
                        {
                            "status": "New",
                            "hubmap_id": "test_id",
                            "entity_type": "dataset",
                        },
                    ],
                },
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v._collect_errors()
            assert v.errors == [
                "Missing required fields: title, abstract",
                "Source ID 'test_id' is not published.",
                "Correct any errors by updating https://ingest.hubmapconsortium.org/publication/test_uuid",
            ]

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

    def test_required_bool(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields | {"publication_status": False},
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_required()
            assert v.errors == []

    def test_check_ancestors_good(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_ancestors()
        assert v.errors == []

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
                | {
                    "direct_ancestors": [
                        {"status": "New", "hubmap_id": "test_id", "entity_type": "dataset"}
                    ]
                },
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_ancestors()
            assert v.errors == ["Source ID 'test_id' is not published."]

    def test_check_ancestors_ancestor_not_dataset(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(
                PublicationMetadataValidator,
                "entity_data",
                self.required_fields
                | {
                    "direct_ancestors": [
                        {"status": "Published", "hubmap_id": "test_id", "entity_type": "sample"}
                    ]
                },
            )
            v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
            v.check_ancestors()
            assert v.errors == ["Source ID 'test_id' is not a dataset."]

    def test_get_project(self, _mock_validator_good):
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.sennetconsortium.org"},
        )
        assert v.project == "sennet"
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"},
        )
        assert v.project == "hubmap"

    def test_ingest_ui_link(self, _mock_validator_good):
        v = PublicationMetadataValidator(
            *self.default_args,
            app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"},
        )
        assert v.ingest_ui_link == "https://ingest.hubmapconsortium.org/publication/test_uuid"

    def test_check_urls_no_errors(self, _mock_validator_good):
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_urls()
        assert v.errors == []

    def test_check_urls_error_formatting(self, monkeypatch, _mock_validator_good):
        monkeypatch.setattr(
            PublicationMetadataValidator,
            "entity_data",
            self.required_fields
            | {
                "publication_url": "pub_url_bad_value",
                "publication_doi": "pub_doi_bad_value",
                "omap_doi": "omap_doi_bad_value",
            },
        )
        v = PublicationMetadataValidator(*self.default_args, **self.default_kwargs)
        v.check_urls()
        assert v.errors == [
            "Bad Publication URL 'pub_url_bad_value'.",
            "Bad Publication DOI 'pub_doi_bad_value'.",
            "Bad OMAP DOI 'omap_doi_bad_value'.",
        ]
