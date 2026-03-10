from publication_metadata_validator import PublicationMetadataValidator


class TestPublicationMetadataValidator:

    required_fields = {
        "source_ids": ["id1", "id2"],
        "publication_url": "pub_url_value",
        "title": "title_value",
        "publication_venue": "pub_venue_value",
        "publication_date": "pub_date_value",
        "publication_status": "pub_status_value",
        "abstract": "abstract_value",
        "uuid": "test_uuid",
    }

    def test_required_present(self, monkeypatch):
        with monkeypatch.context() as m:
            m.setattr(PublicationMetadataValidator, "entity_data", self.required_fields)
            v = PublicationMetadataValidator(
                ["tmp_path"],
                "required_type",
                app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"},
            )
            v._check_required()
            assert v.errors == []

    def test_required_missing(self, monkeypatch):
        required_fields_bad = self.required_fields.copy() | {"title": None, "abstract": ""}
        with monkeypatch.context() as m:
            m.setattr(PublicationMetadataValidator, "entity_data", required_fields_bad)
            v = PublicationMetadataValidator(
                ["tmp_path"],
                "required_type",
                app_context={"ingest_url": "https://ingest.api.hubmapconsortium.org"},
            )
            v._check_required()
            assert v.errors == ["Missing required fields: title, abstract"]
