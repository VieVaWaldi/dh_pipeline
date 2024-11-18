from dataloader.data_models.digicher_model import (
    ResearchOutputs,
    People,
    Topics,
    Projects,
)
from transformer.utils.document_standardiser import create_standardised_document
from transformer.utils.mapping_info import MappingInfo


class TestDataTransformer:
    def test_simple_single_value(self):
        """Test basic mapping of a single value to a table"""
        document = {"id": "paper123", "title": "Test Paper"}

        mapping = {
            "id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
            "title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
        }

        expected = {
            ResearchOutputs.__tablename__: [
                {"id_original": "paper123", "title": "Test Paper"}
            ]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    def test_nested_structure(self):
        """Test handling of nested document structure"""
        document = {"paper": {"metadata": {"id": "paper123", "title": "Test Paper"}}}

        mapping = {
            "paper.metadata.id": MappingInfo(
                ResearchOutputs, ResearchOutputs.id_original
            ),
            "paper.metadata.title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
        }

        expected = {
            ResearchOutputs.__tablename__: [
                {"id_original": "paper123", "title": "Test Paper"}
            ]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    """
    ToDo:
    - AI is overwritten by ML because AI has no list_idx and ML gets list_idx 0. 
    
    Expected :{'topics': [{'name': 'AI'}, {'name': 'ML'}, {'name': 'NLP'}]}
    Actual   :{'topics': [{'name': 'ML'}, {'name': 'NLP'}]}
    """
    def test_multiple_mappings_same_target(self):
        """Test multiple source fields mapping to same target field"""
        document = {"paper": {"primary_topic": "AI", "secondary_topics": ["ML", "NLP"]}}

        mapping = {
            "paper.primary_topic": MappingInfo(Topics, Topics.name),
            "paper.secondary_topics[_]": MappingInfo(Topics, Topics.name),
        }

        expected = {
            Topics.__tablename__: [{"name": "AI"}, {"name": "ML"}, {"name": "NLP"}]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    """
    ToDo:
    -  
    """

    def test_relationship_preservation(self):
        """Test preservation of relationships in nested structures"""
        document = {
            "authors": [
                {"name": "John Doe", "topics": ["ai", "math"]},
                {"name": "Jane Smith", "topics": ["literature"]},
            ]
        }

        mapping = {
            "authors[_].name": MappingInfo(People, People.name),
            "authors[_].telephone_number[_]": MappingInfo(
                People, People.telephone_number
            ),
        }

        expected = {
            People.__tablename__: [
                {"name": "John Doe", "telephone_number": "123"},
                {"name": "John Doe", "telephone_number": "456"},
                {"name": "Jane Smith", "telephone_number": "789"},
            ]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    def test_belongs_to_relationship(self):
        """Test belongs_to relationships between tables"""
        document = {
            "project": {
                "id": "proj123",
                "title": "Big Project",
                "publications": [
                    {"id": "paper1", "topics": "AI"},
                    {"id": "paper2", "topics": "NLP"},
                ],
            }
        }

        mapping = {
            "project.id": MappingInfo(Projects, Projects.id),
            "project.title": MappingInfo(Projects, Projects.title),
            "project.publications[_].id": MappingInfo(
                ResearchOutputs, ResearchOutputs.id_original
            ),
            "project.publications[_].topics": MappingInfo(
                Topics, Topics.name, belongs_to=ResearchOutputs
            ),
        }

        expected = {
            Projects.__tablename__: [{"id": "proj123", "title": "Big Project"}],
            ResearchOutputs.__tablename__: [
                {"id_original": "paper1"},
                {"id_original": "paper2"},
            ],
            Topics.__tablename__: {
                ResearchOutputs.__tablename__: [{"name": "AI"}, {"name": "NLP"}]
            },
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    def test_handling_null_values(self):
        """Test handling of null/missing values"""
        document = {
            "paper": {
                "id": "paper123",
                "summary": "",
                "missing_field": None,
            }
        }

        mapping = {
            "paper.id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
            "paper.summary": MappingInfo(ResearchOutputs, ResearchOutputs.summary),
            "paper.missing_field": MappingInfo(ResearchOutputs, ResearchOutputs.title),
        }

        expected = {
            ResearchOutputs.__tablename__: [
                {"id_original": "paper123", "summary": "", "title": "None"}
            ]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    def test_deep_nesting_with_arrays(self):
        """Test handling of deeply nested structures with arrays"""
        document = {
            "data": {
                "papers": [
                    {
                        "metadata": {
                            "authors": [
                                {
                                    "info": {
                                        "name": "John Doe",
                                        "titles": ["Univ A", "Univ B"],
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }

        mapping = {
            "data.papers[_].metadata.authors[_].info.name": MappingInfo(
                People, People.name
            ),
            "data.papers[_].metadata.authors[_].info.titles[_]": MappingInfo(
                People, People.title
            ),
        }

        expected = {
            People.__tablename__: [
                {"name": "John Doe", "affiliation": "Univ A"},
                {"name": "John Doe", "affiliation": "Univ B"},
            ]
        }

        result = create_standardised_document(document, mapping)
        assert result == expected

    def test_real_world_arxiv_example(self):
        """Test with a real-world Arxiv document structure"""
        document = {
            "ns0:entry": {
                "ns0:id": "http://arxiv.org/abs/2408.04270v1",
                "ns0:title": "Analysis Paper",
                "ns0:author": [{"ns0:name": "John Doe"}, {"ns0:name": "Jane Smith"}],
                "ns1:primary_category": {"@term": "cs.CL"},
                "ns0:category": {"@term": "cs.AI"},
            }
        }

        mapping = {
            "ns0:entry.ns0:id": MappingInfo(
                ResearchOutputs, ResearchOutputs.id_original
            ),
            "ns0:entry.ns0:title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
            "ns0:entry.ns0:author[_].ns0:name": MappingInfo(People, People.name),
            "ns0:entry.ns1:primary_category.@term": MappingInfo(Topics, Topics.name),
            "ns0:entry.ns0:category.@term": MappingInfo(Topics, Topics.name),
        }

        expected = {
            ResearchOutputs: [
                {
                    "id_original": "http://arxiv.org/abs/2408.04270v1",
                    "title": "Analysis Paper",
                }
            ],
            People: [{"name": "John Doe"}, {"name": "Jane Smith"}],
            Topics: [{"name": "cs.CL"}, {"name": "cs.AI"}],
        }

        result = create_standardised_document(document, mapping)
        assert result == expected
