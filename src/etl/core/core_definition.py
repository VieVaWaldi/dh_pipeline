from pathlib import Path
from typing import Dict

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from dataloader.data_models.digicher_model import (
    ResearchOutputs,
    People,
    Topics,
    Dois,
    Weblinks,
    Source_Type,
)
from dataloader.dataloader import batch_ingest_documents
from transformer.utils.document_standardiser import extract_mapped_fields, MappingInfo

document_mapping: Dict[str, MappingInfo] = {
    # <<< Publications >>> #
    "id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
    "arxivId": MappingInfo(ResearchOutputs, ResearchOutputs.arxiv_id),
    "title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
    "publishedDate": MappingInfo(ResearchOutputs, ResearchOutputs.publication_date),
    "journals[_].title": MappingInfo(ResearchOutputs, ResearchOutputs.journal),
    "abstract": MappingInfo(ResearchOutputs, ResearchOutputs.abstract),
    "fullText": MappingInfo(ResearchOutputs, ResearchOutputs.full_text),
    "documentType": MappingInfo(ResearchOutputs, ResearchOutputs.comment),
    # language.name # ToDo: Use That
    # references  # ToDo: Use That
    # <<< People >>> #
    # ToDo: Check having both doesnt cause issues, should add them all to a single list
    "authors[_].name": MappingInfo(People, People.name),
    # <<< Institutions >>> #
    # None
    # <<< Topics >>> #
    # Use primary and secondary topic here! Maybe Alchemy will enforce that anyway with junctions
    "fieldOfStudy": MappingInfo(Topics, Topics.name),
    # <<< Dois >>> #
    "doi": MappingInfo(Dois, Dois.doi),
    # <<< WebLinks >>> #
    "links[_].type": MappingInfo(Weblinks, Weblinks.name),
    "links[_].url": MappingInfo(Weblinks, Weblinks.link),
}


def run_transformer(source_path: Path, batch_size: int):
    batch = []
    for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
        transformed_document = extract_mapped_fields(document, document_mapping)
        batch.append(transformed_document)

        # is_valid, errors = validate_document(transformed_document)
        # if not is_valid:
        #     print(f"Document {doc_idx} validation errors: {errors}")
        #     continue
        # Track here like the error jsons {} from claude

        if len(batch) >= batch_size:
            batch_ingest_documents(batch, Source_Type.core)
            batch = []
            print(f"Processed {doc_idx + 1} documents")

    # Dont forget the rest
    if batch:
        batch_ingest_documents(batch, Source_Type.core)


if __name__ == "__main__":
    core_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB"
    )
    batch_size = 10
    run_transformer(core_path, batch_size)
