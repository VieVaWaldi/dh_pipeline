from pathlib import Path
from typing import Dict

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from dataloader.data_models.digicher_model import (
    ResearchOutputs,
    People,
    Institutions,
    Topics,
    Dois,
    Weblinks,
    Source_Type,
)
from dataloader.dataloader import batch_ingest_documents
from transformer.old.utils.document_standardiser import (
    create_standardised_document,
)
from transformer.old.utils.mapping_info import MappingInfo

# fmt: off
ENTRY = "ns0:entry"
AUTHOR = "ns0:author"
LINK = "ns0:link"
DOI = "ns1:doi"

mapping_configuration: Dict[str, MappingInfo] = {
    # <<< Publications >>> #
    f"{ENTRY}.ns0:id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
    f"{ENTRY}.ns0:title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
    f"{ENTRY}.ns0:published": MappingInfo(ResearchOutputs, ResearchOutputs.publication_date),
    # Only 3k compared to 20k
    f"{ENTRY}.ns1:journal_ref": MappingInfo(ResearchOutputs, ResearchOutputs.journal),
    f"{ENTRY}.ns0:summary": MappingInfo(ResearchOutputs, ResearchOutputs.summary),
    f"{ENTRY}.ns1:comment": MappingInfo(ResearchOutputs, ResearchOutputs.comment),

    # <<< People >>> #
    # You ll have either one or the other ... Thats how the sources work
    f"{ENTRY}.{AUTHOR}": MappingInfo(People, People.name),
    f"{ENTRY}.{AUTHOR}[_].ns0:name": MappingInfo(People, People.name),

    # <<< Institutions >>> #
    f"{ENTRY}.{AUTHOR}.ns1:affiliation": MappingInfo(Institutions, Institutions.name),
    f"{ENTRY}.{AUTHOR}[_].ns1:affiliation": MappingInfo(Institutions, Institutions.name),
    f"{ENTRY}.{AUTHOR}[_].ns1:affiliation[_]": MappingInfo(Institutions, Institutions.name),

    # <<< Topics >>> #
    f"{ENTRY}.ns1:primary_category.@term": MappingInfo(Topics, Topics.name),
    f"{ENTRY}.ns0:category.@term": MappingInfo(Topics, Topics.name),
    f"{ENTRY}.ns0:category[_].@term": MappingInfo(Topics, Topics.original_name_3),

    # <<< Dois >>> #
    f"{ENTRY}.{DOI}": MappingInfo(Dois, Dois.doi),

    # <<< WebLinks >>> #
    f"{ENTRY}.{LINK}[_].@href": MappingInfo(Weblinks, Weblinks.link),
    f"{ENTRY}.{LINK}[_].@title": MappingInfo(Weblinks, Weblinks.name),
}
# fmt: on


def run_transformer(source_path: Path, batch_size: int):
    batch = []
    for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
        standardised_document = create_standardised_document(
            document, mapping_configuration
        )
        batch.append(standardised_document)

        if len(batch) >= batch_size:
            batch_ingest_documents(batch, Source_Type.arxiv)
            batch = []
            print(f"Processed {doc_idx + 1} documents")

    # Don't forget the rest
    if batch:
        batch_ingest_documents(batch, Source_Type.arxiv)


if __name__ == "__main__":
    arxiv_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    run_transformer(arxiv_path, batch_size=10)
