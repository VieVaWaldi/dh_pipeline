from pathlib import Path
from typing import Dict

from common_utils.file_handling.file_parsing.general_parser import yield_all_documents
from data_models.digicher_model import (
    ResearchOutputs,
    People,
    Topics,
    Source_Type,
    FundingProgrammes,
    Institutions,
    Projects,
)
from dataloader.dataloader import batch_ingest_documents
from etl.old.utils.document_standardiser import (
    create_standardised_document,
)
from etl.old.utils.mapping_info import MappingInfo

# fmt: off
PRE = "project.relations.associations"

mapping_configuration: Dict[str, MappingInfo] = {

    # <<< People  >>> #
        # <<< People -from- ResearchOutputs >>> #
        # There are multiple names in here separated with a comma
        "project.relations.associations.result[_].details.authors": MappingInfo(People, People.name, ResearchOutputs),

        # <<< People -from- Institutions >>> #
        f"{PRE}.organization.relations.associations.person.title": MappingInfo(People, People.title, Projects),
        f"{PRE}.organization[_].relations.associations.person.title": MappingInfo(People, People.title, Projects),
        f"{PRE}.organization.relations.associations.person.firstNames": MappingInfo(People, People.meta_first_name, Projects),
        f"{PRE}.organization[_].relations.associations.person.firstNames": MappingInfo(People, People.meta_first_name, Projects),
        f"{PRE}.organization.relations.associations.person.lastName": MappingInfo(People, People.meta_last_name, Projects),
        f"{PRE}.organization[_].relations.associations.person.lastName": MappingInfo(People, People.meta_last_name, Projects),
        f"{PRE}.organization.relations.associations.person.address.telephoneNumber": MappingInfo(People, People.telephone_number, Projects),
        f"{PRE}.organization[_].relations.associations.person.address.telephoneNumber": MappingInfo(People, People.telephone_number, Projects),

    # <<< Topics  >>> #
        # <<< Topics -from- ResearchOutputs >>> #
        f"{PRE}.result.relations.categories.category[_].title": MappingInfo(Topics, Topics.name, ResearchOutputs),
        f"{PRE}.result[_].relations.categories.category[_].title": MappingInfo(Topics, Topics.name, ResearchOutputs),
        f"{PRE}.result.relations.categories.category[_].code": MappingInfo(Topics, Topics.code, ResearchOutputs),
        f"{PRE}.result[_].relations.categories.category[_].code": MappingInfo(Topics, Topics.code, ResearchOutputs),
        # .displayCode.#text -> Is always only one value and fucks with the array
        # f"{PRE}.result.relations.categories.category[_].displayCode.#text": MappingInfo(Topics, Topics.display_code, ResearchOutputs),
        # f"{PRE}.result[_].relations.categories.category[_].displayCode.#text": MappingInfo(Topics, Topics.display_code, ResearchOutputs),
        f"{PRE}.result.relations.categories.category[_].description": MappingInfo(Topics, Topics.description, ResearchOutputs),
        f"{PRE}.result[_].relations.categories.category[_].description": MappingInfo(Topics, Topics.description, ResearchOutputs),
        f"{PRE}.result.relations.categories.category[_].@classification": MappingInfo(Topics, Topics.cordis_classification, ResearchOutputs),
        f"{PRE}.result[_].relations.categories.category[_].@classification": MappingInfo(Topics, Topics.cordis_classification, ResearchOutputs),

        # <<< Topics -from- Projects >>> #
        "project.relations.categories.category[_].title": MappingInfo(Topics, Topics.name, Projects),
        "project.relations.categories.category[_].code": MappingInfo(Topics, Topics.code, Projects),
        # "project.relations.categories.category[_].displayCode.#text": MappingInfo(Topics, Topics.display_code, Projects),
        "project.relations.categories.category[_].description": MappingInfo(Topics, Topics.description, Projects),
        "project.relations.categories.category[_].@classification": MappingInfo(Topics, Topics.cordis_classification, Projects),

    # <<< WebLinks >>> #
    # ToDo: Get Weblinks from Project and Publications

    # <<< Dois >>> #
    # ToDo: Get dois from Project and Publications

    # <<< ResearchOutputs >>> #
    f"{PRE}.result.id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
    f"{PRE}.result[_].id": MappingInfo(ResearchOutputs, ResearchOutputs.id_original),
    # Must be mapped to publication:
    f"{PRE}.result.@type": MappingInfo(ResearchOutputs, ResearchOutputs.type),
    f"{PRE}.result[_].@type": MappingInfo(ResearchOutputs, ResearchOutputs.type),
    # REF DOI:
    # f"{PRE}.result.identifiers.doi": MappingInfo(ResearchOutputs, ResearchOutputs.doi_id),
    # f"{PRE}.result[_].identifiers.doi": MappingInfo(ResearchOutputs, ResearchOutputs.doi_id),
    f"{PRE}.result.title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
    f"{PRE}.result[_].title": MappingInfo(ResearchOutputs, ResearchOutputs.title),
    # Is contentUpdateDate not published:
    f"{PRE}.result.contentUpdateDate": MappingInfo(ResearchOutputs, ResearchOutputs.publication_date),
    f"{PRE}.result[_].contentUpdateDate": MappingInfo(ResearchOutputs, ResearchOutputs.publication_date),
    f"{PRE}.result.details.journalTitle": MappingInfo(ResearchOutputs, ResearchOutputs.journal),
    f"{PRE}.result[_].details.journalTitle": MappingInfo(ResearchOutputs, ResearchOutputs.journal),
    f"{PRE}.result.description": MappingInfo(ResearchOutputs, ResearchOutputs.summary),
    f"{PRE}.result[_].description": MappingInfo(ResearchOutputs, ResearchOutputs.summary),
    f"{PRE}.result.teaser": MappingInfo(ResearchOutputs, ResearchOutputs.comment),
    f"{PRE}.result[_].teaser": MappingInfo(ResearchOutputs, ResearchOutputs.comment),

    # <<< Institutions >>> #
        # <<< Institutions -from- Projects >>> #
        f"{PRE}.organization.legalName": MappingInfo(Institutions, Institutions.name, Projects),
        f"{PRE}.organization[_].legalName": MappingInfo(Institutions, Institutions.name, Projects),
        f"{PRE}.organization.@sme": MappingInfo(Institutions, Institutions.sme, Projects),
        f"{PRE}.organization[_].@sme": MappingInfo(Institutions, Institutions.sme, Projects),
        f"{PRE}.organization.address.street": MappingInfo(Institutions, Institutions.address_street, Projects),
        f"{PRE}.organization[_].address.street": MappingInfo(Institutions, Institutions.address_street, Projects),
        f"{PRE}.organization.address.postBox": MappingInfo(Institutions, Institutions.address_postbox, Projects),
        f"{PRE}.organization[_].address.postBox": MappingInfo(Institutions, Institutions.address_postbox, Projects),
        f"{PRE}.organization.address.postalCode": MappingInfo(Institutions, Institutions.address_postalcode, Projects),
        f"{PRE}.organization[_].address.postalCode": MappingInfo(Institutions, Institutions.address_postalcode, Projects),
        f"{PRE}.organization.address.city": MappingInfo(Institutions, Institutions.address_city, Projects),
        f"{PRE}.organization[_].address.city": MappingInfo(Institutions, Institutions.address_city, Projects),
        f"{PRE}.organization.address.country": MappingInfo(Institutions, Institutions.address_country, Projects),
        f"{PRE}.organization[_].address.country": MappingInfo(Institutions, Institutions.address_country, Projects),
        f"{PRE}.organization.address.geolocation": MappingInfo(Institutions, Institutions.address_geolocation, Projects),
        f"{PRE}.organization[_].address.geolocation": MappingInfo(Institutions, Institutions.address_geolocation, Projects),
        f"{PRE}.organization.address.url": MappingInfo(Institutions, Institutions.url, Projects),
        f"{PRE}.organization[_].address.url": MappingInfo(Institutions, Institutions.url, Projects),
        f"{PRE}.organization.shortName": MappingInfo(Institutions, Institutions.short_name, Projects),
        f"{PRE}.organization[_].shortName": MappingInfo(Institutions, Institutions.short_name, Projects),
        f"{PRE}.organization.vatNumber": MappingInfo(Institutions, Institutions.vat_number, Projects),
        f"{PRE}.organization[_].vatNumber": MappingInfo(Institutions, Institutions.vat_number, Projects),

        # <<< Institutions -from- ResearchOutputs >>> #
        f"{PRE}.result[_].relations.associations.organization.legalName": MappingInfo(Institutions, Institutions.name, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.street": MappingInfo(Institutions, Institutions.address_street, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.postalCode": MappingInfo(Institutions, Institutions.address_postalcode, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.city": MappingInfo(Institutions, Institutions.address_city, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.country": MappingInfo(Institutions, Institutions.address_country, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.geolocation": MappingInfo(Institutions, Institutions.address_geolocation, ResearchOutputs),
        f"{PRE}.result[_].relations.associations.organization.address.url": MappingInfo(Institutions, Institutions.url, ResearchOutputs),

    # <<< Funding Programmes >>> #
    f"{PRE}.programme.code": MappingInfo(FundingProgrammes, FundingProgrammes.code),
    f"{PRE}.programme[_].code": MappingInfo(FundingProgrammes, FundingProgrammes.code),
    f"{PRE}.programme.title": MappingInfo(FundingProgrammes, FundingProgrammes.title),
    f"{PRE}.programme[_].title": MappingInfo(FundingProgrammes, FundingProgrammes.title),
    f"{PRE}.programme.shortTitle": MappingInfo(FundingProgrammes, FundingProgrammes.short_title),
    f"{PRE}.programme[_].shortTitle": MappingInfo(FundingProgrammes, FundingProgrammes.short_title),
    f"{PRE}.programme.frameworkProgramme": MappingInfo(FundingProgrammes, FundingProgrammes.framework_programme),
    f"{PRE}.programme[_].frameworkProgramme": MappingInfo(FundingProgrammes, FundingProgrammes.framework_programme),
    f"{PRE}.programme.pga": MappingInfo(FundingProgrammes, FundingProgrammes.pga),
    f"{PRE}.programme[_].pga": MappingInfo(FundingProgrammes, FundingProgrammes.pga),
    f"{PRE}.programme.rcn": MappingInfo(FundingProgrammes, FundingProgrammes.rcn),
    f"{PRE}.programme[_].rcn": MappingInfo(FundingProgrammes, FundingProgrammes.rcn),

    # <<< Projects >>> #
    "project.id": MappingInfo(Projects, Projects.id_original),
    # REF "project.identifiers.grantDoi": MappingInfo(Projects, Projects.doi_id),
    # REF "": MappingInfo(Projects, Projects.funding_programme_id),
    "project.acronym": MappingInfo(Projects, Projects.acronym),
    "project.title": MappingInfo(Projects, Projects.title),
    "project.status": MappingInfo(Projects, Projects.status),
    "project.startDate": MappingInfo(Projects, Projects.start_date),
    "project.endDate": MappingInfo(Projects, Projects.end_date),
    "project.ecSignatureDate": MappingInfo(Projects, Projects.ec_signature_date),
    "project.totalCost": MappingInfo(Projects, Projects.total_cost),
    "project.ecMaxContribution": MappingInfo(Projects, Projects.ec_max_contribution),
    "project.objective": MappingInfo(Projects, Projects.objective),

    f"{PRE}.call.identifier": MappingInfo(Projects, Projects.call_identifier),
    f"{PRE}.call.title": MappingInfo(Projects, Projects.call_title),
    f"{PRE}.call.rcn": MappingInfo(Projects, Projects.call_rcn)
}
# fmt: on


def run_transformer(source_path: Path, batch_size: int):
    batch = []
    for doc_idx, (document, path) in enumerate(yield_all_documents(source_path)):
        standardised_document = create_standardised_document(
            document, mapping_configuration
        )
        batch.append(standardised_document)

        # is_valid, errors = validate_document(transformed_document)
        # if not is_valid:
        #     print(f"Document {doc_idx} validation errors: {errors}")
        #     continue
        # Track here like the error jsons {} from claude

        if len(batch) >= batch_size:
            batch_ingest_documents(batch, Source_Type.cordis)
            batch = []
            print(f"Processed {doc_idx + 1} documents")

    # Dont forget the rest
    if batch:
        batch_ingest_documents(batch, Source_Type.cordis)


if __name__ == "__main__":
    core_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/cordis_culturalORheritage"
    )
    batch_size = 10
    run_transformer(core_path, batch_size)
