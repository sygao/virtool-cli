import sys
from collections import defaultdict

import structlog

from virtool_cli.ncbi.client import NCBIClient
from virtool_cli.ncbi.model import NCBIGenbank
from virtool_cli.ref.repo import EventSourcedRepo
from virtool_cli.ref.resources import EventSourcedRepoOTU
from virtool_cli.ref.schema import OTUSchema
from virtool_cli.ref.utils import IsolateName, IsolateNameType
from virtool_cli.utils.models import Molecule

logger = structlog.get_logger("otu")


def create_otu_auto(
    repo: EventSourcedRepo,
    taxid: int,
    accessions: list[str],
    ignore_cache: bool = False,
):
    otu_logger = logger.bind(taxid=taxid)

    client = NCBIClient.from_repo(repo.path, ignore_cache)

    taxonomy = client.fetch_taxonomy_record(taxid)
    if taxonomy is None:
        otu_logger.fatal(f"Could not retrieve {taxid} from NCBI Taxonomy")
        return None

    records = client.fetch_genbank_records(accessions)
    if len(records) != len(accessions):
        otu_logger.fatal(f"Could not retrieve all requested accessions.")
        return None

    binned_records = group_genbank_records_by_isolate(records)
    if len(binned_records) > 1:
        otu_logger.fatal(
            f"More than one isolate found. Cannot create schema automatically."
        )
        return None


def create_otu(
    repo: EventSourcedRepo,
    taxid: int,
    ignore_cache: bool = False,
) -> EventSourcedRepoOTU:
    """Initialize a new OTU from a Taxonomy ID."""
    otu_logger = logger.bind(taxid=taxid)

    if repo.get_otu_by_taxid(taxid):
        raise ValueError(
            f"Taxonomy ID {taxid} has already been added to this reference.",
        )

    ncbi = NCBIClient.from_repo(repo.path, ignore_cache)

    taxonomy = ncbi.fetch_taxonomy_record(taxid)

    if taxonomy is None:
        otu_logger.fatal(f"Taxonomy ID {taxid} not found")
        sys.exit(1)

    try:
        otu = repo.create_otu(
            acronym="",
            legacy_id=None,
            name=taxonomy.name,
            schema=None,
            taxid=taxid,
        )

        return otu
    except ValueError as e:
        otu_logger.warning(e)
        sys.exit(1)


def update_otu(
    repo: EventSourcedRepo,
    otu: EventSourcedRepoOTU,
    ignore_cache: bool = False,
):
    """Fetch a full list of Nucleotide accessions associated with the OTU
    and pass the list to the add method.
    """
    ncbi = NCBIClient.from_repo(repo.path, ignore_cache)

    linked_accessions = ncbi.link_accessions_from_taxid(otu.taxid)

    add_sequences(repo, otu, linked_accessions)


def add_sequences(
    repo: EventSourcedRepo,
    otu: EventSourcedRepoOTU,
    accessions: list,
    ignore_cache: bool = False,
):
    """Take a list of accessions, filter for eligible accessions and
    add new sequences to the OTU
    """
    client = NCBIClient.from_repo(repo.path, ignore_cache)

    otu_logger = logger.bind(taxid=otu.taxid, otu_id=str(otu.id), name=otu.name)
    fetch_list = list(set(accessions).difference(otu.blocked_accessions))
    if not fetch_list:
        otu_logger.info("OTU is up to date.")
        return

    otu_logger.info(f"Fetching {len(fetch_list)} accessions...", fetch_list=fetch_list)

    records = client.fetch_genbank_records(fetch_list)

    # if records and not otu.molecule:
    #     # TODO: Upcoming UpdateMolecule event?
    #     molecule = get_molecule_from_records(records)
    #     otu_logger.debug("Retrieved new molecule data", molecule=molecule)

    record_bins = group_genbank_records_by_isolate(records)

    new_accessions = []

    for isolate_key in record_bins:
        record_bin = record_bins[isolate_key]

        isolate_id = otu.get_isolate_id_by_name(isolate_key)
        if isolate_id is None:
            otu_logger.debug(
                f"Creating isolate for {isolate_key.type}, {isolate_key.value}",
            )
            isolate = repo.create_isolate(
                otu_id=otu.id,
                legacy_id=None,
                source_name=isolate_key.value,
                source_type=isolate_key.type,
            )
            isolate_id = isolate.id

        for accession in record_bin:
            record = record_bin[accession]
            if accession in otu.accessions:
                otu_logger.warning(f"{accession} already exists in OTU")
            else:
                sequence = repo.create_sequence(
                    otu_id=otu.id,
                    isolate_id=isolate_id,
                    accession=record.accession,
                    definition=record.definition,
                    legacy_id=None,
                    segment=record.source.segment,
                    sequence=record.sequence,
                )

                new_accessions.append(sequence.accession)

    if new_accessions:
        otu_logger.info(
            f"Added {len(new_accessions)} sequences to {otu.taxid}",
            new_accessions=new_accessions,
        )

    else:
        otu_logger.info("No new sequences added to OTU")


def create_schema_from_records(records: list[NCBIGenbank]) -> OTUSchema | None:
    molecule = get_molecule_from_records(records)

    binned_records = group_genbank_records_by_isolate(records)
    if len(binned_records) > 1:
        logger.fatal(
            "More than one isolate found. Cannot create schema automatically.",
            bins=binned_records,
        )
        return None

    if segments is None:
        segments = _get_segments_from_records(records)

    if segments:
        return OTUSchema(molecule=molecule, segments=segments)

    return None


def get_molecule_from_records(records: list[NCBIGenbank]) -> Molecule:
    """Return relevant molecule metadata from one or more records"""
    if not records:
        raise IndexError("No records given")

    for record in records:
        if record.refseq:
            return Molecule.model_validate(
                {
                    "strandedness": record.strandedness.value,
                    "type": record.moltype.value,
                    "topology": record.topology.value,
                }
            )

    record = records[0]
    return Molecule.model_validate(
        {
            "strandedness": record.strandedness.value,
            "type": record.moltype.value,
            "topology": record.topology.value,
        }
    )


def group_genbank_records_by_isolate(
    records: list[NCBIGenbank],
) -> dict[IsolateName, dict[str, NCBIGenbank]]:
    """Indexes Genbank records by isolate name"""
    isolates = defaultdict(dict)

    for record in records:
        if (isolate_name := _get_isolate_name(record)) is not None:
            isolates[isolate_name][record.accession] = record

    return isolates


def _get_isolate_name(record: NCBIGenbank) -> IsolateName | None:
    """Get the isolate name from a Genbank record"""
    record_logger = logger.bind(
        accession=record.accession,
        definition=record.definition,
        source_data=record.source,
    )

    if record.source.model_fields_set.intersection(
        {IsolateNameType.ISOLATE, IsolateNameType.STRAIN, IsolateNameType.CLONE},
    ):
        for source_type in IsolateNameType:
            if source_type in record.source.model_fields_set:
                return IsolateName(
                    type=IsolateNameType(source_type),
                    value=record.source.model_dump()[source_type],
                )

    elif record.refseq:
        record_logger.debug(
            "RefSeq record does not contain sufficient source data. "
            + "Edit before inclusion."
        )

        return IsolateName(
            type=IsolateNameType(IsolateNameType.REFSEQ),
            value=record.accession,
        )

    record_logger.debug("Record does not contain sufficient source data for inclusion.")

    return None


def _get_segments_from_records(records) -> dict[str, bool]:
    if len(records) == 1:
        record = records[0]

        if record.source.segment != "":
            segment_name = record.source.segment
        else:
            segment_name = record.source.organism

        return {segment_name: True}

    segment_set = set()
    for record in records:
        if record.source.segment:
            segment_set.add(record.source.segment)
        else:
            raise ValueError("No segment name found for multipartite OTU segment.")

    return {segment_name: True for segment_name in segment_set}
