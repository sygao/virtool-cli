import json
from pathlib import Path

import aiofiles

from virtool_cli.utils.reference import get_isolate_paths, get_sequence_paths


async def get_otu_accessions(otu_path: Path) -> list:
    """
    Gets all accessions from an OTU directory and returns a list

    :param otu_path: Path to an OTU directory under a reference directory
    :return: A list of all accessions under an OTU
    """
    accession_list = []

    for isolate_path in get_isolate_paths(otu_path):
        for sequence_path in get_sequence_paths(isolate_path):
            with open(sequence_path, "r") as f:
                sequence = json.load(f)

            accession_list.append(
                sequence.get('accession')
            )

    return accession_list


async def label_isolates(otu_path: Path) -> dict:
    """
    Return all isolates present in an OTU directory

    :param otu_path: Path to an OTU directory
    :return: A dictionary of isolates indexed by source_name
    """
    if not otu_path.exists():
        raise FileNotFoundError

    isolates = {}

    for iso_path in get_isolate_paths(otu_path):
        async with aiofiles.open(iso_path / "isolate.json", "r") as f:
            contents = await f.read()
            isolate = json.loads(contents)
        isolates[isolate.get("source_name")] = isolate

    return isolates


async def get_otu_accessions_metadata(otu_path: Path) -> dict:
    """
    Returns sequence metadata for all sequences present under an OTU

    :param otu_path: Path to an OTU directory under a reference directory
    :return: An accession-keyed dict containing all constituent sequence metadata
    """
    # get length and segment metadata from sequences
    all_metadata = {}

    for isolate_path in get_isolate_paths(otu_path):
        for sequence_path in get_sequence_paths(isolate_path):
            sequence_metadata = await get_sequence_metadata(sequence_path)

            accession = sequence_metadata["accession"]

            all_metadata[accession] = sequence_metadata

    return all_metadata


async def read_otu(path: Path) -> dict:
    """
    Returns a json file in dict form

    :param path: Path to an OTU directory under a reference source
    :return: Deserialized OTU data in dict form
    """
    async with aiofiles.open(path / "otu.json", "r") as f:
        contents = await f.read()
        otu = json.loads(contents)

    return otu


async def fetch_exclusions(otu_path: Path) -> list:
    async with aiofiles.open(otu_path / "exclusions.json", "r") as f:
        contents = await f.read()
        exclusions = json.loads(contents)

    return exclusions


async def get_sequence_metadata(sequence_path: Path) -> dict:
    """
    Gets the accession length and segment name from a sequence file
    and returns it in a dict

    :param sequence_path: Path to a sequence file
    :return: A dict containing the sequence accession, sequence length and segment name if present
    """
    sequence = await parse_sequence(sequence_path)

    sequence_metadata = {
        "accession": sequence["accession"],
        "length": len(sequence["sequence"]),
    }

    segment = sequence.get("segment", None)
    if segment is not None:
        sequence_metadata["segment"] = segment

    return sequence_metadata


async def parse_sequence(path):
    async with aiofiles.open(path, "r") as f:
        contents = await f.read()
        sequence = json.loads(contents)

    return sequence
