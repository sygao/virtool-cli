import json
from pathlib import Path

from virtool_cli.utils.id_generator import generate_unique_ids
from virtool_cli.utils.format import format_isolate


async def init_isolate(
    otu_path: Path, isolate_id: str, isolate_name: str, isolate_type: str, logger
) -> dict | None:
    """
    """
    new_isolate = format_isolate(
        isolate_name, isolate_type, isolate_id
    )

    await store_isolate(new_isolate, isolate_id, otu_path)

    isolate_path = otu_path / f"{isolate_id}/isolate.json"
    if isolate_path.exists():
        logger.info("Created a new isolate directory", path=str(otu_path / isolate_id))
        return new_isolate

    else:
        logger.error("Could not initiate isolate")
        return None


def assign_isolate_id(isolate_name, ref_isolates, unique_iso, logger) -> str:
    if isolate_name in ref_isolates:
        iso_id = ref_isolates[isolate_name]["id"]
        logger.debug(
            "Existing isolate name found", iso_name=isolate_name, iso_hash=iso_id
        )
        return iso_id

    else:
        try:
            iso_id = generate_unique_ids(n=1, excluded=list(unique_iso)).pop()
            return iso_id
        except Exception as e:
            logger.exception(e)
            return ""


def extract_isolates(sequence_data):
    isolate_names = set()
    for data in sequence_data:
        source_name = data['isolate']['source_name']
        if source_name in isolate_names:
            isolate_names.add(source_name)

    return list(isolate_names)


async def store_isolate(
    isolate: dict, isolate_id: str, otu_path: Path
):
    """
    Creates a new isolate directory and metadata file under an OTU directory,
    then returns the metadata in dict form

    :param isolate: Dictionary containing isolate metadata
    :param isolate_id: Unique ID number for this new isolate
    :param otu_path: Path to the parent OTU
    :return: The unique isolate id
    """
    iso_path = otu_path / isolate_id
    iso_path.mkdir()

    with open(iso_path / "isolate.json", "w") as f:
        json.dump(isolate, f, indent=4)


async def store_sequence(sequence: dict, sequence_id: str, iso_path: Path):
    """
    Write sequence to isolate directory within the src directory

    :param sequence: Dictionary containing formatted sequence data
    :param sequence_id: Unique ID number for this new sequence
    :param iso_path: Path to the parent isolate
    :return: The unique sequence id (aka seq_hash)
    """
    sequence["_id"] = sequence_id
    seq_path = iso_path / f"{sequence_id}.json"

    with open(seq_path, "w") as f:
        json.dump(sequence, f, indent=4, sort_keys=True)
