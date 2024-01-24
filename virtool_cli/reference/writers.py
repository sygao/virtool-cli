import json
from pathlib import Path
import asyncio
import structlog

from virtool_cli.utils.reference import search_otu_by_id, get_all_unique_ids
from virtool_cli.utils.id_generator import generate_unique_ids
from virtool_cli.utils.format import format_isolate
from virtool_cli.utils.storage import (
    write_records, label_isolates, store_isolate, store_sequence
)

DEFAULT_INTERVAL = 0.001


class SequenceWriter:
    """
    Writer class for batch reference updates.
    Holds and updates sequence ids and isolate ids.
    """
    def __init__(
        self, src_path: Path, isolate_ids: set | None = None, sequence_ids = set | None
    ):
        self.src_path = src_path
        self.isolate_ids = set() if isolate_ids is None else isolate_ids
        self.sequence_ids = set() if sequence_ids is None else sequence_ids
        self.logger = structlog.get_logger().bind(src_path=str(src_path))

    async def run_loop(self, queue: asyncio.Queue):
        """
        Creates a writing loop which takes packets from the queue and:
            1) Locates the OTU directory, and
            2) Writes the new sequence to path

        :param queue: An asyncio queue containing packets of processed record data
        """
        self.logger.debug("Starting writer...")

        while True:
            packet = await queue.get()
            otu_id, sequence_data = await process_sequence_packet(packet)
            sequence_data = packet["data"]

            logger = self.logger.bind(otu_id=otu_id)

            otu_path = await search_otu_path_by_id(otu_id, self.src_path, logger)
            if not otu_path:
                queue.task_done()
                continue

            logger = logger.bind(otu_path=str(otu_path))

            logger.debug("Writing packet...")
            await self.write_otu_records(
                otu_path=otu_path,
                new_sequences=sequence_data,
                logger=logger
            )

            await asyncio.sleep(DEFAULT_INTERVAL)
            queue.task_done()

    async def write_otu_records(
        self,
        otu_path: Path,
        new_sequences: list[dict],
        logger: structlog.BoundLogger = structlog.get_logger(),
    ) -> list:
        """
        Takes an OTU directory and a list of N sequences to be added under it, and:
            1) Makes a dictionary of isolates keyed by isolate name
            2) Generates N new unique sequence IDs
            3) Determines the isolate path the new sequence goes under
                a) If name exists, retrieves isolate ID from the dictionary in step 1 if found
                b) If name does not exist, generates 1 new isolate ID and creates a new directory
            4) Writes the new sequence under the isolate path

        :param otu_path: A path to an OTU directory under a src reference directory
        :param new_sequences: List of new sequences under the OTU
        :param logger: Optional entry point for a shared BoundLogger
        """
        ref_isolates = await label_isolates(otu_path)

        unassigned_sequence_ids = generate_unique_ids(
            n=len(new_sequences), excluded=list(self.sequence_ids)
        )

        logger.debug(f"Writing {len(new_sequences)} sequences...")

        new_sequence_paths = []
        for seq_data in new_sequences:
            # Assign an isolate path
            isolate_data = seq_data.pop("isolate")
            iso_path, ref_isolates = await self.assign_isolate(
                isolate_data, ref_isolates, otu_path, logger
            )

            # Assign a sequence ID and store sequence
            sequence_id = unassigned_sequence_ids.pop()
            logger.debug(
                "Assigning new sequence",
                seq_hash=sequence_id,
            )

            try:
                await store_sequence(seq_data, sequence_id, iso_path)

            except Exception as e:
                logger.exception(e)

            self.sequence_ids.add(sequence_id)
            sequence_path = iso_path / f"{sequence_id}.json"

            logger.info(
                f"Wrote new sequence '{sequence_id}'", path=str(sequence_path)
            )

            new_sequence_paths.append(sequence_path)

        return new_sequence_paths

    async def assign_isolate(
        self, isolate_data, ref_isolates, otu_path, logger
    ) -> tuple[Path, dict]:
        """
        """
        isolate_name = isolate_data["source_name"]
        isolate_type = isolate_data["source_type"]

        iso_id = self.assign_isolate_id(isolate_name, ref_isolates, logger)

        if not (otu_path / iso_id).exists():
            new_isolate = await init_isolate(
                otu_path, iso_id, isolate_name, isolate_type, logger
            )
            ref_isolates[isolate_name] = new_isolate

            self.isolate_ids.add(iso_id)

            logger.info("Created a new isolate directory", path=str(otu_path / iso_id))

        return otu_path / iso_id, ref_isolates

    def assign_isolate_id(self, isolate_name, ref_isolates, logger) -> str:
        if isolate_name in ref_isolates:
            iso_id = ref_isolates[isolate_name]["id"]
            logger.debug(
                "Existing isolate name found", iso_name=isolate_name, iso_hash=iso_id
            )
            return iso_id

        else:
            try:
                iso_id = generate_unique_ids(n=1, excluded=list(self.isolate_ids)).pop()
                return iso_id
            except Exception as e:
                logger.exception(e)
                return ""


async def cacher_loop(
    src_path: Path,
    cache_path: Path,
    queue: asyncio.Queue,
):
    """
    Awaits new sequence data per OTU and writes new data into one JSON file per OTU

    :param src_path: Path to a reference directory
    :param cache_path: Path to a directory containing cached update lists
    :param queue: Queue holding formatted sequence and isolate data processed by this loop
    """
    logger = structlog.get_logger()
    logger.debug("Starting cache writer...")

    while True:
        packet = await queue.get()
        otu_id, sequence_data = await process_sequence_packet(packet)

        logger = logger.bind(otu_id=otu_id)

        logger.info(f"Packet {otu_id} taken from queue")

        otu_path = await search_otu_path_by_id(otu_id, src_path, logger)
        if not otu_path:
            queue.task_done()
            continue

        logger = logger.bind(otu_path=str(otu_path))
        logger.debug("Writing packet...")

        await cache_new_sequences(sequence_data, otu_id, cache_path)
        cached_update_path = (cache_path / f"{otu_id}.json")
        if cached_update_path.exists():
            logger.debug(
                "Wrote summary to cache.",
                cached_update_path=str((cache_path / f"{otu_id}.json"))
            )

        await asyncio.sleep(DEFAULT_INTERVAL)
        queue.task_done()


async def search_otu_path_by_id(otu_id, src_path, logger):
    otu_path = search_otu_by_id(otu_id, src_path)
    if not otu_path:
        logger.error("OTU path by id not found")

        await asyncio.sleep(DEFAULT_INTERVAL)
        return None

    return otu_path


async def init_isolate(
    otu_path: Path, isolate_id: str, isolate_name: str, isolate_type: str, logger
) -> dict | None:
    """
    """
    new_isolate = format_isolate(isolate_name, isolate_type, isolate_id)

    await store_isolate(new_isolate, isolate_id, otu_path)

    isolate_path = otu_path / f"{isolate_id}/isolate.json"
    if isolate_path.exists():
        logger.info("Created a new isolate directory", path=str(otu_path / isolate_id))
        return new_isolate

    else:
        logger.error("Could not initiate isolate")
        return None


async def process_sequence_packet(packet):
    otu_id = packet["otu_id"]
    sequence_data = packet["data"]

    return otu_id, sequence_data


async def cache_new_sequences(
    processed_updates: list[dict], otu_id: str, cache_path: Path
):
    """
    Takes a list of processed update data and caches it under a given cache path

    :param processed_updates: Preprocessed sequence records
    :param otu_id: Unique OTU identifier
    :param cache_path: Path to a directory containing cached update lists
    """
    summary_path = cache_path / (otu_id + ".json")

    with open(summary_path, "w") as f:
        json.dump(processed_updates, f, indent=2, sort_keys=True)

#
# async def writer_loop(
#     src_path: Path,
#     queue: asyncio.Queue,
# ):
#     """
#     DEPRECATED
#     Awaits new sequence data per OTU and writes new data
#     to the correct location under the reference directory
#
#     :param src_path: Path to a reference directory
#     :param queue: Queue holding formatted sequence and isolate data processed by this loop
#     """
#     logger = structlog.get_logger()
#     logger.debug("Starting writer...")
#
#     unique_iso, unique_seq = await get_all_unique_ids(src_path)
#
#     while True:
#         packet = await queue.get()
#         otu_id, sequence_data = await process_sequence_packet(packet)
#
#         logger = logger.bind(otu_id=otu_id)
#
#         sequence_data = packet["data"]
#
#         otu_path = await get_otu_path(otu_id, src_path, logger)
#         if not otu_path:
#             queue.task_done()
#             continue
#
#         logger = logger.bind(otu_path=str(otu_path))
#
#         logger.debug("Writing packet...")
#         await write_records(
#             otu_path=otu_path,
#             new_sequences=sequence_data,
#             unique_iso=unique_iso,
#             unique_seq=unique_seq,
#             logger=logger
#         )
#
#         await asyncio.sleep(DEFAULT_INTERVAL)
#         queue.task_done()
