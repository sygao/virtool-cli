from pathlib import Path
import json
import asyncio
import structlog

from virtool_cli.utils.logging import configure_logger
from virtool_cli.utils.reference import is_v1, get_all_unique_ids
from virtool_cli.reference.writers import SequenceWriter

DEFAULT_INTERVAL = 0.001

base_logger = structlog.get_logger()


def run(
    cache_path: Path,
    src_path: Path,
    auto_evaluate: bool = False,
    debugging: bool = False,
):
    """
    CLI entry point for update.uncache.run()

    Reads pre-processed update data from "otu_id"-labelled files in a cache directory
    and writes the contents to a reference directory

    :param cache_path: Path to a directory containing cached update lists
    :param src_path: Path to a reference directory
    :param auto_evaluate: Auto-evaluation flag, enables automatic filtering for fetched results
    :param debugging: Enables verbose logs for debugging purposes
    """
    configure_logger(debugging)
    logger = base_logger.bind(src=str(src_path))

    if is_v1(src_path):
        logger.error(
            'reference folder "src" is a deprecated v1 reference.'
            + 'Run "virtool ref migrate" before trying again.'
        )
        return

    if auto_evaluate:
        logger.warning(
            "Auto-evaluation unavailable at present"
        )

    logger.info("Updating src directory accessions...")

    asyncio.run(
        update_reference_from_cache(
            cache_path=cache_path,
            src_path=src_path,
            auto_evaluate=auto_evaluate
        )
    )


async def update_reference_from_cache(
    cache_path: Path, src_path: Path, auto_evaluate: bool = False
):
    """
    Reads pre-processed update data from "otu_id"-labelled files in a cache directory
    and writes the contents to a reference directory

    :param cache_path: Path to a directory containing cached update lists
    :param src_path: Path to a reference directory
    :param auto_evaluate: Auto-evaluation flag, enables automatic filtering for fetched results
    """
    # Holds raw NCBI GenBank data
    queue = asyncio.Queue()

    # Deserializes cache files and feeds the contents to the queue
    loader = asyncio.create_task(
        loader_loop(cache_path=cache_path, queue=queue)
    )

    # Create UpdateWriter for this session
    unique_iso, unique_seq = await get_all_unique_ids(src_path)
    update_writer = SequenceWriter(
        src_path=src_path, isolate_ids=unique_iso, sequence_ids=unique_seq
    )

    # Pull formatted sequences from queue, checks isolate metadata
    # and write JSON to the correct location in the src directory
    asyncio.create_task(
        update_writer.run_loop(queue)
    )

    await asyncio.gather(*[loader], return_exceptions=True)

    await queue.join()

    return


async def loader_loop(
    cache_path: Path, queue: asyncio.Queue
):
    """
    Deserializes cache files and feeds the contents to the queue.
    Cache files must be named using the "{otu_id}.json" rubric.

    :param cache_path: Path to a directory containing cached update lists
    :param queue: Queue holding fetched NCBI GenBank data
    """
    logger = structlog.get_logger(__name__ + ".fetcher")
    logger.debug("Starting loader...")

    for path in cache_path.glob("*.json"):
        otu_id = path.stem

        logger = logger.bind(otu_id = otu_id)
        logger.debug(f"Loading {otu_id}...")

        with open(path, "r") as f:
            sequence_data = json.load(f)

        packet = {
            "otu_id": otu_id,
            "data": sequence_data,
        }

        await queue.put(packet)
        logger.debug(
            f"Pushed {len(sequence_data)} requests to queue",
            n_requests=len(sequence_data)
        )
        await asyncio.sleep(DEFAULT_INTERVAL)