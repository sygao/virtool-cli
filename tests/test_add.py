import os
import subprocess
from pathlib import Path

import pytest
from syrupy import SnapshotAssertion
from syrupy.filters import props

from virtool_cli.ref.otu import create_otu_with_schema
from virtool_cli.ref.otu import create_otu, update_otu
from virtool_cli.ref.repo import EventSourcedRepo


VIRTOOL_REF = ["virtool", "ref"]


def run_create_otu_command(
    path: Path, taxid: int, accessions: list | None = None, autofill: bool = False
):
    if accessions is None:
        accessions = []

    options = ["--path", str(path)]
    if autofill:
        options.append("--autofill")

    subprocess.run(
        VIRTOOL_REF + ["otu", "create"] + [str(taxid)] + accessions + options,
        check=False,
    )


def run_update_otu_command(taxid: int, path: Path):
    subprocess.run(
        VIRTOOL_REF + ["otu", "update"] + [str(taxid)] + ["--path", str(path)],
        check=False,
    )


def run_add_sequences_command(taxid: int, accessions: list[str], path: Path):
    subprocess.run(
        VIRTOOL_REF
        + ["sequences", "add"]
        + accessions
        + ["--taxid", str(taxid)]
        + ["--path", str(path)],
        check=False,
    )


class TestCreateOTU:
    def test_empty_success(
        self,
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        """Test that an OTU can be created in an empty repository."""
        print(os.listdir(precached_repo.path / "src"))

        otu = create_otu(precached_repo, 345184)

        assert otu.dict() == snapshot(exclude=props("id", "isolates"))

        # Ensure only one OTU is present in the repository, and it matches the return
        # value of the creation function.
        assert precached_repo.get_all_otus() == [otu]

    def test_empty_fail(self, scratch_repo: EventSourcedRepo):
        with pytest.raises(ValueError):
            create_otu(scratch_repo, 345184)

    @pytest.mark.parametrize(
        "taxid, accessions",
        [(1278205, ["NC_020160"]), (345184, ["DQ178610", "DQ178611"])],
    )
    def test_otu_autoschema(
        self,
        taxid: int,
        accessions: list[str],
        precached_repo,
        snapshot: SnapshotAssertion,
    ):
        assert not precached_repo.get_all_otus()

        otu = create_otu_with_schema(
            repo=precached_repo, taxid=taxid, accessions=accessions
        )

        assert precached_repo.get_all_otus()

        assert otu.schema is not None

        assert otu.dict() == snapshot(exclude=props("id"))


class TestCreateOTUCommands:
    @pytest.mark.parametrize(
        "taxid, accessions",
        [(1278205, ["NC_020160"]), (345184, ["DQ178610", "DQ178611"])],
    )
    def test_autoschema(
        self,
        taxid: int,
        accessions: list[str],
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        run_create_otu_command(
            taxid=taxid,
            path=precached_repo.path,
            accessions=accessions,
        )

        otus = EventSourcedRepo(precached_repo.path).get_all_otus()

        assert len(otus) == 1
        otu = otus[0]

        assert otu.dict() == snapshot(exclude=props("id", "isolates"))

    @pytest.mark.ncbi()
    def test_autofill(
        self,
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        run_create_otu_command(
            taxid=345184,
            path=precached_repo.path,
            accessions=["DQ178610", "DQ178611"],
            autofill=True,
        )

        otus = EventSourcedRepo(precached_repo.path).get_all_otus()

        assert len(otus) == 1
        otu = otus[0]

        assert otu.dict() == snapshot(exclude=props("id", "isolates"))
        assert otu.accessions

    @pytest.mark.ncbi()
    def test_autofill_with_schema(
        self,
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        run_create_otu_command(
            taxid=345184,
            accessions=["DQ178610", "DQ178611"],
            path=precached_repo.path,
            autofill=True,
        )

        otus = EventSourcedRepo(precached_repo.path).get_all_otus()

        assert len(otus) == 1
        otu = otus[0]

        assert otu.schema == snapshot
        assert {"DQ178610", "DQ178611"}.intersection(otu.accessions)


class TestAddSequences:
    def test_success(
        self,
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        accessions = ["DQ178614", "DQ178613", "DQ178610", "DQ178611"]
        run_add_sequences_command(345184, accessions, precached_repo.path)

        for otu in precached_repo.get_all_otus():
            assert otu.accessions == set(accessions)

            assert otu.dict() == snapshot(exclude=props("id", "isolates"))

            for isolate in otu.isolates:
                assert isolate.dict() == snapshot(exclude=props("id", "sequences"))

                for accession in sorted(isolate.accessions):
                    assert isolate.get_sequence_by_accession(
                        accession,
                    ).dict() == snapshot(exclude=props("id"))


@pytest.mark.ncbi()
class TestUpdateOTU:
    def test_success_no_exclusions(
        self,
        precached_repo: EventSourcedRepo,
        snapshot: SnapshotAssertion,
    ):
        otu = create_otu(precached_repo, 345184)
        update_otu(precached_repo, otu)

        otu = precached_repo.get_otu(otu.id)

        assert [otu.dict() for otu in precached_repo.iter_otus()] == snapshot(
            exclude=props("id", "isolates"),
        )

        assert otu.accessions == {
            "DQ178608",
            "DQ178609",
            "DQ178610",
            "DQ178611",
            "DQ178612",
            "DQ178613",
            "DQ178614",
            "NC_038792",
            "NC_038793",
        }
