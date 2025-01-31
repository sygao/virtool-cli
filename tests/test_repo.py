import uuid
from pathlib import Path
from uuid import UUID

import orjson
import pytest

from virtool_cli.ref.repo import EventSourcedRepo
from virtool_cli.ref.resources import (
    EventSourcedRepoIsolate,
    EventSourcedRepoOTU,
    EventSourcedRepoSequence,
)
from virtool_cli.ref.utils import DataType, IsolateName, IsolateNameType
from virtool_cli.ref.schema import OTUSchema, Segment
from virtool_cli.utils.models import Molecule, MolType, Strandedness, Topology


@pytest.fixture()
def initialized_repo(empty_repo: EventSourcedRepo):
    otu = empty_repo.create_otu(
        acronym="TMV",
        legacy_id=None,
        name="Tobacco mosaic virus",
        schema=OTUSchema(
            molecule=Molecule(
                strandedness=Strandedness.SINGLE,
                type=MolType.RNA,
                topology=Topology.LINEAR,
            ),
            segments=[Segment(name="A", length=150, required=True)],
        ),
        taxid=12242,
    )

    isolate_a = empty_repo.create_isolate(otu.id, None, "A", IsolateNameType.ISOLATE)
    empty_repo.create_sequence(
        otu.id,
        isolate_a.id,
        "TMVABC",
        "TMV",
        None,
        "RNA",
        "ACGT",
    )

    return empty_repo


def init_otu(empty_repo: EventSourcedRepo) -> EventSourcedRepoOTU:
    return empty_repo.create_otu(
        acronym="TMV",
        legacy_id="abcd1234",
        name="Tobacco mosaic virus",
        schema=OTUSchema(
            molecule=Molecule(
                strandedness=Strandedness.SINGLE,
                type=MolType.RNA,
                topology=Topology.LINEAR,
            ),
            segments=[Segment(name="A", required=True, length=100)],
        ),
        taxid=12242,
    )


def test_new(empty_repo: EventSourcedRepo, tmp_path: Path):
    """Test that creating a new ``Repo`` object returns the expected object and creates
    the expected directory structure.
    """
    assert empty_repo.path == tmp_path / "test_repo"
    assert empty_repo.last_id == 1

    assert empty_repo.meta.data_type == DataType.GENOME
    assert empty_repo.meta.name == "Generic Viruses"
    assert empty_repo.meta.organism == "virus"


class TestCreateOTU:
    def test_ok(self, empty_repo: EventSourcedRepo):
        """Test that creating an OTU returns the expected ``RepoOTU`` object and creates
        the expected event file.
        """
        otu = empty_repo.create_otu(
            acronym="TMV",
            legacy_id="abcd1234",
            name="Tobacco mosaic virus",
            schema=OTUSchema(
                molecule=Molecule(
                    strandedness=Strandedness.SINGLE,
                    type=MolType.RNA,
                    topology=Topology.LINEAR,
                ),
                segments=[Segment(name="A", required=True, length=100)],
            ),
            taxid=12242,
        )

        assert (
            otu.dict()
            == EventSourcedRepoOTU(
                uuid=otu.id,
                acronym="TMV",
                excluded_accessions=None,
                legacy_id="abcd1234",
                name="Tobacco mosaic virus",
                schema=OTUSchema(
                    molecule=Molecule(
                        strandedness=Strandedness.SINGLE,
                        type=MolType.RNA,
                        topology=Topology.LINEAR,
                    ),
                    segments=[Segment(name="A", required=True, length=100)],
                ),
                taxid=12242,
                isolates=[],
            ).dict()
        )

        with open(empty_repo.path.joinpath("src", "00000002.json")) as f:
            event = orjson.loads(f.read())

        del event["timestamp"]

        assert event == {
            "data": {
                "id": str(otu.id),
                "acronym": "TMV",
                "legacy_id": "abcd1234",
                "name": "Tobacco mosaic virus",
                "rep_isolate": None,
                "schema": {
                    "molecule": {
                        "strandedness": "single",
                        "type": "RNA",
                        "topology": "linear",
                    },
                    "segments": [{"length": 100, "name": "A", "required": True}],
                    "multipartite": False,
                },
                "taxid": 12242,
            },
            "id": 2,
            "query": {
                "otu_id": str(otu.id),
            },
            "type": "CreateOTU",
        }

        assert empty_repo.last_id == 2

    def test_duplicate_name(self, empty_repo: EventSourcedRepo):
        """Test that creating an OTU with a name that already exists raises a
        ``ValueError``.
        """
        empty_repo.create_otu(
            acronym="TMV",
            legacy_id=None,
            name="Tobacco mosaic virus",
            schema=OTUSchema(
                molecule=Molecule(
                    strandedness=Strandedness.SINGLE,
                    type=MolType.RNA,
                    topology=Topology.LINEAR,
                ),
                segments=[Segment(name="A", required=True)],
            ),
            taxid=12242,
        )

        with pytest.raises(
            ValueError,
            match="An OTU with the name 'Tobacco mosaic virus' already exists",
        ):
            empty_repo.create_otu(
                acronym="TMV",
                legacy_id=None,
                name="Tobacco mosaic virus",
                schema=OTUSchema(
                    molecule=Molecule(
                        strandedness=Strandedness.SINGLE,
                        type=MolType.RNA,
                        topology=Topology.LINEAR,
                    ),
                    segments=[Segment(name="A", required=True)],
                ),
                taxid=438782,
            )

    def test_duplicate_legacy_id(self, empty_repo: EventSourcedRepo):
        """Test that creating an OTU with a legacy ID that already exists raises a
        ``ValueError``.
        """
        empty_repo.create_otu(
            acronym="TMV",
            legacy_id="abcd1234",
            name="Tobacco mosaic virus",
            schema=OTUSchema(
                molecule=Molecule(
                    strandedness=Strandedness.SINGLE,
                    type=MolType.RNA,
                    topology=Topology.LINEAR,
                ),
                segments=[Segment(name="A", required=True)],
            ),
            taxid=12242,
        )

        with pytest.raises(
            ValueError,
            match="An OTU with the legacy ID 'abcd1234' already exists",
        ):
            empty_repo.create_otu(
                acronym="",
                legacy_id="abcd1234",
                name="Abaca bunchy top virus",
                schema=OTUSchema(
                    molecule=Molecule(
                        strandedness=Strandedness.SINGLE,
                        type=MolType.RNA,
                        topology=Topology.LINEAR,
                    ),
                    segments=[Segment(name="A", required=True)],
                ),
                taxid=438782,
            )


def test_create_isolate(empty_repo: EventSourcedRepo):
    """Test that creating an isolate returns the expected ``RepoIsolate`` object and
    creates the expected event file.
    """
    otu = init_otu(empty_repo)

    isolate = empty_repo.create_isolate(otu.id, None, "A", IsolateNameType.ISOLATE)

    assert isinstance(isolate.id, UUID)
    assert isolate.sequences == []
    assert isolate.name.value == "A"
    assert isolate.name.type == "isolate"

    with open(empty_repo.path.joinpath("src", "00000003.json")) as f:
        event = orjson.loads(f.read())

    del event["timestamp"]

    assert event == {
        "data": {
            "id": str(isolate.id),
            "legacy_id": None,
            "name": {"type": "isolate", "value": "A"},
        },
        "id": 3,
        "query": {
            "otu_id": str(otu.id),
            "isolate_id": str(isolate.id),
        },
        "type": "CreateIsolate",
    }

    assert empty_repo.last_id == 3


def test_create_sequence(empty_repo: EventSourcedRepo):
    """Test that creating a sequence returns the expected ``RepoSequence`` object and
    creates the expected event file.
    """
    otu = init_otu(empty_repo)

    isolate = empty_repo.create_isolate(otu.id, None, "A", IsolateNameType.ISOLATE)

    sequence = empty_repo.create_sequence(
        otu.id,
        isolate.id,
        "TMVABC",
        "TMV",
        None,
        "RNA",
        "ACGT",
    )

    assert sequence == EventSourcedRepoSequence(
        id=sequence.id,
        accession="TMVABC",
        definition="TMV",
        legacy_id=None,
        segment="RNA",
        sequence="ACGT",
    )

    with open(empty_repo.path.joinpath("src", "00000004.json")) as f:
        event = orjson.loads(f.read())

    del event["timestamp"]

    assert event == {
        "data": {
            "id": str(sequence.id),
            "accession": "TMVABC",
            "definition": "TMV",
            "legacy_id": None,
            "segment": "RNA",
            "sequence": "ACGT",
        },
        "id": 4,
        "query": {
            "otu_id": str(otu.id),
            "isolate_id": str(isolate.id),
            "sequence_id": str(sequence.id),
        },
        "type": "CreateSequence",
    }

    assert empty_repo.last_id == 4


class TestRetrieveOTU:
    def test_get_otu(self, empty_repo: EventSourcedRepo):
        """Test that getting an OTU returns the expected ``RepoOTU`` object including two
        isolates with one sequence each.
        """
        otu = empty_repo.create_otu(
            acronym="TMV",
            legacy_id=None,
            name="Tobacco mosaic virus",
            taxid=12242,
            schema=OTUSchema(
                molecule=Molecule(
                    strandedness=Strandedness.SINGLE,
                    type=MolType.RNA,
                    topology=Topology.LINEAR,
                ),
                segments=[Segment(name="A", required=True)],
            ),
        )

        isolate_a = empty_repo.create_isolate(
            otu.id, None, "A", IsolateNameType.ISOLATE
        )
        empty_repo.create_sequence(
            otu.id,
            isolate_a.id,
            "TMVABC",
            "TMV",
            None,
            "RNA",
            "ACGT",
        )

        isolate_b = empty_repo.create_isolate(
            otu.id, None, "B", IsolateNameType.ISOLATE
        )
        empty_repo.create_sequence(
            otu.id,
            isolate_b.id,
            "TMVABCB",
            "TMV",
            None,
            "RNA",
            "ACGTGGAGAGACC",
        )

        otu = empty_repo.get_otu(otu.id)

        otu_contents = [
            EventSourcedRepoIsolate(
                uuid=isolate_a.id,
                legacy_id=None,
                name=IsolateName(type=IsolateNameType.ISOLATE, value="A"),
                sequences=[
                    EventSourcedRepoSequence(
                        id=otu.isolates[0].sequences[0].id,
                        accession="TMVABC",
                        definition="TMV",
                        legacy_id=None,
                        segment="RNA",
                        sequence="ACGT",
                    ),
                ],
            ),
            EventSourcedRepoIsolate(
                uuid=isolate_b.id,
                legacy_id=None,
                name=IsolateName(type=IsolateNameType.ISOLATE, value="B"),
                sequences=[
                    EventSourcedRepoSequence(
                        id=otu.isolates[1].sequences[0].id,
                        accession="TMVABCB",
                        definition="TMV",
                        legacy_id=None,
                        segment="RNA",
                        sequence="ACGTGGAGAGACC",
                    ),
                ],
            ),
        ]

        assert (
            otu.dict()
            == EventSourcedRepoOTU(
                uuid=otu.id,
                acronym="TMV",
                excluded_accessions=[],
                legacy_id=None,
                name="Tobacco mosaic virus",
                schema=OTUSchema(
                    molecule=Molecule(
                        strandedness=Strandedness.SINGLE,
                        type=MolType.RNA,
                        topology=Topology.LINEAR,
                    ),
                    segments=[Segment(name="A", required=True)],
                ),
                taxid=12242,
                isolates=otu_contents,
            ).dict()
        )

        assert empty_repo.last_id == 6

    def test_retrieve_nonexistent_otu(self, initialized_repo: EventSourcedRepo):
        """Test that getting an OTU that does not exist returns ``None``."""
        assert initialized_repo.get_otu(uuid.uuid4()) is None

    def test_get_accessions(self, initialized_repo: EventSourcedRepo):
        otu = list(initialized_repo.get_all_otus(ignore_cache=True))[0]

        assert otu.accessions == {"TMVABC"}

        isolate_b = initialized_repo.create_isolate(
            otu.id, None, "B", IsolateNameType.ISOLATE
        )
        initialized_repo.create_sequence(
            otu.id,
            isolate_b.id,
            "TMVABCB",
            "TMV",
            None,
            "RNA",
            "ACGTGGAGAGACC",
        )

        otu = list(initialized_repo.get_all_otus(ignore_cache=True))[0]

        assert otu.accessions == {"TMVABC", "TMVABCB"}

    def test_get_blocked_accessions(self, initialized_repo: EventSourcedRepo):
        otu = initialized_repo.get_otu_by_taxid(12242)

        isolate_b = initialized_repo.create_isolate(
            otu.id, None, "B", IsolateNameType.ISOLATE
        )
        initialized_repo.create_sequence(
            otu.id,
            isolate_b.id,
            "TMVABCB",
            "TMV",
            None,
            "RNA",
            "ACGTGGAGAGACC",
        )

        initialized_repo.exclude_accession(otu.id, "GROK")
        initialized_repo.exclude_accession(otu.id, "TOK")

        otu = initialized_repo.get_otu(otu.id)

        assert otu.blocked_accessions == {"TMVABC", "TMVABCB", "GROK", "TOK"}


class TestGetIsolate:
    def test_get_isolate(self, initialized_repo: EventSourcedRepo):
        otu = list(initialized_repo.get_all_otus(ignore_cache=True))[0]

        isolate_ids = {isolate.id for isolate in otu.isolates}

        for isolate_id in isolate_ids:
            assert otu.get_isolate(isolate_id) in otu.isolates

    def test_get_isolate_id_by_name(self, initialized_repo: EventSourcedRepo):
        otu = initialized_repo.get_all_otus()[0]

        isolate_ids = {isolate.id for isolate in otu.isolates}

        assert (
            otu.get_isolate_id_by_name(
                IsolateName(type=IsolateNameType.ISOLATE, value="A")
            )
            in isolate_ids
        )


def test_exclude_accession(empty_repo: EventSourcedRepo):
    """Test that excluding an accession from an OTU writes the expected event file and
    returns the expected OTU objects.
    """
    otu = init_otu(empty_repo)

    empty_repo.exclude_accession(otu.id, "TMVABC")

    with open(empty_repo.path.joinpath("src", "00000003.json")) as f:
        event = orjson.loads(f.read())

        del event["timestamp"]

        assert event == {
            "data": {
                "accession": "TMVABC",
            },
            "id": 3,
            "query": {
                "otu_id": str(otu.id),
            },
            "type": "ExcludeAccession",
        }

    assert empty_repo.get_otu(otu.id).excluded_accessions == {
        "TMVABC",
    }

    empty_repo.exclude_accession(otu.id, "ABTV")

    assert empty_repo.get_otu(otu.id).excluded_accessions == {
        "TMVABC",
        "ABTV",
    }
