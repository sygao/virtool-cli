from pydantic import BaseModel, computed_field

from utils.models import Molecule
from virtool_cli.utils.models import Molecule


class OTUSchema(BaseModel):
    """A schema for the intended data"""

    molecule: Molecule
    """The molecular metadata for this OTU."""

    segments: dict[str, bool]
    """The segments contained in this OTU."""

    @computed_field
    def multipartite(self) -> bool:
        if len(self.segments) < 2:
            return False

        return True

    @classmethod
    def build_from_scratch(
        cls, strandedness: str, moltype: str, topology: str, segments: dict[str, bool]
    ):
        return OTUSchema(
            molecule=Molecule.model_validate(
                {"strandedness": strandedness, "type": moltype, topology: "topology"}
            ),
            segments=segments,
        )
