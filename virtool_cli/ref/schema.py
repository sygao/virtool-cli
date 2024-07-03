from pydantic import BaseModel, computed_field

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
