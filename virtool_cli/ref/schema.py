from pydantic import BaseModel, computed_field

from virtool_cli.utils.models import Molecule


class OTUSchema(BaseModel):
    """A schema for the intended data"""

    molecule: Molecule

    segments: dict[str, bool]

    @computed_field
    def multipartite(self) -> bool:
        if len(self.segments) < 2:
            return False

        return True
