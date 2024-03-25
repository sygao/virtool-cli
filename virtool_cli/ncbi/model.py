from enum import StrEnum

from pydantic import BaseModel, Field, AliasChoices, field_validator, computed_field


class NCBIDatabase(StrEnum):
    NUCCORE = "nuccore"
    TAXONOMY = "taxonomy"


class NCBIRank(StrEnum):
    SPECIES = "species"
    ISOLATE = "isolate"


class NCBISource(BaseModel):
    taxid: int = Field(validation_alias="db_xref")
    organism: str
    mol_type: str
    isolate: str = ""
    host: str = ""
    segment: str = ""
    strain: str = ""
    clone: str = ""

    @field_validator("taxid", mode="before")
    @classmethod
    def db_xref_to_taxid(cls, raw: str) -> int:
        return int(raw.split(":")[1])


class NCBINuccore(BaseModel):
    accession: str = Field(validation_alias="GBSeq_primary-accession")
    definition: str = Field(validation_alias="GBSeq_definition")
    sequence: str = Field(validation_alias="GBSeq_sequence")
    source: NCBISource = Field(validation_alias="GBSeq_feature-table")
    comment: str = Field("", validation_alias="GBSeq_comment")

    @field_validator("sequence", mode="after")
    @classmethod
    def to_uppercase(cls, raw: str) -> str:
        return raw.upper()

    @field_validator("source", mode="before")
    @classmethod
    def create_source(cls, raw: list) -> NCBISource:
        for feature in raw:
            if feature["GBFeature_key"] == "source":
                return NCBISource(
                    **{
                        qual["GBQualifier_name"]: qual["GBQualifier_value"]
                        for qual in feature["GBFeature_quals"]
                    }
                )

        raise ValueError("Feature table contains no ``source`` table.")


class NCBILineage(BaseModel):
    id: int = Field(validation_alias=AliasChoices("id", "TaxId"))
    name: str = Field(validation_alias=AliasChoices("name", "ScientificName"))
    rank: str = Field(validation_alias=AliasChoices("rank", "Rank"))


class NCBITaxonomy(BaseModel):
    id: int = Field(validation_alias=AliasChoices("id", "TaxId"))
    name: str = Field(
        validation_alias=AliasChoices("name", "ScientificName"),
    )
    lineage: list[NCBILineage] = Field(
        validation_alias=AliasChoices("lineage", "LineageEx"),
    )
    rank: NCBIRank = Field(validation_alias=AliasChoices("rank", "Rank"))

    @computed_field
    def species(self) -> NCBILineage:
        if self.rank is NCBIRank.SPECIES:
            return NCBILineage(id=self.id, name=self.name, rank=self.rank)

        for item in self.lineage:
            if item.rank == "species":
                return item

    @field_validator("lineage", mode="before")
    @classmethod
    def create_lineage_list(cls, raw: list[dict]):
        lineage = []
        for level_data in raw:
            level = NCBILineage(**level_data)
            lineage.append(level)

        return lineage

    @field_validator("rank", mode="before")
    @classmethod
    def validate_rank(cls, raw: str):
        return NCBIRank(raw)
