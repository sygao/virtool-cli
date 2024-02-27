import os
from Bio import Entrez

from structlog import get_logger
from urllib.parse import quote_plus
from urllib.error import HTTPError

from virtool_cli.ncbi.error import IncompleteRecordsError, NCBIParseError
from virtool_cli.ncbi.utils import parse_nuccore
from virtool_cli.ncbi.cache import NCBICache

Entrez.email = os.environ.get("NCBI_EMAIL")
Entrez.api_key = os.environ.get("NCBI_API_KEY")

DEFAULT_INTERVAL = 0.001

base_logger = get_logger()


class NCBIClient:
    def __init__(self, repo):
        self.repo = repo
        self.cache = NCBICache(repo.path / ".cache/ncbi")

    async def fetch_from_otu_id(self, otu_id: str, use_cached=True):
        logger = base_logger.bind(otu_id=otu_id)

        taxon_id = self.repo.get_otu_by_id(otu_id).taxid

        records = await self._retrieve_records(otu_id, taxon_id, use_cached)

        for record in records:
            try:
                ncbi_sequence, ncbi_source = parse_nuccore(record)
            except NCBIParseError as e:
                logger.error(f"Parse failure: {e}")
                continue

            print(ncbi_sequence)
            print(ncbi_source)

    async def fetch_from_taxon_id(self, taxon_id: int, use_cached=True):
        logger = base_logger.bind(taxid=taxon_id)

        otu_id = self.repo.maps.taxid_to_otu_id[taxon_id]

        records = await self._retrieve_records(otu_id, taxon_id, use_cached)

        for record in records:
            try:
                ncbi_sequence, ncbi_source = parse_nuccore(record)
            except NCBIParseError as e:
                logger.error(f"Parse failure: {e}")
                continue

            print(ncbi_sequence)
            print(ncbi_source)

    async def _retrieve_records(self, otu_id: str, taxon_id: int, use_cached=True):
        records = None
        if use_cached:
            records = self.cache.load_records(otu_id)

        if not records:
            accessions = await self.link_accessions(taxon_id)
            records = await self.fetch_accessions(accessions)

        return records

    @staticmethod
    async def link_accessions(taxon_id: int) -> list:
        """
        Requests a cross-reference for NCBI Taxonomy and Nucleotide via ELink
        and returns the results as a list.

        :param taxon_id: A NCBI Taxonomy ID
        :return: A list of accessions linked to the Taxon Id
        """
        elink_results = Entrez.read(
            Entrez.elink(
                dbfrom="taxonomy",
                db="nuccore",
                id=str(taxon_id),
                idtype="acc",
            )
        )

        if not elink_results:
            return []

        # Discards unneeded tables and formats needed table as a list
        for link_set_db in elink_results[0]["LinkSetDb"]:
            if link_set_db["LinkName"] == "taxonomy_nuccore":
                id_table = link_set_db["Link"]

                return [keypair["Id"] for keypair in id_table]

    async def fetch_accessions(self, accessions: list[str]) -> list[dict]:
        """
        Take a list of accession numbers, download the corresponding records
        from GenBank as XML and return the parsed records

        :param accessions: List of accession numbers to fetch from GenBank
        :return: A list of deserialized sequence records from NCBI Nucleotide
        """
        if not accessions:
            return []

        logger = base_logger.bind(accessions=accessions)

        try:
            records = self._fetch_serialized_records(accessions)
            return records

        except IncompleteRecordsError as e:
            logger.error(e.message)

            if e.data:
                logger.debug("Partial results fetched, returning results...")
                return e.data

        except HTTPError as e:
            if e.code == 400:
                logger.error(f"{e}. Bad accessions?")
            else:
                logger.error(e)

        return []

    async def fetch_accession(self, accession: str) -> dict:
        """
        A wrapper for the fetching of a single accession
        """
        record = await self.fetch_accessions([accession])

        if record:
            return record[0]

    @staticmethod
    def _fetch_serialized_records(accessions: list) -> list[dict] | None:
        """
        Requests XML GenBank records for a list of accessions
        and returns an equal-length list of serialized records.

        Raises an error if fewer records are fetched than accessions.

        :param accessions: A list of n accessions
        :return: A list of n deserialized records
        """
        with Entrez.efetch(
            db="nuccore", id=accessions, rettype="gb", retmode="xml"
        ) as f:
            records = Entrez.read(f)

        # Handle cases where not all accessions can be fetched
        if len(records) == len(accessions):
            return records

        raise IncompleteRecordsError("Bad accession in list", data=records)

    async def fetch_taxonomy(self, taxon_id: int) -> dict:
        """Requests a taxonomy record from NCBI Taxonomy"""
        return await self._fetch_taxon_long(taxon_id)

    @staticmethod
    def _fetch_raw_records(accessions: list) -> str:
        """
        Requests XML GenBank records for a list of accessions
        and returns results as unparsed XML

        :param accessions: A list of accessions
        :return: XML data as an unparsed string
        """
        with Entrez.efetch(
            db="nuccore", id=accessions, rettype="gb", retmode="xml"
        ) as f:
            raw_records = f.read()

        return raw_records

    @staticmethod
    async def fetch_taxonomy_id_by_name(name: str) -> int | None:
        """Returns a best-guess taxon ID for a given OTU name.

        Searches the NCBI taxonomy database for a given OTU name, then fetches and
        returns its taxonomy.

        Returns ``None`` if no matching taxonomy is found.

        :param name: the name of an otu
        :return: The taxonomy id for the given otu name
        """
        with Entrez.esearch(db="taxonomy", term=name) as f:
            record = Entrez.read(f)

        try:
            taxid = int(record["IdList"][0])
        except IndexError:
            return None

        return taxid

    @staticmethod
    async def _fetch_taxon_docsum(taxon_id: int):
        record = Entrez.read(
            Entrez.efetch(
                db="taxonomy",
                id=taxon_id,
                rettype="docsum",
                retmode="xml",
            )
        )

        return record[0]

    @staticmethod
    async def _fetch_taxon_long(taxon_id: int) -> dict:
        with Entrez.efetch(db="taxonomy", id=taxon_id, rettype="null") as f:
            record = Entrez.read(f)

        return record[0]

    async def fetch_taxon_rank(self, taxon_id: int) -> str:
        taxonomy = await self._fetch_taxon_docsum(taxon_id)

        return taxonomy["Rank"]

    async def fetch_species_taxid(self, taxid: int) -> int | None:
        """Gets the species taxid for the given lower-rank taxid.

        :param taxid: NCBI Taxonomy UID
        :return: The NCBI Taxonomy ID of the OTU's species
        """
        taxonomy = await self._fetch_taxon_long(taxid)

        if taxonomy["Rank"] == "species":
            return int(taxonomy["TaxId"])

        for line in taxonomy["LineageEx"]:
            if line["Rank"] == "species":
                return int(line["TaxId"])

        return None

    @staticmethod
    async def check_spelling(name: str, db: str = "taxonomy") -> str:
        """Takes the name of an OTU, requests an alternative spelling
        from the Entrez ESpell utility and returns the suggestion

        :param name: The OTU name that requires correcting
        :param db: Database to check against. Defaults to 'taxonomy'.
        :return: String containing NCBI-suggested spelling changes
        """
        with Entrez.espell(db=db, term=quote_plus(name)) as f:
            record = Entrez.read(f)

        if record:
            return record["CorrectedQuery"]

        return name
