import os
import asyncio
from urllib.error import HTTPError
from Bio import Entrez, SeqIO

Entrez.email = os.environ.get("NCBI_EMAIL")
Entrez.api_key = os.environ.get("NCBI_API_KEY")

NCBI_REQUEST_INTERVAL = 0.3 if Entrez.email and Entrez.api_key else 0.8


async def fetch_taxid(name: str) -> int:
    """
    Searches the NCBI taxonomy database for a given OTU name, 
    then fetches and returns its taxon id. 
    Returns None if no taxon id is found.

    :param name: Name of a given OTU
    :return: Taxonomy id for the given OTU
    """
    try:
        handle = Entrez.esearch(db="taxonomy", term=name)
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        return e

    try:
        taxid = int(record["IdList"][0])
    except IndexError:
        taxid = None

    return taxid

async def fetch_accession_uids(accessions: list) -> dict:
    """
    Creates a dictionary keyed by the input accessions,
    fetches the corresponding UIDs from NCBI Genbank and 
    returns as many key-value pairs as possible.
    If a UID cannot be found for an accession, value defaults to None.add()

    :param name: List of accession numbers
    :return: Dictionary of Genbank UIDs keyed by corresponding accession number
    """
    indexed_accessions = { accession: None for index, accession in enumerate(accessions) }

    try:
        handle = Entrez.efetch(db="nucleotide", id=accessions, rettype="docsum")
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        raise e
    
    await asyncio.sleep(NCBI_REQUEST_INTERVAL)

    for r in record:
        if r.get('Caption') in indexed_accessions.keys():
            indexed_accessions[r.get('Caption')] = int(r.get('Id'))
        elif r.get('AccessionVersion') in indexed_accessions.keys():
            indexed_accessions[r.get('AccessionVersion')] = int(r.get('Id'))
    
    return indexed_accessions

async def fetch_docsums(fetch_list: list) -> list:
    """
    Take a list of accession numbers and requests documents from NCBI GenBank
    
    :param fetch_list: List of accession numbers to fetch from GenBank
    :param logger: Structured logger

    :return: A list of GenBank data converted from XML to dicts if possible, 
        else an empty list
    """
    try:
        handle = Entrez.efetch(db="nucleotide", id=fetch_list, rettype="docsum")
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        return []
    
    await asyncio.sleep(NCBI_REQUEST_INTERVAL)

    return record

async def fetch_taxonomy_rank(taxon_id):
    """
    """
    try:
        handle = Entrez.efetch(db="taxonomy", id=taxon_id, rettype="null")
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        return []
    
    taxid = ''
    for r in record:
        taxid = r['Rank']
    return taxid

async def fetch_upstream_record_taxids(fetch_list: list) -> list:
    """
    Take a list of accession numbers and request the records from NCBI GenBank
    
    :param fetch_list: List of accession numbers to fetch from GenBank
    :param logger: Structured logger

    :return: A list of GenBank data converted from XML to dicts if possible, 
        else an empty list
    """
    try:
        handle = Entrez.efetch(db="nucleotide", id=fetch_list, rettype="docsum")
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        return []
    
    taxids = set()

    for r in record:
        taxids.add(int(r.get('TaxId')))
    
    await asyncio.sleep(NCBI_REQUEST_INTERVAL)
    
    return list(taxids)

# async def process_records(records, listing: dict):
#     """
#     """
#     otu_updates = []
#     for seq_list in records:
#         for seq_data in seq_list:
#             [ accession, version ] = (seq_data.id).split('.')
            
#             if accession in filter_set:
#                 continue

#             otu_updates.

#     return otu_updates