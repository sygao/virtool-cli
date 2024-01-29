from pathlib import Path
import json


def generate_taxid_table(src_path: Path) -> dict:
    """
    Creates a dictionary of taxid: otu_path key-pairs

    :param src_path:
    """
    taxid_table = {}
    for otu_path in src_path.iterdir():
        if not is_otu_directory(otu_path):
            continue

        with open(otu_path / "otu.json", "r") as f:
            otu = json.load(f)

        taxid = int(otu.get('taxid', -1))
        if taxid > 0:
            taxid_table[taxid] = otu_path.name

    return taxid_table


def is_otu_directory(path: Path) -> bool:
    if path.is_dir:
        if (path / "otu.json").exists():
            return True

    return False
