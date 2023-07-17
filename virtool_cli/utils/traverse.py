import json
from pathlib import Path

def get_otu_data(otu_path: Path) -> dict:
    """
    Returns data from an OTU's otu.json
    """

    with open(otu_path / "otu.json", "r") as f:
        otu = json.load(f)

    return otu

def rm_r(path):
    """
    """
    try:
        for chaff in path.iterdir():
            chaff.unlink()
    except Exception as e:
        return e
    path.rmdir()