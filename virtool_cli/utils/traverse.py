import json
from pathlib import Path

def get_all_otu_paths(src_path: Path) -> list:
    """
    Generates a list of paths to all OTUs in a src directory.

    :param src_path: Path to a src database directory
    :return: List of paths to all OTU in a src directory
    """
    paths = []

    for alpha in src_path.glob('[a-z]'):
        otu_paths = [otu for otu in alpha.iterdir() if otu.is_dir()]
        paths += otu_paths

    return paths

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