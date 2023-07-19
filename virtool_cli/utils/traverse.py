import json
from pathlib import Path


def get_alphabins(src_path: Path) -> list:
    """
    Generates a list of paths to all alphabetical bins.
    """
    return [alphabin for alphabin in src_path.glob('[a-z]') if alphabin.is_dir()] 

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

if __name__ == '__main__':
    REPO_DIR = Path.home() / "Development/UVic/Virtool/Repositories/"
    repo_path = REPO_DIR / 'ref-mini'
    src_path = repo_path / 'src'

    otu_paths = get_all_otu_paths(src_path)

    for otu_path in otu_paths:
        print(otu_path.relative_to(repo_path))
    