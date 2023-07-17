from pathlib import Path
import structlog

from virtool_cli.utils.traverse import get_otu_data, rm_r


def run(src_path):
    flatten_src(src_path)

def flatten_src(src_path):
    """
    """
    letter_bins = [alpha for alpha in src_path.glob('[a-z]')]

    for alpha in letter_bins:
        otu_paths = [otu for otu in alpha.iterdir() if otu.is_dir()]
        
        for otu_path in otu_paths:
            new_name = generate_otu_name(
                old_name=otu_path.name, 
                otu_id=get_otu_data(otu_path)['_id'])
            new_path = src_path / new_name
            otu_path.rename(new_path)
            print(new_path)

        rm_r(alpha)
    
def generate_otu_name(old_name: str, otu_id: str):
    return old_name + '--' + otu_id


if __name__ == '__main__':
    TEST_DIR = Path(__file__).parents[1] / 'tests/files/src_migrate'
    print(TEST_DIR)
    run(TEST_DIR)