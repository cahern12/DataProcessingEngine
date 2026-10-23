import argparse
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.poc_validation import restart_check
parser=argparse.ArgumentParser();parser.add_argument('--workspace',type=Path,required=True)
if __name__=='__main__':print(restart_check(parser.parse_args().workspace)['status'])
