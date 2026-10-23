"""Read-only public retail snapshot retrieval; requires no Azure credentials."""
import argparse
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import ROOT,read_json,write_json
from dpe.pricing import retrieve


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--filters-from',type=Path,default=ROOT/'benchmarks/pricing/azure-eastus-2026-10-22')
    args=parser.parse_args()
    if args.out.exists():raise ValueError('use a fresh price snapshot directory')
    index=[]
    for name in ('vm','storage','aks','network','nat'):
        original=read_json(args.filters_from/name/'snapshot.json')
        records=retrieve(args.out/name,original['filter'],original['currency'])
        index.append({'service':name,'records':len(records),'metadata':name+'/snapshot.json'})
    write_json(args.out/'index.json',{'scope':'Microsoft Azure public retail; no deployment or billing access','services':index})
