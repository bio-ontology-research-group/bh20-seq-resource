import argparse
import time
import arvados
import arvados.collection
import json
import magic
from pathlib import Path
import urllib.request
import socket
import getpass
import sys
import os
import json
import requests
sys.path.insert(0,'.')
from bh20sequploader.qc_metadata import qc_metadata, to_rdf
from bh20sequploader.qc_fasta import qc_fasta

ARVADOS_API_HOST=os.environ.get('ARVADOS_API_HOST', 'cborg.cbrc.kaust.edu.sa')
ARVADOS_API_TOKEN=os.environ.get('ARVADOS_API_TOKEN', '')
UPLOAD_PROJECT='cborg-j7d0g-zcdm4l3ts28ioqo'
ARVADOS_COL_BASE_URI='https://workbench.cborg.cbrc.kaust.edu.sa/collections/'
BORG_COVID_API='https://upload.cborg.cbrc.kaust.edu.sa/api/upload/'

def main():
    parser = argparse.ArgumentParser(description='Upload SARS-CoV-19 sequences for analysis')
    parser.add_argument('sequence', type=argparse.FileType('r'), help='sequence FASTA/FASTQ')
    parser.add_argument('metadata', type=argparse.FileType('r'), help='sequence metadata json')
    parser.add_argument("--validate", action="store_true", help="Dry run, validate only")
    args = parser.parse_args()

    print(ARVADOS_API_HOST, ARVADOS_API_TOKEN)
    api = arvados.api(host=ARVADOS_API_HOST, token=ARVADOS_API_TOKEN, insecure=True)

    try:
        target = qc_fasta(args.sequence)
    except ValueError as e:
        print(e)
        exit(1)

    if not qc_metadata(args.metadata.name):
        print("Failed metadata qc")
        exit(1)

    if args.validate:
        print("Valid")
        exit(0)

    col = arvados.collection.Collection(api_client=api)

    with col.open(target, "w") as f:
        r = args.sequence.read(65536)
        seqlabel = r[1:r.index("\n")]
        print(seqlabel)
        while r:
            f.write(r)
            r = args.sequence.read(65536)
    args.sequence.close()

    print("Reading metadata")
    with col.open("metadata.yaml", "w") as f:
        r = args.metadata.read(65536)
        while r:
            f.write(r)
            r = args.metadata.read(65536)
    args.metadata.close()

    external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')

    try:
        username = getpass.getuser()
    except KeyError:
        username = "unknown"

    properties = {
        "sequence_label": seqlabel,
        "upload_app": "bh20-seq-uploader",
        "upload_ip": external_ip,
        "upload_user": "%s@%s" % (username, socket.gethostname())
    }

    result = col.save_new(owner_uuid=UPLOAD_PROJECT, name="%s uploaded by %s from %s" %
                 (seqlabel, properties['upload_user'], properties['upload_ip']),
                 properties=properties, ensure_unique_name=True)
    response = col.api_response()

    res_uri = ARVADOS_COL_BASE_URI + response['uuid']
    graph = to_rdf(res_uri, args.metadata.name)

    with col.open('metadata.rdf', "wb") as f:
        f.write(graph.serialize(format="pretty-xml"))
    col.save()

    url = BORG_COVID_API + response['uuid'] + "/metadata"
    print(requests.post(url))
    print(json.dumps(response))

if __name__ == "__main__":
    main()
