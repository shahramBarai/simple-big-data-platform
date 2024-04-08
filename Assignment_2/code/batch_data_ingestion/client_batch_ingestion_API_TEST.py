import requests, argparse, json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str, help='Set the client ID.')
    parser.add_argument('--server_address', type=str, default='http://127.0.0.1:5000/')
    parser.add_argument('--file_path', type=str, help='Set the path of the dataset to ingest including file extension.')
    return parser.parse_args()

args = parse_args()

server_address = args.server_address+'ingestion'

with open(args.file_path, 'r') as f:
    r = requests.post(server_address, files={'client_id': args.client_id, 'file': f})
print('File submission request:', json.loads(r.text)['message'], "; STATUS:", r.status_code)
