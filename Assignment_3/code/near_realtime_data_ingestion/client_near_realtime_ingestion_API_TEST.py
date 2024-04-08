import requests, argparse, json, time
import datetime
import pandas as pd


# A common function to convert timestamp to string
def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()


def parse_args():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_address', type=str, default='http://127.0.0.1:5000/')
    parser.add_argument('-id', '--client_id', type=str, help='Set the client ID.')
    parser.add_argument('-i', '--input_file', help='Input file', default='../../data/data.csv')
    parser.add_argument('-c', '--chunksize', help='Chunk size for big file', default=90)
    parser.add_argument('-s', '--sleep_time', help='Sleep time in seconds')
    return parser.parse_args()

args = parse_args()

server_address = args.server_address+'stream_ingestion'

if __name__ == '__main__':
    # Read large input file in chunks
    INPUT_DATA_FILE = args.input_file
    chunk_size = int(args.chunk_size)
    sleep_time = int(args.sleep_time)

    if args.client_id == 'client1':
        input_data = pd.read_csv(INPUT_DATA_FILE, parse_dates=['time'], iterator=True, chunksize=chunk_size)
    elif args.client_id == 'client2':
        input_data = pd.read_csv(INPUT_DATA_FILE, sep=",", iterator=True,  chunksize=chunk_size)
    else:
        print(f"{args.client_id} is not accessible!")

    # Process each chunk of input data
    for chunk_data in input_data:
        chunk = chunk_data.dropna()
        for index, row in chunk.iterrows():
            # Convert the data row to a JSON string
            json_data = json.dumps(row.to_dict(), default=datetime_converter)
            # Send the data via API
            print(f'Send: {json_data}')
            r = requests.post(server_address, files={'client_id': args.client_id, 'data': json_data})
            print('File submission request:', json.loads(r.text)['message'], "; STATUS:", r.status_code)
            # Wait for some time before sending the next chunk, for emulation
            time.sleep(sleep_time)
            print("")
