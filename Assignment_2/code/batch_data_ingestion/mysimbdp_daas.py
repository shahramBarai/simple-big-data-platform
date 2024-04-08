from flask import Flask, request
from flask_restful import Api, Resource
from mysimbdp_batchingestmanager import BatchManager
import os, time, json

app = Flask(__name__)
api = Api(app)

UPLOAD_DIRECTORY = 'client-staging-input-directory/'
BATCH_MANAGER = BatchManager('customer_information.json')

if not os.path.exists(UPLOAD_DIRECTORY):
    os.makedirs(UPLOAD_DIRECTORY)


@app.route('/ingestion', methods=['POST'])
def ingestion():
    receive_time = str(time.time_ns())

    # check if client ID and file were uploaded
    if 'client_id' not in request.files or 'file' not in request.files:
        return {'message': 'Client ID or file not uploaded'}, 400

    # read client ID and file from request
    client_id = request.files['client_id'].read().decode("utf-8")
    file = request.files['file']
    file_extension = file.filename.split('.')[-1]
    file_size = file.seek(0, os.SEEK_END)               # seek() return the new absolute position

    check_result_message, check_result_status = BATCH_MANAGER.check_client(client_id, file_extension, file_size)
    if check_result_message != 'OK':
        return {'message': check_result_message}, check_result_status

    BATCH_MANAGER.set_receive_time_and_size(client_id, receive_time, file_size)

    # construct file path and save file
    filename = UPLOAD_DIRECTORY + receive_time + '_' + client_id + '.' + file_extension
    file.seek(0, os.SEEK_SET)       # seek back to start position of stream, otherwise save() will write a 0 byte file
    file.save(filename)

    BATCH_MANAGER.update_files_number(-1)
    return {'message': 'Data ingestion successful'}, 201


if __name__ == '__main__':
    app.run(debug=True)