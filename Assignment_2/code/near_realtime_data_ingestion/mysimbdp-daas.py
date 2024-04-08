from mysimbdp_streamingestmanager import StreamManager
from confluent_kafka import Producer
from flask import Flask, request
from flask_restful import Api
import json
import time

app = Flask(__name__)
api = Api(app)

# Kafka bootstrap server address
KAFKA_BOOTSTRAP_SERVER = "localhost:9093"

STREAM_MANAGER = StreamManager('mysimbdp_stream_ingest_monitor.json')


# Delivery callback function
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


@app.route('/stream_ingestion', methods=['POST'])
def stream_ingestion():
    receive_time = time.time()

    # check if client ID and data were uploaded
    if 'client_id' not in request.files or 'data' not in request.files:
        return {'message': 'Client ID or data not uploaded'}, 400

    # read client ID and data from request
    client_id = request.files['client_id'].read().decode("utf-8")
    json_data = json.load(request.files['data'])
    f_size = len(json_data)

    check_result_message, check_result_status = STREAM_MANAGER.check_client(client_id, f_size)
    if check_result_message != 'OK':
        return {'message': check_result_message}, check_result_status

    STREAM_MANAGER.add_time(client_id, "receive_time", receive_time)

    # Kafka producer instance
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER})

    # Send the data to Kafka
    kafka_producer.produce(client_id, json.dumps(json_data).encode('utf-8'), callback=delivery_report)
    kafka_producer.flush()

    return {'message': 'Data ingestion successful'}, 201


if __name__ == '__main__':
    app.run(debug=True)