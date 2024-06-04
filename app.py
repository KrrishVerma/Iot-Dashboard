from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import threading
import time
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import json
import logging

app = Flask(__name__)
socketio = SocketIO(app)

# Set up logging
logging.basicConfig(filename='sensor_script.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# AWS IoT setup
aws_iot_endpoint = "ak1zwrjylni7d-ats.iot.us-east-1.amazonaws.com"
mqtt_port = 8883
mqtt_topic = "sensor/data"

# AWS IoT client setup
client = boto3.client('iot-data', region_name='us-east-1')

# Store historical data
historical_data = []

# Load historical data from DynamoDB
def load_historical_data():
    global historical_data
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Sensor')
    response = table.scan()
    historical_data = response['Items']
    logger.info("Historical data loaded from DynamoDB")

load_historical_data()

# MQTT message callback
def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode('utf-8'))
    logger.info(f"Received message from AWS IoT: {payload}")

    # Store the latest data in the historical data list
    historical_data.append(payload)

    # Emit data to WebSocket clients
    socketio.emit('update', payload)

# Connect to AWS IoT
def connect_aws_iot():
    client.on_connect = lambda client, userdata, flags, rc: logger.info(f"Connected to AWS IoT with result code {rc}")
    client.on_disconnect = lambda client, userdata, rc: logger.info(f"Disconnected from AWS IoT with result code {rc}")
    client.on_message = on_message

    try:
        client.tls_set(ca_certs="root-CA.crt", certfile="certificate.pem.crt", keyfile="private.pem.key")
        client.connect(aws_iot_endpoint, mqtt_port, 60)
        client.subscribe(mqtt_topic)
        client.loop_start()
    except Exception as e:
        logger.error(f"AWS IoT connection error: {e}")

connect_aws_iot()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/latest_data')
def latest_data_endpoint():
    return jsonify(historical_data[-1] if historical_data else {})

@app.route('/historical_data')
def historical_data_endpoint():
    return jsonify(historical_data)

def start_flask():
    socketio.run(app, host='0.0.0.0', port=5000)

# Start the Flask server in a separate thread
flask_thread = threading.Thread(target=start_flask)
flask_thread.start()
