--1

import boto3
import json
import time
import random

# Initialize the Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')  # Use your AWS region

# Stream name
stream_name = 'ClickstreamData'  # Replace with your actual Kinesis stream name

# Sample products for the clickstream data
products = [
    {"item_id": "MOB001", "item_name": "Mobile Phone"},
    {"item_id": "LAP002", "item_name": "Laptop"},
    {"item_id": "CAM003", "item_name": "Camera"}
]

# Function to generate random clickstream data
def generate_clickstream_data():
    product = random.choice(products)
    click_data = {
        "item_id": product["item_id"],
        "item_name": product["item_name"],
        "click_count": random.randint(1, 100)  # Random click count between 1 and 100
    }
    return click_data

# Function to continuously send clickstream data to Kinesis
def send_data_to_kinesis():
    while True:
        # Generate random clickstream data
        click_data = generate_clickstream_data()

        # Send the data to Kinesis
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(click_data),
            PartitionKey=click_data['item_id']  # Partition by item_id
        )
        
        # Print the response
        print(f"Data sent to Kinesis: {click_data}")
        print(f"Kinesis response: {response}")

        # Wait before sending the next data
        time.sleep(5)  # Adjust sleep interval as needed

# Call the function to start streaming data
send_data_to_kinesis()


--2
import boto3
import json
import time

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')  # Use your region

# Stream Name
stream_name = 'TruckDataStream'  # or your stream name

# Sample truck data
truck_data = {
    "truck_id": "TRK001",
    "gps_location": {
        "latitude": 34.052235,
        "longitude": -118.243683,
        "altitude": 89.0,
        "speed": 65.0
    },
    "vehicle_speed": 65.0,
    "engine_diagnostics": {
        "engine_rpm": 2500,
        "fuel_level": 75.0,
        "temperature": 90.0,
        "oil_pressure": 40.0,
        "battery_voltage": 13.8
    },
    "odometer_reading": 102345.6,
    "fuel_consumption": 15.5
}

# Continuously send data to Kinesis stream
while True:
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(truck_data),
        PartitionKey="partitionKey1"
    )
    print(f"Data sent to Kinesis: {response}")
    time.sleep(5)  # Send data every 5 seconds (adjust as needed)


--3

import json
import boto3
from datetime import datetime
import base64
from decimal import Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TruckTelemetryTable')

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        truck_data = json.loads(payload, parse_float=Decimal)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Ensure the correct key names are used
        item = {
    'truck_id': truck_data.get('truck_id', 'default_id'),  # Use 'truck_id'
    'timestamp': timestamp,
    'location': truck_data.get('location', 'unknown_location'),
    'truck_name': truck_data.get('truck_name', 'unknown_name'),
    'temperature': Decimal(str(truck_data.get('temperature', 0))),  # If your payload includes temperature
    'speed': Decimal(str(truck_data.get('speed', 0)))  # If your payload includes speed
        }

        try:
            table.put_item(Item=item)
        except Exception as e:
            print(f"Error putting item: {e}")
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Truck event processed successfully!')
    }

--4

from datetime import datetime
import boto3
import base64
import json
from decimal import Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ClickstreamDataTable')

def lambda_handler(event, context):
    for record in event['Records']:
        # Decode the base64-encoded Kinesis data
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        
        # Convert the payload into JSON
        clickstream_data = json.loads(payload, parse_float=Decimal)
        
        # Add the current timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Construct the item to be inserted into DynamoDB
        item = {
            'item_id': clickstream_data.get('item_id'),
            'item_name': clickstream_data.get('item_name'),
            'click_count': clickstream_data.get('click_count'),
            'timestamp': timestamp  # Add the timestamp field
        }

        # Insert the item into DynamoDB
        try:
            table.put_item(Item=item)
        except Exception as e:
            print(f"Error putting item: {e}")
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Clickstream event processed successfully!')
    }

