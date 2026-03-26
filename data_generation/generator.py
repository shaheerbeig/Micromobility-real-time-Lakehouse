import json , time , random , uuid
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

KAFKA_BROKER = 'localhost:9092'
TELEMETRY_TOPIC = 'scooter_telemetry'
RIDE_EVENTS_TOPIC = 'ride_events'

producer_config = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


def generate_ride_event():
    return {
        "ride_id": str(uuid.uuid4()),  
        "user_id": fake.random_int(min=1, max=1000),
        "scooter_id": f"S-{fake.random_int(min=100, max=999)}",
        "event_type": random.choice(["start", "end"]), 
        "timestamp": int(time.time() * 1000) 
    }

def generate_telemetry():
    return {
        "scooter_id": f"S-{fake.random_int(min=100, max=999)}",
        "battery_level": random.randint(0, 100),
        "latitude": float(fake.latitude()),   
        "longitude": float(fake.longitude()),
        "timestamp": int(time.time() * 1000)
    }

if __name__ == '__main__':
    print("Starting.")
    
    try:
        while True:
            ride_data = generate_ride_event()
            telemetry_data = generate_telemetry()
                       
            producer.produce(
                topic=RIDE_EVENTS_TOPIC, 
                value=json.dumps(ride_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.produce(
                topic=TELEMETRY_TOPIC, 
                value=json.dumps(telemetry_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0)
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()
        print("Generator fully stopped.")
