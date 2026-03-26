import json
import random
import time
import uuid
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
    event_type = random.choice(["start", "end"])
    duration_mins = random.randint(2, 45)
    distance_km = round(random.uniform(0.4, 12.0), 2)
    fare_amount = round(2.5 + (distance_km * 0.75), 2)

    return {
        "ride_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1, max=1000),
        "scooter_id": f"S-{fake.random_int(min=100, max=999)}",
        "event_type": event_type,
        "timestamp": int(time.time() * 1000),
        "duration_mins": duration_mins,
        "distance_km": distance_km,
        "fare_amount": fare_amount,
        "city": "new_york",
        "payment_method": random.choice(["card", "wallet"]),
        "ride_status": "in_progress" if event_type == "start" else "completed",
    }


def generate_telemetry():
    return {
        "scooter_id": f"S-{fake.random_int(min=100, max=999)}",
        "battery_level": random.randint(0, 100),
        "latitude": float(fake.latitude()),
        "longitude": float(fake.longitude()),
        "timestamp": int(time.time() * 1000),
        "speed_kmh": round(random.uniform(0, 35), 2),
        "odometer_km": round(random.uniform(50, 5000), 2),
        "temperature_c": round(random.uniform(10, 38), 1),
        "signal_strength": random.randint(1, 5),
        "is_locked": random.choice([True, False]),
        "scooter_status": random.choice(["available", "in_use", "charging", "maintenance"]),
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
            print("Ride data generated",ride_data)
            print("Telemetry data generated",telemetry_data)

    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()
        print("Generator fully stopped.")
