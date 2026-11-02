import json, random, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092", "client.id": "uber-producer"})
TOPIC = "uber_rides"
CUSTOMERS = ["cust_101", "cust_102", "cust_103", "cust_104", "cust_105"]
DRIVERS = ["driver_501", "driver_502", "driver_503", "driver_504"]
LOCATIONS = ["Indiranagar", "Koramangala", "Whitefield", "HSR Layout", "Electronic City", "Jayanagar"]
VEHICLES = ["Sedan", "SUV", "Bike"]

def delivery_report(error, message):
    if error:
        print(f"DELIVERY FAILED: {error}")
    else:
        print(f"Delivered ride={message.key().decode()} partition={message.partition()} offset={message.offset()}")

def generate_ride():
    ride_id = f"ride_{uuid.uuid4().hex[:12]}"
    customer_id, driver_id = random.choice(CUSTOMERS), random.choice(DRIVERS)
    pickup = random.choice(LOCATIONS)
    dropoff = random.choice([x for x in LOCATIONS if x != pickup])
    vehicle = random.choice(VEHICLES)
    distance = round(random.uniform(2, 30), 2)
    surge = round(random.uniform(1.0, 2.0), 2)
    fare = round(distance * random.uniform(12, 20) * surge, 2)

    for event_type in ["RIDE_REQUESTED", "DRIVER_ACCEPTED", "RIDE_STARTED", "RIDE_COMPLETED"]:
        event = {
            "event_id": str(uuid.uuid4()), "ride_id": ride_id,
            "event_type": event_type,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "customer_id": customer_id, "driver_id": driver_id,
            "pickup_location": pickup, "drop_location": dropoff,
            "vehicle_type": vehicle, "distance_km": distance,
            "fare_amount": fare, "surge_multiplier": surge,
            "status": event_type.replace("RIDE_", "")
        }
        producer.produce(TOPIC, key=ride_id, value=json.dumps(event), callback=delivery_report)
        producer.poll(0)
        print(f"Published {event_type}: {ride_id}")
        time.sleep(0.25)

if __name__ == "__main__":
    try:
        for _ in range(20):
            generate_ride()
            time.sleep(0.5)
        producer.flush()
        print("Finished publishing 80 events.")
    except KeyboardInterrupt:
        print("Producer interrupted.")
    finally:
        producer.flush()
