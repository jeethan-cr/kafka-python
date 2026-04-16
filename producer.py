from kafka import KafkaProducer
import json
import time

producer= KafkaProducer(bootstrap_servers='localhost:9093',
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                        acks='all')

topic="kafka-python"
print("Producer is ready to send messages...")
for i in range(10):
    data={"id": i, "message": f"Hello Kafka {i}"}

    producer.send(topic, value=data)
    print(f"Sent: {data}")
    time.sleep(1)

producer.flush()
producer.close()
print("Producer has finished sending messages.")