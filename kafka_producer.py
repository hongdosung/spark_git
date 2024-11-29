from confluent_kafka import Producer
import socket
conf = {"bootstrap.servers": "kafka-service:9092", "client.id": socket.gethostname()}
producer = Producer(conf)
producer.produce("<주제 이름>", key="<키>", value="<값>")