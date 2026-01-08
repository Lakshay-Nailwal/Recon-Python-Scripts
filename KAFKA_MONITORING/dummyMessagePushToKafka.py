from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

producer.send("test-topic", value=b"msg-p0", partition=0)
producer.send("test-topic", value=b"msg-p1", partition=1)
producer.send("test-topic", value=b"msg-p2", partition=2)

producer.flush()
producer.close()
