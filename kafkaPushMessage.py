from confluent_kafka import Producer
import json
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = "staging_candidate_bin_update"
BOOTSTRAP_SERVERS = "kafka.onprem.staging.gorio.in:9094"


def create_producer(bootstrap_servers):
    return Producer({"bootstrap.servers": bootstrap_servers})

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def send_to_kafka(producer, topic, payload):
    try:
        payload_str = json.dumps(payload)
        producer.produce(
            topic=topic,
            value=payload_str,
            callback=delivery_report
        )
    except Exception as e:
        logging.exception("Failed to send message to Kafka")

if __name__ == "__main__":
    producer = create_producer(BOOTSTRAP_SERVERS)
    
    try:
        payload = {"key": "th214_backup_bins_test.csv", "tenant": "th214"}
        logging.info(f"Sending payload: {payload} to topic {KAFKA_TOPIC}")
        send_to_kafka(producer, KAFKA_TOPIC, payload)
                    
    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Flushing messages...")
    except Exception as ex:
        logging.exception("Unexpected error occurred during execution.")
    finally:
        producer.flush()
        logging.info("All Kafka messages flushed and program completed.")
