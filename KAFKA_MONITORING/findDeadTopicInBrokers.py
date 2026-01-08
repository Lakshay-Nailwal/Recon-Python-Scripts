import sys
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from kafka import KafkaConsumer, TopicPartition  # <-- Make sure TopicPartition is imported

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from csv_utils import append_to_csv

# ==========================
# LOGGING CONFIG
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ==========================
# CONFIG
# ==========================
BROKERS = [
    "localhost:9092",
    # add more brokers here if needed
]
CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()
MAX_WORKERS = 5

# ==========================
# CSV UTILS
# ==========================
def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        logger.info(f"✍️ Writing {len(rows)} row(s) to {filename}")
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

# ==========================
# KAFKA UTILS
# ==========================
def get_all_topics(broker_list):
    """Return all topics from the broker cluster"""
    consumer = KafkaConsumer(bootstrap_servers=broker_list)
    topics = consumer.topics()
    consumer.close()
    logger.info(f"🗂 Found {len(topics)} topics: {topics}")
    return list(topics)

def getMessageCountFast(topic: str, broker_list) -> int:
    """Return total messages in a topic by summing partition offsets"""
    consumer = KafkaConsumer(
        bootstrap_servers=broker_list,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=5000
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        raise Exception(f"No partitions found for topic {topic}")

    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    beginning_offsets = consumer.beginning_offsets(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)

    consumer.close()

    total_messages = sum(end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions)
    logger.info(f"📊 Topic {topic} total messages: {total_messages}")
    return total_messages

# ==========================
# ROW PROCESSOR
# ==========================
def process_topic(topic):
    try:
        logger.info(f"🚀 Processing topic: {topic}")
        message_count = getMessageCountFast(topic, BROKERS)

        if message_count == 0:
            logger.warning(f"❌ Topic {topic} has NO messages")
            safe_append_to_csv("dead_topics.csv", [{"Topic": topic}])
        else:
            logger.info(f"✅ Topic {topic} is ALIVE with {message_count} messages")
            safe_append_to_csv("alive_topics.csv", [{"Topic": topic}])

    except Exception:
        logger.exception(f"🔥 Failed processing topic: {topic}")

# ==========================
# MAIN PROCESSOR
# ==========================
def process_all_topics(broker_list, max_workers):
    topics = get_all_topics(broker_list)
    logger.info(f"🧵 Starting ThreadPoolExecutor with {max_workers} workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_topic, topic) for topic in topics]
        for future in as_completed(futures):
            future.result()

    logger.info("🏁 Topic processing completed")

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    logger.info("🟢 Kafka topic inspection started")
    process_all_topics(BROKERS, max_workers=MAX_WORKERS)
