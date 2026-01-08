import sys
import os
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from kafka import KafkaConsumer, TopicPartition

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
    "kafka01.neo.mercuryonline.co:9092",
    "kafka02.neo.mercuryonline.co:9092",
    "kafka03.neo.mercuryonline.co:9092"
    # Add more brokers here if needed
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
# KAFKA UTILS (FAST)
# ==========================
def getMessageCountFast(topic: str) -> int:
    """Return total messages in a topic by summing partition offsets"""
    logger.info(f"📡 Fetching message count for topic: {topic}")

    consumer = KafkaConsumer(
        bootstrap_servers=BROKERS,
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
def process_row(row):
    topic = row.get("Topic")
    if not topic:
        logger.warning("⚠️ Skipping row without Topic column")
        return

    try:
        logger.info(f"🚀 Processing topic: {topic}")
        message_count = getMessageCountFast(topic)

        if message_count == 0:
            logger.warning(f"❌ Topic {topic} has NO messages")
            safe_append_to_csv("production_dead_topics.csv", [{"Topic": topic}])
        else:
            logger.info(f"✅ Topic {topic} is ALIVE with {message_count} messages")
            safe_append_to_csv("production_alive_topics.csv", [{"Topic": topic}])

    except Exception:
        logger.exception(f"🔥 Failed processing topic: {topic}")

# ==========================
# CSV PARALLEL PROCESSOR
# ==========================
def process_csv_parallel(filename, max_workers):
    logger.info(f"📂 Reading input CSV: {filename}")

    try:
        with open(filename, newline="") as infile:
            rows = list(csv.DictReader(infile))
            logger.info(f"📄 Loaded {len(rows)} rows from CSV")
    except Exception:
        logger.exception("❌ Failed to read CSV")
        return

    logger.info(f"🧵 Starting ThreadPoolExecutor with {max_workers} workers")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        for future in as_completed(futures):
            future.result()

    logger.info("🏁 CSV processing completed")

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    filename = "test.csv"
    full_path = os.path.join(CURRENT_DIRECTORY, filename)

    logger.info("🟢 Kafka topic inspection started")
    process_csv_parallel(full_path, MAX_WORKERS)
