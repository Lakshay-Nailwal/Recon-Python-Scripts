from confluent_kafka import Producer
import json
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = "staging_create_stocktransfer_gatepass"
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
        producer.produce(topic, value=payload_str, callback=delivery_report)
    except Exception as e:
        logging.exception("Failed to send message to Kafka")

if __name__ == "__main__":
    producer = create_producer(BOOTSTRAP_SERVERS)
    
    try:
        payload = {
  "user": "fb4d5351-4216-4986-bc91-70b9c958b105",
  "tenant": "ar124",
  "stockTransfer": {
    "id": 10666,
    "destinationId": None,
    "destinationName": None,
    "destinationType": None,
    "type": None,
    "invoiceNo": "A0124-25A0000033",
    "status": None,
    "interstate": True,
    "createdBy": None,
    "updatedBy": None,
    "createdOn": None,
    "updatedOn": None,
    "items": [],
    "scannedItems": [],
    "pickedItems": [],
    "trayId": None,
    "pickerTaskId": None,
    "fromType": None,
    "fromName": None,
    "fromAddress": None,
    "fromCity": None,
    "fromGst": None,
    "fromDl1": None,
    "fromDl2": None,
    "fromPincode": None,
    "fromWhId": None,
    "fromState": None,
    "toType": None,
    "toName": None,
    "toAddress": None,
    "toCity": None,
    "toGst": None,
    "toDl1": None,
    "toDl2": None,
    "toState": None,
    "toPincode": None,
    "toWhId": None,
    "message": "",
    "referenceId": None,
    "referenceType": None,
    "transfer": None,
    "boxes": [],
    "totalAmount": None,
    "creationMode": None,
    "company": "",
    "hardEinvoicingEnabled": None,
    "eInvoiceQr": "",
    "eInvoiceIrn": "",
    "sourcePartnerDetailId": None,
    "fssaiLicense": None,
    "deliveryMode": None,
    "ewayBillNeeded": None,
    "einvoiceNeeded": None,
    "invoiceDate": None,
    "dispatchDate": None,
    "isM2m": False,
    "purchaseReturnVendorId": None,
    "sourceInvoiceNo": None,
    "hasPrInvoice": None,
    "einvoiceIrn": "",
    "einvoiceQr": ""
  },
  "app": "arsenal",
  "returnSt": False,
  "stType": "NORMAL",
  "strIssueIds": [],
  "strHasPrInvoice": None
}
        logging.info(f"Sending payload: {payload}")
        send_to_kafka(producer, KAFKA_TOPIC, payload)
                    
    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Flushing messages...")
    except Exception as ex:
        logging.exception("Unexpected error occurred during execution.")
    finally:
        producer.flush()
        logging.info("All Kafka messages flushed and program completed.")