import pandas as pd
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import time
import logging
import json
import uuid

# Basic auth header values
ES_HOST = "vpc-staging-discovery-service-1-552nqymgxx4hpi66hbu7u6od6u.ap-south-1.es.amazonaws.com"
ES_USERNAME = "xxxx"
ES_PASSWORD = "xxxx"

MONGO_USERNAME = "xxxx"
MONGO_PASSWORD = "xxxx"
MONGO_HOST = f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@common-mongo.allen-internal-stage.in/?authMechanism=DEFAULT&tls=false"

PAGE_SIZE = 10
DEBUG_MODE = True

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_from_elasticsearch(spark, es_host: str, index: str) -> list:
    logger.info(f"Connecting to Elasticsearch at {es_host} for index {index}")
    es = OpenSearch(
        [es_host],
        http_auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
        scheme="https",
        port=443,
        connection_class=RequestsHttpConnection,
        timeout=60,
    )

    current_time = int(time.time())
    logger.info(f"Current timestamp for query: {current_time}")
    query = {
        "size": PAGE_SIZE,
        "query": {
            "bool": {
                "should": [
                    {"range": {"expiry": {"gt": current_time}}},
                    {"term": {"expiry": 0}}
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [
            {
                "created_at": {
                    "order": "asc",
                    "missing": "_first"
                }
            }
        ]
    }

    logger.info("Executing initial search query")
    es_data = es.search(index=index, body=query, scroll="1m")
    scroll_id = es_data["_scroll_id"]
    records = [hit["_source"] for hit in es_data["hits"]["hits"]]
    logger.info(f"Fetched {len(records)} records in initial query")

    while True:
        logger.info("Fetching next batch of records using scroll API")
        scroll_data = es.scroll(scroll_id=scroll_id, scroll="1m")
        scroll_id = scroll_data["_scroll_id"]
        hits = scroll_data["hits"]["hits"]
        if not hits:
            logger.info("No more records to fetch")
            break
        records.extend([hit["_source"] for hit in hits])
        logger.info(f"Fetched {len(hits)} additional records")
        if DEBUG_MODE:
            logger.debug("Stopping because of debug mode")
            break

    if DEBUG_MODE:
        logger.debug(records)

    logger.info(f"Total records fetched: {len(records)}")
    return records

def transform_data(raw_data: list) -> pd.DataFrame:
    logger.info("Starting data transformation using pandas")
    transformed = []

    for item in raw_data:
        try:
            logger.debug(item)
            user_id = item.get("user_id", "")
            msg_id = item.get("id", "")
            now = int(time.time())
            expire_at = item.get("expiry", now)

            entity_data = item.get("entity_data", {})
            sender_info = entity_data.get("sender_info", {})

            notice_info = {
                "NoticeID": msg_id,
                "Title": entity_data.get("title", ""),
                "Description": entity_data.get("description", ""),
                "Priority": item.get("priority", ""),
                "SenderType": sender_info.get("sender_type", ""),
                "Sender": sender_info.get("sender", ""),
                "Category": item.get("category", ""),
                "Media": entity_data.get("media", []),
                "Expiry": expire_at,
                "Recipient": "RECIPIENT_STUDENT",
                "Status": "NOTICE_STATUS_SEND",
                "CreatedBy": sender_info.get("sender", ""),
                "CreatedAt": item.get("created_at", now),
                "UpdatedBy": "",
                "UpdatedAt": 0,
                "DeletedBy": "",
                "DeletedAt": 0,
                "Schedule": None
            }

            actions = []

            text_content = {
                "Title": "",
                "Body": "",
                "ExpandedBody": "",
                "ImageURL": "",
                "ExpandedImageURL": "",
                "Priority": 0,
                "Actions": actions,
                "NoticeInfo": notice_info
            }

            transformed.append({
                "_id": msg_id,
                "channel_id": f"notification_{user_id}",
                "sender_id": "notification-center-id",
                "seq_id": 69,
                "reply_to_id": 0,
                "type": 0,
                "state": 0,
                "state_code": 0,
                "content": {
                    "text": {
                        "text": json.dumps(text_content)
                    }
                },
                "created_by": "notification-center-id",
                "created_at": item.get("created_at", now),
                "updated_by": "",
                "updated_at": 0,
                "expire_at": expire_at,
                "tags": [9],
                "reference_id": msg_id,
                "read_at": item.get("read_at", 0)
            })

        except Exception as e:
            logger.error(f"Error transforming item: {item}")
            logger.exception(e)

    df = pd.DataFrame(transformed)
    logger.info("Data transformation completed")
    return df

def push_to_mongodb(df: pd.DataFrame, mongo_uri: str, database: str, collection: str):
    logger.info(f"Connecting to MongoDB at {mongo_uri}, database: {database}, collection: {collection}")
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection]
    records = df.to_dict("records")
    try:
        logger.info(f"Inserting {len(records)} records into MongoDB")
        collection.insert_many(records, ordered=False)
        logger.info("Data successfully inserted into MongoDB")
    except BulkWriteError as bwe:
        logger.error("Bulk write error occurred:")
        for error in bwe.details.get('writeErrors', []):
            logger.error(f"Error: {error['errmsg']}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def check_mongodb_connection(mongo_uri: str, database: str, collection: str):
    """
    Connect to MongoDB and log the number of documents in the specified collection.
    """
    logger.info(f"Checking MongoDB connection: {mongo_uri}, DB: {database}, Collection: {collection}")
    try:
        client = MongoClient(mongo_uri)
        db = client[database]
        collection_ref = db[collection]

        doc_count = collection_ref.count_documents({})
        if DEBUG_MODE:
            logger.debug(f"Successfully connected to MongoDB. Document count in collection '{collection}': {doc_count}")
        return doc_count

    except Exception as e:
        logger.error(f"Failed to connect to MongoDB or retrieve document count: {e}")
        return None

def main():
    logger.info("Starting job")

    es_host = ES_HOST
    es_index = "user_communication"
    logger.info(f"Fetching data from Elasticsearch: host={es_host}, index={es_index}")
    raw_data = fetch_from_elasticsearch(None, es_host, es_index)
    logger.debug(raw_data)
    logger.info("Transforming data")
    transformed_data = transform_data(raw_data)

    logger.info("Data transformation completed")

    logger.info("Preparing to push data to MongoDB")
    mongo_uri = MONGO_HOST
    mongo_db = "group_db"
    mongo_collection = "message"
    check_mongodb_connection(mongo_uri, mongo_db, mongo_collection)

    push_to_mongodb(transformed_data, mongo_uri, mongo_db, mongo_collection)

    logger.info("Committing Glue job")

    logger.info("Job completed successfully")

if __name__ == "__main__":
    main()
