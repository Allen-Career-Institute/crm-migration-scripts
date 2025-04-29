import pandas as pd
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import time
import logging
import json
import hashlib
from bson import ObjectId

# Basic auth header values
ES_HOST = "vpc-staging-discovery-service-1-552nqymgxx4hpi66hbu7u6od6u.ap-south-1.es.amazonaws.com"
ES_USERNAME = "dev_user"
ES_PASSWORD = "W!zW+IcQF[h%XwD5"

MONGO_USERNAME = "xxxx"
MONGO_PASSWORD = "xxxx"
MONGO_HOST = f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@common-mongo.allen-internal-stage.in/?authMechanism=DEFAULT&tls=false"

PAGE_SIZE = 1000
DEBUG_MODE = False
es_index = "user_communication"
mongo_db = "group_db"
mongo_collection = "message"

# global variables
scroll_id = None
es = None
client = None

# Configure logging based on DEBUG_MODE
if DEBUG_MODE:
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)


def fetch_from_elasticsearch(es_host: str, index: str, limit: int = 0) -> list:
    logger.info(f"Connecting to Elasticsearch at {es_host} for index {index}")
    global es, scroll_id
    logger.info(f"scroll id: {scroll_id}")
    if es is None:
        es = OpenSearch(
            [es_host],
            http_auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            scheme="https",
            port=443,
            connection_class=RequestsHttpConnection,
            timeout=60,
        )

    records = []
    if scroll_id is None:
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

        logger.info("Executing search query")
        es_data = es.search(index=index, body=query, scroll="5m")
        scroll_id = es_data["_scroll_id"]
        records = [hit["_source"] for hit in es_data["hits"]["hits"]]
        logger.info(f"Fetched {len(records)} records in initial query")
    else:
        logger.info("Using existing scroll ID to fetch more records")

    while True:
        logger.debug("Fetching next batch of records using scroll API")
        scroll_data = es.scroll(scroll_id=scroll_id, scroll="5m")
        scroll_id = scroll_data["_scroll_id"]
        hits = scroll_data["hits"]["hits"]
        if not hits:
            logger.info("No more records to fetch")
            break
        records.extend([hit["_source"] for hit in hits])
        logger.debug(f"Fetched {len(hits)} additional records")
        if DEBUG_MODE:
            logger.debug("Stopping because of debug mode")
            break
        if limit != 0 and len(records) >= limit:
            logger.info(f"Limit, {limit} reached, stopping fetch")
            break

    logger.info(f"Total records fetched: {len(records)}")
    return records


def transform_data(raw_data: list) -> pd.DataFrame:
    logger.info("Starting data transformation using pandas")
    transformed = []
    now = int(time.time())

    for item in raw_data:
        try:
            logger.debug(item)
            user_id = item.get("user_id", "")
            msg_id = item.get("entity_id", "")

            expiry_val = item.get("expiry")
            expire_at = expiry_val if expiry_val else now + 365 * 24 * 60 * 60

            entity_data = item.get("entity_data", {})
            sender_info = entity_data.get("sender_info", {})

            sender_type = 1 if sender_info.get("sender_type") == "NOTICE_SENDER_TEACHER" else 0
            
            notice_info = {
                "notice_id": msg_id,
                "title": entity_data.get("title", ""),
                "description": entity_data.get("description", ""),
                "priority": item.get("priority", ""),
                "sender_info": {
                    "sender_type": sender_type,
                    "sender": sender_info.get("sender", ""),
                },
                "category": item.get("category", ""),
                "media": entity_data.get("media", []),
                "expiry": expire_at,
                "recipient": 1,
                "status": 2,
                "created_by": sender_info.get("sender", ""),
                "created_at": item.get("created_at", now),
            }

            text_content = {
                "Title": "",
                "Body": "",
                "ExpandedBody": "",
                "ImageURL": "",
                "ExpandedImageURL": "",
                "Priority": 0,
                "Actions": [],
                "NoticeInfo": notice_info
            }

            logger.debug(f"Transformed item: {text_content}")

            transformed.append({
                "_id": generate_id(user_id, msg_id),
                "channel_id": f"notification_{user_id}",
                "sender_id": "notification-center-id",
                "seq_id": 0,
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
    logger.info("Data transformation completed for %d records", len(df))
    return df


def push_to_mongodb(df: pd.DataFrame, mongo_uri: str, database: str, collection: str):
    logger.info(f"Connecting to MongoDB at {mongo_uri}, database: {database}, collection: {collection}")
    global client
    if client is None:
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
    logger.info(f"Checking MongoDB connection: {mongo_uri}, DB: {database}, Collection: {collection}")
    global client
    try:
        if client is None:
            client = MongoClient(mongo_uri)
        db = client[database]
        collection_ref = db[collection]

        doc_count = collection_ref.count_documents({})
        logger.debug(f"Successfully connected to MongoDB. Document count in collection '{collection}': {doc_count}")
        return doc_count

    except Exception as e:
        logger.error(f"Failed to connect to MongoDB or retrieve document count: {e}")
        return None


def generate_id(user_id, msg_id):
    input_str = f"{user_id}_{msg_id}"
    hash_obj = hashlib.sha256(input_str.encode())
    hash_bytes = hash_obj.digest()[:12]
    hex_str = hash_bytes.hex()
    obj = ObjectId(hex_str)
    return obj

def migrate_data_in_chunks(CHUNK_SIZE=100000):
    i = 0
    while True:
        try:
            logger.info(f"Migration, iteration {i}")
            raw_data = fetch_from_elasticsearch(ES_HOST, es_index, CHUNK_SIZE)
            if not raw_data:
                logger.info("No more data to migrate")
                break
            transformed_data = transform_data(raw_data)

            if DEBUG_MODE:
                check_mongodb_connection(MONGO_HOST, mongo_db, mongo_collection)
            else:
                push_to_mongodb(transformed_data, MONGO_HOST, mongo_db, mongo_collection)

            i += 1
            if DEBUG_MODE:
                logger.info("Debug mode is on, stopping after one iteration")
                break
        except Exception as e:
            logger.error(f"Error during iteration {i}: {e}")

def main():
    logger.info("Starting job")

    migrate_data_in_chunks(2000)

    logger.info("Job completed")


if __name__ == "__main__":
    main()
