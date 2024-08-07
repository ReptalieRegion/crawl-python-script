from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
import functions_framework
import certifi
import boto3
import os

@functions_framework.http
def main(request):
    load_dotenv()

    client = connect_to_mongodb()
    collections = get_collections(client)

    unremovable_data = {
        "entity": set(),
        "post": set(),
        "user": set(),        
        "follow": set(),
        "comment": set(),
        "comment_reply": set(),
        "inactive": set(),
    }

    with client.start_session() as session:
        try:
            with session.start_transaction():
                process_inactive_users(collections, unremovable_data)
                process_temp_users(collections, unremovable_data)
                delete_old_records(collections, unremovable_data)
                delete_old_images(collections["image"], unremovable_data)
                
            session.commit_transaction()
            print('Success')
            return "Success"
        except Exception as e:
            session.abort_transaction()
            print('Fail')
            return "Fail"

# 비활성 사용자 데이터를 처리합니다.
def process_inactive_users(collections, unremovable_data):
    filter_query = {"joinProgress": {"$ne": "DONE"}, "createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=1)}}
    inactive_list = find_and_delete_data(collections["social"], filter_query)

    if inactive_list:
        unremovable_data["inactive"].update(ObjectId(item["userId"]) for item in inactive_list)
        delete_old_data(collections["user"], {"_id": {"$in": list(unremovable_data["inactive"])}})
        delete_old_data(collections["image"], {"type": "profile", "typeId": {"$in": list(unremovable_data["inactive"])}})

# 임시 사용자 데이터를 처리합니다.
def process_temp_users(collections, unremovable_data):
    delete_old_data(collections["temp_user"], {"createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=1)}})
    temp_user_info = find_data(collections["temp_user"], {})

    for user in temp_user_info:
        user_id = ObjectId(user["userId"])
        unremovable_data["user"].add(user_id)

        filter_queries = [
            (collections["diary_entity"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True}),
            (collections["follow"], {"$or": [{"following": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True}, {"follower": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True}]}),
            (collections["share_post"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True}),
            (collections["share_comment"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True}),
            (collections["share_comment_reply"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True}),
            (collections["share_like"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True}),
        ]

        for collection, query in filter_queries:
            delete_old_data(collection, query)

        collect_unremovable_data(collections, user_id, unremovable_data)

# 삭제되지 않는 데이터를 수집합니다.
def collect_unremovable_data(collections, user_id, unremovable_data):
    queries = [
        (collections["diary_entity"], "entity", {"userId": user_id, "isDeleted": True}),
        (collections["follow"], "follow", {"$or": [{"follower": user_id, "isCanceled": True}, {"following": user_id, "isCanceled": True}]}),
        (collections["share_post"], "post", {"userId": user_id, "isDeleted": True}),
        (collections["share_comment"], "comment", {"userId": user_id, "isDeleted": True}),
        (collections["share_comment_reply"], "comment_reply", {"userId": user_id, "isDeleted": True}),
    ]

    for collection, key, query in queries:
        items = find_data(collection, query)
        unremovable_data[key].update(ObjectId(item['_id']) for item in items)

# 오래된 레코드를 삭제합니다.
def delete_old_records(collections, unremovable_data):
    delete_old_data(collections["notification_log"], {"createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=2)}})
    delete_old_data(collections["diary_entity"], {"_id": {"$nin": list(unremovable_data["entity"])}, "isDeleted": True})
    delete_old_data(collections["diary_calendar"], {"entityId": {"$nin": list(unremovable_data["entity"])}, "isDeleted": True})
    delete_old_data(collections["diary_weight"], {"entityId": {"$nin": list(unremovable_data["entity"])}, "isDeleted": True})
    delete_old_data(collections["follow"], {"_id": {"$nin": list(unremovable_data["follow"])}, "isCanceled": True})
    delete_old_data(collections["share_post"], {"_id": {"$nin": list(unremovable_data["post"])}, "isDeleted": True})
    delete_old_data(collections["share_comment"], {"_id": {"$nin": list(unremovable_data["comment"])}, "postId": {"$nin": list(unremovable_data["post"])}, "isDeleted": True})
    delete_old_data(collections["share_comment_reply"], {"_id": {"$nin": list(unremovable_data["comment_reply"])}, "commentId": {"$nin": list(unremovable_data["comment"])}, "isDeleted": True})
    delete_old_data(collections["share_like"], {"postId": {"$nin": list(unremovable_data["post"])}, "isCanceled": True})

# 데이터를 검색하고 삭제합니다.
def find_and_delete_data(collection, filter_query):
    data = find_data(collection, filter_query)
    delete_old_data(collection, filter_query)
    return data

# 데이터베이스에서 데이터를 검색합니다.
def find_data(collection, filter_query):
    return list(collection.find(filter_query))

# 데이터베이스에서 데이터를 삭제합니다.
def delete_old_data(collection, filter_query):
    collection.delete_many(filter_query)

# 오래된 이미지를 삭제합니다.
def delete_old_images(collection, unremovable_data):
    filter_query = {
        "$and": [
            {"typeId": {"$nin": list(unremovable_data["entity"])}},
            {"typeId": {"$nin": list(unremovable_data["post"])}},
            {"typeId": {"$nin": list(unremovable_data["user"])}},
            {"isDeleted": True}
        ]
    }
    remove_image_key = find_and_delete_data(collection, filter_query)
    basic_image_keys = env_vars("USER_BASE_IMAGE")

    object_keys = [item["imageKey"] for item in remove_image_key if item["imageKey"] != basic_image_keys]

    if object_keys:
        aws_access_key_id = env_vars("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = env_vars("AWS_SECRET_ACCESS_KEY")
        aws_bucket_name = env_vars("AWS_PROD_BUCKET")

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        s3.delete_objects(
            Bucket=aws_bucket_name,
            Delete={'Objects': [{'Key': key} for key in object_keys], 'Quiet': False}
        )

# MongoDB에 연결합니다.
def connect_to_mongodb():
    mongodb_uri = env_vars("MONGODB_PROD_URI")
    return MongoClient(mongodb_uri, tls=True, tlsCAFile=certifi.where())

# MongoDB 컬렉션을 가져옵니다.
def get_collections(client):
    db = client["crawl"]
    return {name: db[name] for name in ["temp_user", "user", "social", "notification_log", "follow", "diary_entity", "diary_calendar", "diary_weight", "share_comment_reply", "share_comment", "share_like", "share_post", "image"]}

# 환경 변수를 가져옵니다.
def env_vars(key):
    return os.environ.get(key, f"Specified environment variable is not set: {key}")

main("")
