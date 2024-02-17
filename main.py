from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
import functions_framework
import certifi
import boto3
import os

@functions_framework.http
def main(self):
    load_dotenv()

    client = connect_to_mongodb()
    collections = get_collections(client)

    unremovable_data = {
        "entity": [],
        "post": [],
        "user": [],        
        "follow": [],
        "comment": [],
        "comment_reply": [],
        "inactive": [],
    }

    with client.start_session() as session:
        try:
            with session.start_transaction():
                inactive_list = find_data(collections["social"], {"joinProgress": {"$ne": "DONE"}, "createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=1)}})

                if len(inactive_list) > 0: 
                    unremovable_data["inactive"].extend(ObjectId(item["userId"]) for item in inactive_list)
                    delete_old_data(collections["social"], {"joinProgress": {"$ne": "DONE"}, "createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=1)}})
                    delete_old_data(collections["user"], {"_id": {"$in": unremovable_data["inactive"]}})
                    delete_old_data(collections["image"], {"type": "profile", "typeId": {"$in": unremovable_data["inactive"]}})

                delete_old_data(collections["temp_user"], {"createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=1)}})
                temp_user_info = find_data(collections["temp_user"], {})

                for user in temp_user_info:
                    unremovable_data["user"].append(ObjectId(user["userId"]))

                    delete_old_data(collections["diary_entity"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True})
                    delete_old_data(collections["follow"], {"$or": [{"following": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True}, {"follower": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True}]})
                    delete_old_data(collections["share_post"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True})
                    delete_old_data(collections["share_comment"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True})
                    delete_old_data(collections["share_comment_reply"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isDeleted": True})
                    delete_old_data(collections["share_like"], {"userId": user["userId"], "updatedAt": {"$lt": user["createdAt"]}, "isCanceled": True})

                    entity_list = find_data(collections["diary_entity"], {"userId": user["userId"], "isDeleted": True})
                    follow_list = find_data(collections["follow"], { "$or": [{"follower": user["userId"], "isCanceled": True},{"following": user["userId"], "isCanceled": True}]})
                    post_list = find_data(collections["share_post"], {"userId": user["userId"], "isDeleted": True})
                    comment_list = find_data(collections["share_comment"], {"userId": user["userId"], "isDeleted": True})
                    comment_reply_list = find_data(collections["share_comment_reply"], {"userId": user["userId"], "isDeleted": True})

                    unremovable_data["entity"].extend(ObjectId(item['_id']) for item in entity_list)
                    unremovable_data["follow"].extend(ObjectId(item['_id']) for item in follow_list)
                    unremovable_data["post"].extend(ObjectId(item['_id']) for item in post_list)
                    unremovable_data["comment"].extend(ObjectId(item['_id']) for item in comment_list)
                    unremovable_data["comment_reply"].extend(ObjectId(item['_id']) for item in comment_reply_list)

                delete_old_data(collections["notification_log"], {"createdAt": {"$lt": datetime.utcnow() - timedelta(weeks=2)}})
                delete_old_data(collections["diary_entity"], {"_id": {"$nin": unremovable_data["entity"]}, "isDeleted": True})
                delete_old_data(collections["diary_calendar"], {"entityId": {"$nin": unremovable_data["entity"]}, "isDeleted": True})
                delete_old_data(collections["diary_weight"], {"entityId": {"$nin": unremovable_data["entity"]}, "isDeleted": True})
                delete_old_data(collections["follow"], {"_id": {"$nin": unremovable_data["follow"]}, "isCanceled": True})
                delete_old_data(collections["share_post"],{"_id": {"$nin": unremovable_data["post"]}, "isDeleted": True})
                delete_old_data(collections["share_comment"], {"_id": {"$nin": unremovable_data["comment"]} ,"postId": {"$nin": unremovable_data["post"]}, "isDeleted": True})
                delete_old_data(collections["share_comment_reply"], {"_id": {"$nin": unremovable_data["comment_reply"]},"commentId": {"$nin": unremovable_data["comment"]}, "isDeleted": True})
                delete_old_data(collections["share_like"], {"postId": {"$nin": unremovable_data["post"]}, "isCanceled": True})
                
                delete_old_images(collections["image"], {"$and": [{"typeId": {"$nin": unremovable_data["entity"]}, "isDeleted": True}, {"typeId": {"$nin": unremovable_data["post"]}, "isDeleted": True}, {"typeId": {"$nin": unremovable_data["user"]}, "isDeleted": True}]})

            session.commit_transaction()
            print('Success')
            return "Success"
        except Exception as e:
            session.abort_transaction()
            print('Fail')
            return "Fail"

# DB 데이터 검색
def find_data(collection, filter_query):
    return list(collection.find(filter_query))

# DB 데이터 삭제
def delete_old_data(collection, filter_query):
    collection.delete_many(filter_query)

# 테이블 이미지 삭제 및 S3 이미지 삭제처리
def delete_old_images(collection, filter_query):
    remove_image_key = find_data(collection, filter_query)
    delete_old_data(collection, filter_query)
    basic_image_keys = env_vars("USER_BASE_IMAGE")

    object_keys = [item["imageKey"] for item in remove_image_key if item["imageKey"] != basic_image_keys ]

    if len(object_keys) > 0:
        aws_access_key_id = env_vars("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = env_vars("AWS_SECRET_ACCESS_KEY")
        aws_bucket_name = env_vars("AWS_PROD_BUCKET")

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        return s3.delete_objects(
            Bucket=aws_bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in object_keys],
                'Quiet': False  
            }
        )

# mongoDB 연결
def connect_to_mongodb():
    mongodb_uri = env_vars("MONGODB_PROD_URI")
    client = MongoClient(mongodb_uri, tls=True, tlsCAFile=certifi.where())  
    return client

# mongoDB 테이블 가져오기
def get_collections(client):
    db = client["crawl"] 
    temp_user = db["tempusers"]
    user = db["users"]
    social = db["socials"]
    notification_log = db["notificationlogs"]
    follow = db["follows"]
    diary_entity = db["diaryentities"]
    diary_calendar = db["diarycalendars"]
    diary_weight = db["diaryweights"]
    share_comment_reply = db["sharecommentreplies"]
    share_comment = db["sharecomments"]
    share_like = db["sharelikes"]
    share_post = db["shareposts"]
    image = db["images"]
    
    return {
        "temp_user": temp_user,
        "user": user,
        "social": social,
        "notification_log": notification_log,
        "follow": follow,
        "diary_entity": diary_entity,
        "diary_calendar": diary_calendar,
        "diary_weight": diary_weight,
        "share_comment_reply": share_comment_reply,
        "share_comment": share_comment,
        "share_like": share_like,
        "share_post": share_post,
        "image": image
    }

# 환경변수 가져오기
def env_vars(Key):
    return os.environ.get(Key, f"Specified environment variable is not set. key : {Key}")

main("")