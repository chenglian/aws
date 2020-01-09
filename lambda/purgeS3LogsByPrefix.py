import json
import boto3
import time
import sys

# todays\'s epoch
tday = time.time()
duration = 86400*1 # of days in epoch seconds
#checkpoint for deletion
expire_limit = tday-duration
# initialize s3 client
s3_client = boto3.client('s3')
my_bucket = "logs-208699583703"
my_prefix_key = "s3-lambda-stage"
file_size = [] #just to keep track of the total savings in storage size

def lambda_handler(event, context):

    try:
        s3_file = get_key_info(my_bucket, my_prefix_key)
        for i, fs in enumerate(s3_file["timestamp"]):
            file_expired = check_expiration(fs,expire_limit)
            if file_expired: #if True is recieved
                file_deleted = delete_s3_file(s3_file["key_path"][i])
                if file_deleted: #if file is deleted
                    file_size.append(s3_file["size"][i])

        print(f"Total # of File(s) Deleted: {len(file_size)}")
        print(f"Total File(s) Size Deleted: {sum(file_size)} Bytes")
    except:
        print ("failure:", sys.exc_info()[1])
        print(f"Total # of File(s) Deleted: {len(file_size)}")
        print(f"Total File(s) Size Deleted: {sum(file_size)} Bytes")
        
    return {
        'statusCode': 200,
        'body': json.dumps('OK!')
    }



#Functions
#works to only get us key/file information
def get_key_info(bucket, prefix):

    print(f"Getting S3 Key Name, Size and LastModified from the Bucket: {bucket} with Prefix: {prefix}")

    key_names = []
    file_timestamp = []
    file_size = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        response = s3_client.list_objects_v2(**kwargs)
        for obj in response["Contents"]:
            key_names.append(obj["Key"])
            file_timestamp.append(obj["LastModified"].timestamp())
            file_size.append(obj["Size"])
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break

    key_info = {
        "key_path": key_names,
        "timestamp": file_timestamp,
        "size": file_size
    }
    print(f'{len(key_names)} Keys in {bucket} with {prefix} Prefix found!')

    return key_info


# Check if date passed is older than date limit
def check_expiration(key_date, limit):
    if key_date < limit:
        return True


# connect to s3 and delete the file
def delete_s3_file(file_path, bucket=my_bucket):
    s3_client.delete_object(Bucket=bucket, Key=file_path)
    return True

