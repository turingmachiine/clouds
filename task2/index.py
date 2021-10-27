import os
import io
import boto3
import base64
import requests
import json
import uuid

from PIL import Image


def handler(event, context):
    object_details = event['messages'][0]['details']
    filename = object_details['object_id']
    bucket = object_details['bucket_id']
    if '/faces/' in filename:
            return {
                'statusCode': 200,
                'body': 'Goodbye World!',
            }  
    
    session = boto3.session.Session(
        aws_access_key_id=os.getenv('aws_access_key_id'), 
        aws_secret_access_key=os.getenv('aws_secret_access_key'), 
        region_name='ru-central1'
    )
    
    s3 = session.resource(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

    local_name = '/tmp/' + filename.split('/')[-1]
    print(local_name)
    s3.Bucket(bucket).download_file(filename, local_name)
    
    im = Image.open(local_name)
    faces = json.loads(find_faces(encode_file(open(local_name, 'rb'))))['results'][0]['results'][0]['faceDetection']['faces']
    
    face_filenames = []
    for face in faces:
        cropped = im.crop((
            int(face['boundingBox']['vertices'][0]['x']),
            int(face['boundingBox']['vertices'][0]['y']),
            int(face['boundingBox']['vertices'][2]['x']),
            int(face['boundingBox']['vertices'][2]['y'])
        ))
        cropped.save('/tmp/face.jpg')
        bucket_filename = filename + '/faces/face_' + str(uuid.uuid4()) + '.jpg'
        s3.Bucket(bucket).upload_file('/tmp/face.jpg', bucket_filename)
        face_filenames.append(bucket_filename)
    
    sqs = boto3.resource(
        service_name = 'sqs'
        endpoint_url = 'https://message-queue.api.cloud.yandex.net'
    )
    queue = sqs.Queue(os.getenv('queue_url'))
    queue.send_message(MessageBody=str(face_filenames))

    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }  


def find_faces(image):
    request_body = {
        "analyze_specs": [{
            "content": image.decode("utf-8"), 
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }
    data = json.dumps(request_body)
    response = requests.post('https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze', 
        headers={'Authorization': 'Api-Key ' + os.getenv('api_key')}, data=data)
    return response.text


def encode_file(file):
  file_content = file.read()
  return base64.b64encode(file_content)    