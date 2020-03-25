#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
import sys
import os
import requests
import ibm_boto3
from ibm_botocore.client import Config

def main(dict):
    
    # File download
    URL = dict['URL']
    try:
        filename = os.path.basename(URL)
        print('File name : ' + filename)
        with requests.get(URL, stream=True) as r:
            r.raise_for_status()
            print('Content-Type : ' + r.headers['Content-Type'])
            with open('/tmp/' + filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=512 * 1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()
        print('Download file completed.')
    except Exception as excpt:
        print('Download file failed, Error: {}'.format(str(excpt)))

    # Set up IBM Cloud Object Storage
    COS_ENDPOINT = dict['COS_ENDPOINT']
    COS_API_KEY_ID = dict['COS_API_KEY_ID']
    COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
    COS_RESOURCE_CRN = dict['COS_RESOURCE_CRN']
    COS_BUCKET_LOCATION = dict['COS_BUCKET_LOCATION']

    cos = ibm_boto3.client("s3",
        ibm_api_key_id=COS_API_KEY_ID,
        ibm_service_instance_id=COS_RESOURCE_CRN,
        ibm_auth_endpoint=COS_AUTH_ENDPOINT,
        config=Config(signature_version="oauth"),
        endpoint_url=COS_ENDPOINT
    )
    
    # Configure transfer manager
    part_size = 1024 * 1024 * 20
    file_threshold = 1024 * 1024 * 20

    transfer_config = ibm_boto3.s3.transfer.TransferConfig(
        multipart_threshold=file_threshold,
        multipart_chunksize=part_size
    )

    # Create transfer manager
    transfer_mgr = ibm_boto3.s3.transfer.TransferManager(cos, config=transfer_config)
    file_path = '/tmp/' + filename
    bucket_name = COS_BUCKET_LOCATION
    item_name = filename
    status = False

    # File upload
    try:
        future = transfer_mgr.upload(file_path, bucket_name, item_name)
        future.result()
        print('Upload file to COS completed.')
        status = True
    except Exception as excpt:
        print('Upload file to COS failed, Error: {}'.format(str(excpt)))
    return { 'message': status }
