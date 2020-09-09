import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from zipfile import ZipFile 
import zipfile
from s3_md5_compare import md5_compare


def data_to_s3(frmt):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    source_dataset_url = 'https://projects.fivethirtyeight.com/data-webpage-data/datasets/trump-approval-ratings'
    try:
        response = urlopen(source_dataset_url + frmt)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    else:
        data_set_name = 'AVtest'
        #data_set_name = os.environ['DATA_SET_NAME']
        filename = data_set_name + frmt
        file_location = '/tmp/' + filename

        with open(file_location, 'wb') as f:
            f.write(response.read())
            f.close()
        
        #unzips the zipped folder
        listOfFiles = []
        fh = open(file_location, 'rb')
        zipExtract = zipfile.ZipFile(fh)
        for name in zipExtract.namelist():
            #instead of unzipping in directory, the unzipped folder is sent to the /tmp/ folder
            listOfFiles = zipExtract.namelist()
            outpath = "/tmp/"
            zipExtract.extract(name, outpath)
        fh.close()
        
        #listOfFiles[0] is the name of the subfolder that holds all the files
        folderDir = '/tmp/' + listOfFiles[0]

        for filename in os.listdir('/tmp'):
            print('name: ' + filename)
    
        # variables/resources used to upload to s3
        s3_bucket = os.environ['S3_BUCKET']
        new_s3_key = data_set_name + '/dataset/'
        s3 = boto3.client('s3')

        s3_uploads = []
        asset_list = []

        for r, d, f in os.walk(folderDir):
            for filename in f:
                
                
                #obj_name = os.path.join(r, filename).split('/', 3).pop().replace(' ', '_').lower()
                file_location = os.path.join(r, filename)
                obj_name = os.path.join(r, filename)
                new_s3_key = data_set_name + '/dataset' + obj_name
                print('s3 key: ' + new_s3_key)
                print('file loc: ' + file_location) 

                has_changes = md5_compare(s3, s3_bucket, new_s3_key, file_location)
                if has_changes:
                    s3.upload_file(file_location, s3_bucket, new_s3_key)
                    print('Uploaded: ' + filename)
                else:
                    print('No changes in: ' + filename)

                asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
                s3_uploads.append({'has_changes': has_changes, 'asset_source': asset_source})

        count_updated_data = sum(upload['has_changes'] == True for upload in s3_uploads)
        if count_updated_data > 0:
            asset_list = list(map(lambda upload: upload['asset_source'], s3_uploads))
            if len(asset_list) == 0:
                raise Exception('Something went wrong when uploading files to s3')
        # asset_list is returned to be used in lamdba_handler function
        # if it is empty, lambda_handler will not republish
        return asset_list
        

def source_dataset():

    # list of enpoints to be used to access data included with product
    data_endpoints = [
        '.zip'
    ]

    # multithreading speed up accessing data, making lambda run quicker
    with (Pool(1)) as p:
        asset_list = p.map(data_to_s3, data_endpoints)

    # asset_list is returned to be used in lamdba_handler function
    return asset_list

source_dataset()