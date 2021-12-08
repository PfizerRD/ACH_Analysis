import boto3
from pfawsaccess import *
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def download_aws(s3_object):
    from os import makedirs, sep

    bucket = s3_object.bucket_name
    path = s3_object.key
    #local_path = local_path + path
    local_path = f'/Users/psaltd/Desktop/achondroplasia/data/{path}'

    # only check the directory part
    makedirs(sep.join(local_path.split('/')[:-1]), exist_ok=True)
    # download from the bucket, with the bucket path, to the local path
    download_object(bucket, path, local_path)

    return local_path

class downloads():

    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.download_aws(self.s3_object)

    def download_aws(self, s3_object):
        import os

        #s3_object = self.s3_object
        bucket = s3_object.bucket_name
        path = s3_object.key
        # local_path = local_path + path
        local_path = f'/Users/psaltd/Desktop/achondroplasia/data/{path}'
        if os.path.exists(local_path):
            print('{} is already downloaded!'.format(local_path.split('/')[-1]))
            pass
        else:
            # only check the directory part
            os.makedirs(os.sep.join(local_path.split('/')[:-1]), exist_ok=True)
            # download from the bucket, with the bucket path, to the local path
            download_object(bucket, path, local_path)
            print('%s has been downloaded!' % s3_object.key.split('/')[-1])

def filter_files(files, include, exclude, extension):
    '''

    :param files: The collection of files on AWS
    :param include: Inclusion criteria
    :param exclude: Exclusion criteria
    :param extension: Desired file type extension
    :return: List of filtered filenames
    '''
    if '*' in exclude:
        mask = [False] * len(files)

        for i, file in enumerate(files):
            mask[i] |= (file.suffix.lower() == extension.lower())
            mask[i] |= any([s.lower() in str(file).lower() for s in include]) or (include == [])
    elif '*' in include:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

    else:
        mask = [True] * len(files)

        for i, file in enumerate(files):
            mask[i] &= (file.suffix.lower() == extension.lower())
            mask[i] &= any([s.lower() in str(file).lower() for s in include]) or (include == [])
            mask[i] &= not any([s.lower() in str(file).lower() for s in exclude])

        for i in range(len(mask) - 1, -1, -1):
            if not mask[i]:
                del files[i]

    return files

def hdf_to_data(hdf_data):
    '''
    Convert a hdf object to data frame and take median
    :param hdf_data: This is hdf hdf object
    :return: the median of the data
    '''
    npdf = np.array(hdf_data)
    df = pd.DataFrame(npdf)

    median_data = np.nanmedian(df)

    return median_data

def get_ACH_files_AWS():
    '''
    This function collects the filenames for APDM QC as a dataframe
    :param task: the type of task you are looking for:
        ['Walk', 'Sway', 'Sit_to_Stand', 'TUG', 'Analysis']
    :return: a dataframe with the filename info for S3
    '''

    path = 's3://dtbsprodamrasp128127'
    ## need samlutil permissions for the GAS study

    #tasks = ['Carpet', 'Normal', 'Fast', 'Tile', 'Slow', 'Activities_Part1', 'Activities_Part2', 'Elliptical', 'OPAL']

    prefix = 'raw_zone/c4181001/sensordata/'
    extension = '.bin'

    exclude = ''
    include = '*'#['.bin', '.Wlk', '.MDB']

    s3 = boto3.Session(profile_name='saml').resource('s3')
    bucket = s3.Bucket(path[5:])
    files1 = [Path(i.key) for i in bucket.objects.filter(Prefix=prefix)]
    files = filter_files(files1, include, exclude, extension)

    ## create the df of info
    filename_df = []
    for file in files:
        f = str(file)
        [lz, study, toss, filename] = f.split('/')
        if '.bin' in filename:
            try:
                [start, end] = filename.split(' ', -1)
                [subject, devloc, devID, createdate] = start.split('_')
                filetype = 'geneactiv'
            except ValueError:
                filename = filename.replace(' ', '-', 1) #One file has a space before the wrist (adding logic to handle)
                [start, end] = filename.split(' ', -1)
                [subject, devloc, devID, createdate] = start.split('_')
                filetype = 'geneactiv'
        else:
            try:
                [subject, end] = filename.split('_', 1)
                devID = 'gaitrite'
                devloc = 'gaitrite'
                createdate = ''
                filetype = 'gaitrite.wlk'
            except ValueError:
                subject = ''
                devID = 'gaitrite'
                devloc = 'gaitrite'
                filetype = 'gaitrite.MDB'
                createdate = ''

        obj = {'subject': subject,
               'study': study,
               'deviceID': devID,
               'device_location': devloc,
               'file_creation': createdate,
               'file_type': filetype,
               's3_obj': s3.Object(bucket.name, f)}
        filename_df.append(obj)
    filename_df = pd.DataFrame(filename_df)

    return filename_df

if __name__ == '__main__':
    files = get_ACH_files_AWS()
    [downloads(x.s3_obj) for y, x in tqdm(files.iterrows())]