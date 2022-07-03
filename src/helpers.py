import boto3
from pfawsaccess import *
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import os
import datetime

def download_aws(s3_object):
    from os import makedirs, sep

    bucket = s3_object.bucket_name
    path = s3_object.key
    local_path = f'/Volumes/Promise_Pegasus/ach/{path}'
    #local_path = f'/Users/psaltd/Desktop/achondroplasia/data/{path}'

    # only check the directory part
    makedirs(sep.join(local_path.split('/')[:-1]), exist_ok=True)
    # download from the bucket, with the bucket path, to the local path
    download_object(bucket, path, local_path)

    return local_path

class downloads():
    '''
    Class which downloads data from S3 location
    '''

    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.download_aws(self.s3_object)

    def download_aws(self, s3_object):
        '''
        Function to download the s3 objects
        :param s3_object:
        :return:
        '''
        import os

        #s3_object = self.s3_object
        bucket = s3_object.bucket_name
        path = s3_object.key
        #local_path = f'/Volumes/Promise_Pegasus/ach/{path}'
        local_path = f'/Users/psaltd/Desktop/achondroplasia/data/{path}' #TODO: change to directory data folder
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
    This function collects the files from S3 and returns their paths, names, and device types as a dataframe

    :return: a dataframe with the filename info for S3

    '''

    path = 's3://dtbsprodamrasp128127'
    ## need samlutil permissions for the GAS study

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
            if filename.endswith('.Wlk'):
                filename = filename.replace(' ', '')
                filename = filename.replace('-', '_')
                #[subject, end] = filename.split('_', 1)
                try:
                    [subject, end] = filename.split('_g', 1)
                except:
                    [subject, end] = filename.split('_G', 1)

                subject = subject.replace('_', '-')
                devID = 'gaitrite'
                devloc = 'gaitrite'
                createdate = ''
                filetype = 'gaitrite.wlk'
            else:
                filename = filename.replace(' ', '')
                filename = filename.replace('-', '_')
                subject = filename.split('_G')[0]
                subject = subject.replace('_', '-')
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

def save_files(df, save_name):
    '''
    Script for saving files with a specific naming convention and incorporating the timestamp into the name. Only saves
    data in .csv format at this time.

    :param df: dataframe you are interested in saving to a file
    :param save_name: a string - naming convention which can be used for saving the dataframe to .csv
    :return: None - file is outputted to results directory
    '''
    save_path = './results/'
    if os.path.exists(save_path):
        pass
    else:
        os.makedirs(save_path)
    date = datetime.datetime.today().strftime('%Y%m%d')
    df.to_csv(save_path+'{}_{}.csv'.format(save_name, date), index=False)

def read_visit_data(visit_date, file='../data/CRF/SV2.csv', subject=None):
    '''
    Function to read the visit data information and return the visit dates
    :param file: '/Users/psaltd/Desktop/achondroplasia/C4181001_VISIT_DATE_09FEB2022.xlsx'
    :param subject: Optional: Subject ID
    :return:
    '''
    visit_df = pd.read_csv(file)
    qc_df = pd.read_csv('./results/C4181001_GA_QC.csv')
    sub_visit= visit_df[(visit_df.Subject == subject) &
                        (visit_df.SVDAT_YYYY == float(pd.to_datetime(visit_date, format="%Y-%m-%d %H:%M:%S:%f").date().year)) &
                        (visit_df.SVDAT_MM == float(pd.to_datetime(visit_date, format="%Y-%m-%d %H:%M:%S:%f").date().month)) &
                        (visit_df.SVDAT_DD == float(pd.to_datetime(visit_date, format="%Y-%m-%d %H:%M:%S:%f").date().day))]
    if sub_visit.empty:
        print('Subject: {} visit: {} not found in CRF data'.format(subject, visit_date))
        visit_num = ''
        visit_num_string = ''
    elif len(sub_visit) == 1:
        visit_num_string = sub_visit.InstanceName.values[0].strip(' (1)')
        visit_num = int(sub_visit.InstanceName.values[0].split(' ')[1])
    else:
        raise ValueError

    return visit_num, visit_num_string

def get_PKMAS_visit(subject, visit_date):
    '''
    Check geneactiv start date vs pkmas file recording dates. Want to verify which pkmas files correspond to a geneactiv
    .bin file. For mapping purposes

    :param subject: subject ID (string)
    :param visit_date: visit date (string or datetime)
    :return: matches_gaitrite (1 or 0 if a geneactiv files has a matching pkmas file), correct_file (filename of
    corresponding file)
    '''
    #pkmas_path = '../data/pkmas_metrics/'
    pkmas_path = '../data/pkmas_txt_files/' #changed to for txt files
    files = os.listdir(pkmas_path)
    # subject_file = [x for x in files if subject in x and x.endswith('carpet_sensors.csv')]
    subject_file = [x for x in files if subject in x and x.endswith('G.txt')]
    if subject_file:
        gr_visit_dates = []
        for sf in subject_file:
            pkmas_file = os.path.join(pkmas_path, sf)
            # gr_header = pd.read_csv(pkmas_file, header=None, names=['meta', 'val'], nrows=10, usecols=(0, 1),
            #                         index_col=0)
            [subject_reader, test_time, data] = pkmas_txt_reader(pkmas_file, 'metrics')

            # get the timestamp
            #gr_timestamp = pd.to_datetime(gr_header.loc['Test Time', 'val'])
            #gr_visit_date = gr_timestamp.date()
            gr_visit_date = test_time.date()
            gr_visit_dates.append(gr_visit_date)
        match_idx = [i for i in range(len(gr_visit_dates)) if gr_visit_dates[i] == visit_date]
        if len(match_idx) == 1:
            correct_file = subject_file[match_idx[0]]
            gr_visit_date = gr_visit_dates[match_idx[0]]
        elif len(match_idx) >1:
            gr_visit_date = np.unique(gr_visit_dates)
            correct_file = np.concatenate(np.array(pd.DataFrame(subject_file).iloc[match_idx].values.tolist())).tolist()
        else:
            correct_file = np.nan
            gr_visit_date = np.nan
    else:
        gr_visit_date = np.nan
        correct_file = np.nan

    try:
        if visit_date == gr_visit_date:
            matches_gaitrite = 1
        else:
            matches_gaitrite = 0
    except:
        if visit_date in gr_visit_date:
            matches_gaitrite = 1
        else:
            matches_gaitrite = 0

    return matches_gaitrite, correct_file


def pkmas_txt_reader(file, file_type):
    '''
    This function reads the PKMAS .txt files and returns the dataframe, subject ID, and test_time
    :param file: pkmas file (.txt)
    :param file_type: metrics or sensors - determines how reader operates and returns
    :return: [subjectID, test_time, data]
    '''
    #file = '/Users/psaltd/Documents/GitHub/ACH_Analysis/data/gaitrite_20220608/C4181004 DNK-01-017  - C4181004 - DNK-01-017 - DNK-01-017 - 5-4-2022 11-07-35 AM - E.txt'
    header_df = pd.read_csv(file, nrows=10, sep=';:')
    subject = header_df.iloc[0][0].split(';')[1].split(', ')[1]
    test_time = pd.to_datetime(header_df.iloc[6][0].split(';')[1])
    if file_type == 'metrics':
        data_columns = pd.read_csv(file, skiprows=10, nrows=1, sep=';').columns
        data = pd.read_csv(file, skiprows=27, sep=';', names=data_columns, header = None)

        #cadence
        cadence_dat=pd.read_csv(file, skiprows=14, nrows=1, sep=';')
        cadence_cols =pd.read_csv(file, skiprows=10, nrows=1, sep=';', header=None)
        cadence_dat = pd.DataFrame(cadence_dat)
        cadence_dat.columns = cadence_cols.values.tolist()
        cadence = cadence_dat['Cadence (steps/min.)'].values[0][0]

    elif file_type == 'sensors':
        data = pd.read_csv(file, skiprows=11, sep=';')
        cadence = []
    else:
        print('please enter a valid file type (sensors or metrics)')
        raise ValueError

    return subject, test_time, cadence, data

if __name__ == '__main__':
    pkmas_txt_reader(file = 'd', file_type='sensors')
    #read_visit_data('/Users/psaltd/Desktop/achondroplasia/C4181001_VISIT_DATE_09FEB2022.xlsx')
    files = get_ACH_files_AWS()
    [downloads(x.s3_obj) for y, x in tqdm(files.iterrows())]