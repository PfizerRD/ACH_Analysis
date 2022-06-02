import os
import re

import pandas as pd

from helpers import *
from datetime import datetime
import math

def GA_QC(filename):
    '''
    Function to extract relevant information from GENEActiv bin files for QC
    :param filename: filename of GENEActiv Bin file
    :return: dictionary of information for GENEActiv QC
    '''

    #String maniplulation to get subID
    f = filename.split('/')[-1]
    subj, position, device, end = f.split('_')
    extract_date = end.split('.')[0]

    #io.BytesIO(obj.get()['Body'].read()) -- Run from AWS
    with open(filename, mode='rb') as file:
        fileContent = file.read()

    str_file = fileContent[:2000].decode('ascii') #decode the first 2000 entries

    # pages
    pages_obj = re.search('Pages', str_file) #search for pages
    start, end = pages_obj.span() #get the start and end of the pages
    pages = int(str_file[start:end + 10].split('\r')[0].split(':')[1])

    # dev loc
    loc_obj = re.search('Device Location Code', str_file)
    start, end = loc_obj.span()
    dev_loc = str_file[start:end + 12].split('\r')[0].split(':')[1]

    # Extract time
    extract_obj = re.search('Extract Time', str_file)
    start, end = extract_obj.span()
    extract_time = str_file[start:end + 25].split('\r')[0].split(':', 1)[1]

    # Subject ID
    subj_obj = re.search('Subject Code', str_file)
    start, end = subj_obj.span()
    try:
        subjID = int(str_file[start:end + 10].split('\r')[0].split(':', 1)[1])
    except ValueError:
        subjID = ''

    # study center
    centre_obj = re.search('Study Centre', str_file)
    start, end = centre_obj.span()
    try:
        study_centre = int(str_file[start:end + 10].split('\r')[0].split(':', 1)[1])
    except ValueError:
        study_centre = ''

    # subj notes
    notes_obj = re.search('Subject Notes', str_file)
    start, end = notes_obj.span()
    notes_str = str_file[start:end + 10].split('\r')[0].split(':')[1].strip()

    # Focus on the 1st page
    page1_start = re.search('Recorded Data', str_file)
    start, end = page1_start.span()
    page1_str = str_file[start:]

    # start time
    start, end = re.search('Page Time', page1_str).span()
    start_time_str = page1_str[start:end + 25].split('\r')[0].split(':', 1)[1]

    # DevID
    start, end = re.search('Device Unique Serial Code', page1_str).span()
    devID = int(page1_str[start:end + 10].split('\r')[0].split(':', 1)[1])

    # sampling rate
    start, end = re.search('Measurement Frequency', page1_str).span()
    freq_rate = float(page1_str[start:end + 10].split('\r')[0].split(':', 1)[1])

    # Start_voltage
    start, end = re.search('Battery voltage', page1_str).span()
    voltage1 = float(page1_str[start:end + 10].split('\r')[0].split(':', 1)[1])

    # endstring
    last_page = fileContent[-4000:].decode('ascii')

    # end_voltage
    start, end = re.search('Battery voltage', last_page).span()
    voltage2 = float(last_page[start:end + 10].split('\r')[0].split(':', 1)[1])

    # End time
    start, end = re.search('Page Time', last_page).span()
    end_time_str = last_page[start:end + 25].split('\r')[0].split(':', 1)[1]

    recording_duration = pd.to_datetime(end_time_str, format="%Y-%m-%d %H:%M:%S:%f") - \
                         pd.to_datetime(start_time_str, format="%Y-%m-%d %H:%M:%S:%f")

    #TODO: Make a list of pass/fail criteria
    visit_number, visit_number_string = read_visit_data(visit_date = start_time_str, subject=subj)

    if (recording_duration.days >= 7) & (int(freq_rate) == 50):
        usable = 1
    else:
        usable = 0

    pkmas_visit_match = get_PKMAS_visit(subj, pd.to_datetime(start_time_str, format="%Y-%m-%d %H:%M:%S:%f").date())
    if pkmas_visit_match != 0:
        matches_pkmas = 1
    else:
        matches_pkmas = 0

    obj = {'subject_ID': subj,
           'subID_bin': subjID,
           'study_center': study_centre,
           'dev_loc': position,
           'device_loc_bin': dev_loc,
           'num_pages': pages,
           'devID': int(device),
           'devID_bin': devID,
           'start_time': start_time_str,
           'end_time': end_time_str,
           'recording_duration': recording_duration,
           'sampling_rate': freq_rate,
           'extract_time': extract_time,
           'extract_time_bin': extract_time,
           'start_volt': voltage1,
           'end_volt': voltage2,
           'setup_notes': notes_str,
           'filesize': '{:.2f} MB'.format(os.path.getsize(filename) / 1000000),
           'filename': filename.split('/')[-1],
           'visit_number': visit_number,
           'visit_number_string': visit_number_string,
           'Usable': usable,
           'matches_PKMAS': matches_pkmas}

    #print(obj)
    return obj

def run_GA_QC(path, download_flag=False):
    '''
    Run GA QC For ACH Study
    :param path: path to local data directory
    :return: Output is a QC file in the results directory
    '''
    if download_flag:
        files = get_ACH_files_AWS()
        [downloads(x.s3_obj) for y, x in tqdm(files.iterrows())]

    files = os.listdir(path)
    outputs = []
    for f in files:
        if '.bin' in f:
            outputs.append(GA_QC(path + f))
        else:
            continue

    results = pd.DataFrame(outputs)
    results = results.sort_values('subject_ID') #TODO: adjust the visit info to reflect the new visit dates from CRF

    #qc_file = '/Users/psaltd/Desktop/achondroplasia/QC/C4181001_GA_QC.csv'
    qc_file = './results/C4181001_GA_QC_20220601.csv'
    if os.path.exists(qc_file):
        qc_df = pd.read_csv(qc_file, encoding='latin-1')
        #change SUBJIDs to int
        qc_df['subID_bin'] = [int(x) if not math.isnan(x) else '' for x in qc_df.subID_bin]
        qc_df['study_center'] = [int(x) if not math.isnan(x) else '' for x in qc_df.study_center]

        updated_rows = []
        for index, row in results.iterrows():
            if row.filename not in qc_df.filename.values:
                updated_rows.append(row)

        new_rows = pd.DataFrame(updated_rows)

        updated_QC = pd.concat([qc_df, new_rows])
        updated_QC = updated_QC.reset_index(drop=True)

        save_files(updated_QC, 'C4181001_GA_QC')
    else:
        #results.to_csv(qc_file, index=False)
        save_files(results, 'C4181001_GA_QC')


if __name__ == '__main__':
    path = '/Users/psaltd/Desktop/achondroplasia/data/raw_zone/c4181001/sensordata/' # TODO: Change to working data directory
    run_GA_QC(path, download_flag=True)
