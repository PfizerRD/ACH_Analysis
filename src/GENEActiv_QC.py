import os
import re
from helpers import *
from datetime import datetime
import math

def GA_QC(filename):

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
           'filename': filename.split('/')[-1]}

    #print(obj)
    return obj

if __name__ == '__main__':
    files = get_ACH_files_AWS()
    [downloads(x.s3_obj) for y, x in tqdm(files.iterrows())]
    path = '/Users/psaltd/Desktop/achondroplasia/data/raw_zone/c4181001/sensordata/'
    files = os.listdir(path)
    outputs = []
    for f in files:
        if '.bin' in f:
            outputs.append(GA_QC(path + f))
        else:
            continue

    results = pd.DataFrame(outputs)
    results = results.sort_values('subject_ID')

    qc_file = '/Users/psaltd/Desktop/achondroplasia/QC/C4181001_GA_QC.csv'
    if os.path.exists(qc_file):
        qc_df = pd.read_csv(qc_file)
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

        date = datetime.today().strftime('%Y%m%d')
        #updated_QC.to_csv('/Users/psaltd/Desktop/Cachexia/Cax1009_GA_QC_%s.csv' % (date), index = False)
        updated_QC.to_csv('/Users/psaltd/Desktop/achondroplasia/QC/C4181001_GA_QC.csv', index = False)
    else:
        results.to_csv(qc_file, index=False)

    #GA_QC('/Users/psaltd/Downloads/DNK-01-002_back_059550_2021-09-06 14-44-40.bin')