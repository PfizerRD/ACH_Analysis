# THis file will QC THE PKMAS DATA from the Ach study

# Pull scripts from DMTI Devices and Analytics_QC_Scripts

# # Load in the Processed PKMAS fiels

# Run QC and output results
import os

import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import *
from helpers import *
import sys
sys.path.insert(1, '/Users/psaltd/Documents/GitHub/analytics_qc_scripts/GAITRite/')
from PKMAS_QC import *

def run_PKMAS_qc(filepath):

    file = filepath.split('/')[-1]
    # try:
    #     subject, end = file.split('_')
    #     visit_number = 1
    #     test_number = 1
    # except ValueError:
    #     try:
    #         subject, visit, end = file.split('_')
    #         visit_number = visit.strip('visit')
    #         test_number = 1
    #     except:
    #         subject, visit, test, end = file.split('_')
    #         visit_number = visit.strip('visit')
    #         test_number = test.strip('test')
    # pkmas_df = pd.read_csv(filepath)
    [subject, test_time, cadence, pkmas_df] = pkmas_txt_reader(filepath, 'metrics')

    ## Check Meta info ----
    #subject = pkmas_df.iloc[0, 1].split(',')[1].strip()

    #Check Full Name + ID
    #fullname = pkmas_df.iloc[0,1].strip().replace(', ', '_')
    #corr_filename = fullname.split('_')[1] == subject #Check that the subject ID matches the folder subject
    corr_filename = subject in filepath
    ##Check matching folders

    #Test time
    #test_time = str(pd.to_datetime(pkmas_df.iloc[6, 1]))

    #Memo
    # memo = pkmas_df.iloc[8, 1]
    # if str(memo) == 'nan':
    #     corr_memo = 'No memo entered'
    # else:
    #     corr_memo = 'error with memo'

    # columns = pkmas_df.iloc[10, :]
    # sub_df = pkmas_df.iloc[26:, :]
    sub_df = pkmas_df.copy()
    columns = sub_df.columns.tolist()
    columns[1] = 'Label'
    sub_df.columns = columns
    try:
        sub_df['lapnum'] = [x.split(':')[0] for x in sub_df.Label]
    except:
        columns = pkmas_df.iloc[11, :]
        sub_df = pkmas_df.iloc[27:, :]
        columns[1] = 'Label'
        sub_df.columns = columns
        memo = pkmas_df.iloc[8, 1]
        sub_df['lapnum'] = [x.split(':')[0] for x in sub_df.Label]

    step_df = Step_check(sub_df, maxlaps = 6)
    asym_df = Asym_check(sub_df, expected_laps = 6)
    footstep_check = check_PKMAS_footsteps(sub_df, threshold=2.7, plot=False,
                    metrics = [0, 1, 5, 8, 9, 10, 11, 12, 13, 21, 22, 23, 24, 27, 28, 29, 30, 31, 32, 33, 36])
    # Returns the index of the step --- starting from 0
    try:
        check_df = asym_df.merge(step_df)
    except:
        pass

    check_df['Memo'] = '' #no memo for this study
    #check_df = memo_df.merge(step_df)
    #check_df = check_df.merge(memo_df)

    #check_df['trail'] = trailnum
    new_cols = list(check_df.columns)
    new_cols[0] = 'lapnum'
    check_df.columns = new_cols
    check_df['remove_steps'] = str(footstep_check)
    N_laps = num_laps_check(sub_df)
    check_df['laps'] = N_laps
    check_df['subject'] = subject
    check_df['test_time'] = test_time

    #check_df['visit'] = visit_number
    #check_df['test'] = test_number

    #just added 20211220
    step_backwards = check_backwards_steps(sub_df)
    check_df['backwards_steps'] = str(step_backwards)
    check_df['filename'] = file

    return check_df

if __name__ == '__main__':
    #dat_path = '../data/gaitrite_20220608/'
    dat_path = '../data/pkmas_txt_files/'
    #files = [x for x in os.listdir(dat_path) if x.endswith('PKMAS.csv')]
    files = [x for x in os.listdir(dat_path) if x.endswith('G.txt')]
    results = []
    for file in tqdm(files):
        filepath = os.path.join(dat_path, file)
        if os.path.exists(filepath):
            out = run_PKMAS_qc(filepath)
            results.append(out)
        else:
            print('{} was not able to be processed'.format(file))

    results_df = pd.concat(results)
    save_files(results_df, 'C4181001_pkmas_qc')