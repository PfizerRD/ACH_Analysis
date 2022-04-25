import sys
import os
import pandas as pd
from helpers import *

sys.path.insert(1, '/Users/bluesky/Documents/GitHub/dmtiDevices/geneActiv/')
from geneActiv_epoch_generator import *
from geneActiv_macros import *

def generate_gaEpoch():

    files = get_ACH_files_AWS()
    ga_files = files[(files.file_type == 'geneactiv') | (files.file_type == 'genactiv')]
    ga_files = pd.DataFrame([row for index, row in ga_files.iterrows() if not '.csv' in row.s3_obj.key])
    [downloads(x.s3_obj) for y, x in tqdm(ga_files.iterrows())]

    #get QC file info
    qc_df = pd.read_csv('./results/C4181001_GA_QC.csv')

    path =  '/Volumes/Promise_Pegasus/ach/raw_zone/c4181001/sensordata/'
    files = os.listdir(path)
    save_path = '/Volumes/Promise_Pegasus/ach/processed/geneactiv_epoch/'
    for f in files:
        subject_row = qc_df[qc_df.filename == f]

        if (not subject_row.empty) & (subject_row.Usable.values != 0):
            save_name = save_path + '{}_{}_visit{}'.format(subject_row.subject_ID.values[0], subject_row.dev_loc.values[0],
                                                           int(subject_row.Visit.values[0])) +'_epoch60s.csv'
            if os.path.exists(save_name):
                continue
            else:
                geneActiv_epoch_generator(path+f, save_name)
        else:
            continue

def geneactiv_macros(macrotype, position):
    path = '/Volumes/Promise_Pegasus/ach/processed/geneactiv_epoch/'
    files = os.listdir(path)
    save_path = '/Users/bluesky/Desktop/Anorexia/processed/{}/'.format(macrotype)
    #########

    #########

    for f in tqdm(files):
        # Isik does not want lumbar files
        if not position in f:
            continue
        if os.path.exists(save_path):
            pass
        else:
            os.mkdir(save_path)
        ##for debugging
        [subject, devloc, visit, end] = f.split('_')
        save_name = '{}_{}_{}_{}_processed.csv'.format(subject, visit, devloc, macrotype)
        if os.path.exists(save_path + save_name):
            continue
        else:
            print('{} is being processed...'.format(f))
            if macrotype == 'Sleep_macro':
                geneactiv_sleep_macro_csv(path + f, save_path + save_name)
            if macrotype == 'Everyday_living_macro':
                geneactiv_everyday_living_macro_csv(path + f, save_path + save_name)
        time.sleep(3)


if __name__ == '__main__':
    #geneactiv_macros('Everyday_living_macro', 'wrist')
    generate_gaEpoch()
