#Import libraries
import matplotlib.pyplot as plt
import pandas as pd
from helpers import *
import os
import numpy as np
from datetime import datetime
from tqdm import tqdm

#Processing of PKMAS data

def apply_pkmasQC(df, qc_df):
    steps_to_remove = qc_df.remove_steps.unique()[0].strip('[').strip(']').split(' ')
    df_step1 = pd.concat([row.T for index, row in df.iterrows() if row.iloc[0] not in steps_to_remove], axis = 1).T

    #step 2 check - < 50% steps
    indx_less50perc = qc_df[qc_df['50perc_steps'] == 1.0]
    if indx_less50perc.empty:
        df_step2 = df_step1
    else:
        # remove all steps from laps with <50% steps
        df_step2 = pd.concat([row for index, row in df_step1.iterrows()
                                 if row.iloc[1].split(':')[0] not in indx_less50perc.lapnum], axis=1).T

    ## remove backwards steps
    backwards_steps = pd.DataFrame([row for index, row in qc_df.iterrows() if row.backwards_steps.startswith('(True')])
    if backwards_steps.empty:
        df_step3 = df_step2
    else:
        unique_back = backwards_steps.backwards_steps.unique()
        indeces_back = [int(x.split(', ')[1].strip(')')) for x in unique_back]
        df_step3 = pd.concat([row for index, row in df_step2.iterrows() if row.iloc[0] not in indeces_back], axis =1).T

    #asymmetric step removal
    asyn_steps = [x.strip('[').strip(']') for x in qc_df.asym_error if not x == '[]']
    try:
        asyn_steps = asyn_steps[0].split(', ')
    except:
        pass

    if len(asyn_steps) > 0:
        df_step4 = pd.concat([row for index, row in df_step3.iterrows() if row.iloc[0]
                                  not in asyn_steps], axis=1).T
    else:
        df_step4 = df_step3
    if (qc_df.subject.unique()[0] == 'DNK-01-016') & (qc_df.visit.unique()[0] == 1) & (qc_df.test.unique()[0] == 1):
        setindex = []

    return df_step4

def get_PKMAS_medians(filepath , qc_file = './results/C4181001_pkmas_qc_20220630.csv'):
    #read in qc file
    file = filepath.split('/')[-1]
    try:
        subject, end = file.split('_')
        visit_number = 1
        test_number = 1
    except ValueError:
        try:
            subject, visit, end = file.split('_')
            visit_number = visit.strip('visit')
            test_number = 1
        except:
            subject, visit, test, end = file.split('_')
            visit_number = visit.strip('visit')
            test_number = test.strip('test')

    # if subject == 'DNK-01-029':
    #     print('stop')
    qc_df = pd.read_csv(qc_file)
    qc_row = qc_df[(qc_df.subject == subject) & (qc_df.visit == int(visit_number)) &
    (qc_df.test == int(test_number))]

    #Read file from AWS and take median of metrics
    df = pd.read_csv(filepath)
    columns = df.iloc[10, :]
    sub_df = df.iloc[26:, :]
    sub_df.columns = columns

    sub_df = apply_pkmasQC(sub_df, qc_row)  # Uncomment to implement QC checks
    #Need to reduce dataframe to isolate the raw data
    medians_subdf = np.nanmedian(sub_df.iloc[:, 6:].astype('float'), axis=0)
    medians_subdf = pd.DataFrame([medians_subdf])
    medians_subdf.columns = sub_df.iloc[:, 6:].columns

    if columns[5:].isnull().values.any(): #Most likely a formatting error on the file from PKMAS (skip 1 more row)
        columns = df.iloc[11, :]
        sub_df = df.iloc[27:, :]
        sub_df.columns = columns
        medians_subdf = np.nanmedian(sub_df.iloc[:, 6:].astype('float'), axis=0)
        medians_subdf = pd.DataFrame([medians_subdf])
        medians_subdf.columns = sub_df.iloc[:, 6:].columns
    else:
        pass

    #Get the Cadence (mean - steps/min)
    cadence_col = df.iloc[:,3].dropna().reset_index(drop = True)
    if 'Cadence' in cadence_col.iloc[0]: #inspect the column to make sure its correct!
        # TODO: confirm with our own check on cadence
        cadence = float(cadence_col.iloc[1])
        #estimate cadence ourselves -- cadence = 1/(median(Step time)) * 60 or
        # estimate cadence ourselves -- cadence = 1/(median(Stride time)) * 120 -- 2*60 for strides
        estimated_cadence = 1/np.nanmedian(sub_df['Step Time (sec.)'].astype('float')) * 60
    else:
        raise ValueError

    #Join file metadata for indexing
    meta_obj = pd.DataFrame([{'subject': subject,
                              'visit': visit_number,
                              'test': test_number,
                              'filename': filepath.split('/')[-1],
                              'mean_cadence': cadence,
                              'estimated_median_cadence': round(estimated_cadence, 2),
                              'valid_steps': len(sub_df)}])

    medians_subdf = meta_obj.join(medians_subdf)

    return medians_subdf

def runPKMASprocessing():
    results = []
    #dat_path = '../data/gaitrite/'
    dat_path = '../data/gaitrite_20220608/'
    files = [x for x in os.listdir(dat_path) if x.endswith('PKMAS.csv')]
    for file in tqdm(files):
        out = get_PKMAS_medians(os.path.join(dat_path, file))
        if np.nan in out:
            raise Warning
        results.append(out)

    res_df = pd.concat(results)

    date = datetime.today().strftime('%Y%m%d')
    save_path = './results/'
    if os.path.exists(save_path):
        pass
    else:
        os.makedirs(save_path)

    save_files(res_df, 'C4181001_pkmas_gait_metrics')

if __name__ == '__main__':
    runPKMASprocessing()

