#from gaitpy.gait import Gaitpy
import os
import pandas as pd
import numpy as np
import logging
import tailer as tl
import datetime
from pathlib import Path
from multiprocessing import Pool
#from src.helpers import read_QC
import sys
import skdh
import re
from tqdm import tqdm

class EpochError(Exception):
    pass

def create_epochTimes(time, start, end, epoch_level=60):
    '''
    This function creates windows for the epochs, given the start, end, and the epoch level

    :param start: start time of recording
    :param end: end time of recording
    :param epoch_level: epoch level in seconds (ie. 60) for 60s epochs
    :return:
    '''
    if epoch_level == 60:
        periods = pd.date_range(start = start, end = end, freq='T')
    else:
        raise EpochError('epoch value not valid')

    return periods

def createEpochHeader(file):
    '''

    :param file:
    :return:
    '''
    #get header
    with open(file, mode='rb') as file:
        fileContent = file.read()

    str_file = fileContent[:2000].decode('ascii')

    # load_template

    # d = pd.read_csv('GA_Epoch_Template.csv',
    #                 nrows=99, sep="\t+|,|", header = None)

    d = pd.read_csv('GA_Epoch_Template.csv',
                    nrows=99, header=None)

    #pull data from .bin header
    devID_obj = re.search('Device Unique Serial Code', str_file)
    start, end = devID_obj.span()
    devID = int(str_file[start:end + 10].split('\r')[0].split(':')[1])

    # dev loc
    loc_obj = re.search('Device Location Code', str_file)
    start, end = loc_obj.span()
    dev_loc = str_file[start:end + 12].split('\r')[0].split(':')[1]

    calDate_obj = re.search('Calibration Date', str_file)
    start, end = calDate_obj.span()
    calDate = str(str_file[start:end + 30].split('\r')[0]).split(':',1)[1]

    TZ_obj = re.search('Time Zone', str_file)
    start, end = TZ_obj.span()
    TZ = str(str_file[start:end + 12].split('\r')[0]).split(':', 1)[1]

    # sampling rate
    # Focus on the 1st page
    page1_start = re.search('Recorded Data', str_file)
    start, end = page1_start.span()
    page1_str = str_file[start:]
    start, end = re.search('Measurement Frequency', page1_str).span()
    freq_rate = float(page1_str[start:end + 10].split('\r')[0].split(':', 1)[1])
    freq_rate = '{} Hz'.format(str(freq_rate))

    # start time
    start, end = re.search('Page Time', page1_str).span()
    start_time_str = page1_str[start:end + 25].split('\r')[0].split(':', 1)[1]

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

    studyCode_obj = re.search('Study Code', str_file)
    start, end = studyCode_obj.span()
    try:
        studyCode = int(str_file[start:end + 10].split('\r')[0].split(':', 1)[1])
    except ValueError:
        studyCode = ''

    ConfigTime_obj = re.search('Config Time', str_file)
    start, end = ConfigTime_obj.span()
    ConfigTime = str(str_file[start:end + 30].split('\r')[0].split(':', 1)[1])

    ExtractTime_obj = re.search('Extract Time', str_file)
    start, end = ExtractTime_obj.span()
    ExtractTime = str(str_file[start:end + 30].split('\r')[0].split(':', 1)[1])

    ExtractNotes_obj = re.search('Extract Notes', str_file)
    start, end = ExtractNotes_obj.span()
    ExtractNotes = str(str_file[start:end + 30].split('\r')[0].split(':', 1)[1])

    SubjectNotes_obj = re.search('Subject Notes', str_file)
    start, end = SubjectNotes_obj.span()
    SubjectNotes = str(str_file[start:end + 20].split('\r')[0].split(':', 1)[1]).strip()

    # change info to match what you just pulled from the .bin file
    d = d.set_index(d.iloc[:, 0]).drop(axis=0, columns=0).T
    d['Device Unique Serial Code'] = devID
    d['Device Location Code'] = dev_loc
    d['Calibration Date'] = calDate
    d['Time Zone'] = TZ
    d['Measurement Frequency'] = freq_rate
    d['Start Time'] = start_time_str
    d['Subject Code'] = subjID
    d['Study Centre'] = study_centre
    d['Study Code'] = studyCode
    d['Config Time'] = ConfigTime
    d['Extract Time'] = ExtractTime
    d['Extract Notes'] = ExtractNotes
    d['Subject Notes'] = SubjectNotes

    d_df = pd.DataFrame(d).T
    d_df = d_df.reset_index()

    return d_df

def generateEpochMetrics(df, period):
    '''
    This function receives a DataFrame as input and generates epoch summary metrics to be returned as row
    :param sub_df: input dataframe from epoch duration (ex. 60s segment of data)
    :param period: period of epoch duration
    :return: row with epoch summary values for segment
    '''

    #sub_df = df[(df.time >= period) & (df.time < period + pd.to_timedelta(1, unit='T'))]
    sub_df = df.truncate(period, period + pd.to_timedelta(59.99, unit='s'))

    # need to create row as a list
    xmean = np.mean(sub_df.x).__round__(4)
    ymean = np.mean(sub_df.y).__round__(4)
    zmean = np.mean(sub_df.z).__round__(4)
    lux_mean = int(np.mean(sub_df.lux))
    sum_button = int(np.sum(sub_df.button))
    mean_temp = np.mean(sub_df.temp).__round__(1)
    svm_s1 = np.sqrt(sub_df.x**2 + sub_df.y**2 + sub_df.z**2) - 1
    SVM = np.sum(np.abs(svm_s1)).__round__(2)
    xSTD = np.std(sub_df.x).__round__(4)
    ySTD = np.std(sub_df.y).__round__(4)
    zSTD = np.std(sub_df.z).__round__(4)
    peakLux = int(np.max(sub_df.lux))

    # row = [str(period).replace('.', ':')[:23], xmean, ymean, zmean, int(lux_mean), int(sum_button), mean_temp, SVM, xSTD,
    #        ySTD, zSTD, int(peakLux)]
    if not str(period).endswith('00'):
        period = str(period) + ':000'
    elif str(period).endswith(':00'):
        period = str(period) + ':000'
    else:
        pass
    row = [str(period).replace('.', ':'), xmean, ymean, zmean, int(lux_mean), int(sum_button), mean_temp, SVM, xSTD,
           ySTD, zSTD, int(peakLux)]
    return row

def geneActiv_epoch_generator(file, savename):
    #final_df = createEpochHeader(file)
    dat = skdh.io.ReadBin(bases=[0], periods=[24]).predict(file=file)

    timestamps = pd.to_datetime(dat['time']*1000000000, format='%Y-%m-%d %H:%M:%S')
    periods = create_epochTimes(timestamps, timestamps[0], timestamps[-1])
    accel = pd.DataFrame(dat['accel'])
    df = pd.DataFrame({'time': timestamps, 'x': accel.iloc[:, 0], 'y': accel.iloc[:, 1], 'z': accel.iloc[:, 2],
                       'lux': dat['light']})
    df['button'] = 0
    df['temp'] = dat['temperature']
    df = df.set_index(df.time)

    epoch_data = []
    # group by minute and derive endpoints
    epoch_data = [generateEpochMetrics(df, p) for p in tqdm(periods) if not p == periods[-1]]
    epoch_df = pd.DataFrame(epoch_data)

    final_df = createEpochHeader(file)

    for i in range(20):
        final_df = final_df.append(pd.Series(), ignore_index=True)

    final_df = final_df.append(epoch_df, ignore_index = True)
    if not os.path.exists('../data/processed/'):
        os.mkdir('../data/processed/')
    save_dir = '../data/processed/epochs/'
    if os.path.exists(save_dir):
        pass
    else:
        os.mkdir(save_dir)
    final_df.to_csv(save_dir + savename, header=None, index=False)

    print()
    print('{} has been converted to epoch'.format(file))
    print()

def plot_GA_Epochs(file, show_flag=False):
    import matplotlib.pyplot as plt

    df = pd.read_csv(file, skiprows = 99, header=None)
    df.timestamp = pd.to_datetime(df.iloc[:, 0], format='%Y-%m-%d %H:%M:%S:%f')

    #plot
    plt.figure(figsize=(15,5))
    plt.plot(df.timestamp, df.iloc[:,1], label='x')
    plt.plot(df.timestamp, df.iloc[:, 2], label='y')
    plt.plot(df.timestamp, df.iloc[:, 3], label='z')
    plt.legend()
    plt.xlabel('time')
    plt.ylabel('accel (g)')
    plt.title('{}'.format(file.split('.')[0]))
    plt.tight_layout()
    save_path = '../data/processed/GENEActiv_Raw_Activity/'
    if os.path.exists(save_path):
        pass
    else:
        os.mkdir(save_path)
    plt.savefig(save_path + '{}_GENEActiv_Raw_Activity.pdf'.format(file.split('/')[-1].split('.')[0]))

    if show_flag == True:
        plt.show()
    else:
        pass

if __name__ == '__main__':
    #path = '../data/raw/'
    path = '/Users/psaltd/Desktop/achondroplasia/data/raw_zone/c4181001/sensordata/'
    for file in os.listdir(path):
        if not file.endswith('.bin'):
            continue

        #verify with QC
        qc_file = './results/C4181001_GA_QC_20220601.csv'
        qc_df = pd.read_csv(qc_file)
        qc_df = qc_df[qc_df.Usable != 0]
        sub_qc = qc_df[qc_df.filename == file]
        if sub_qc.empty:
            continue
        try:
            sub_qc_visit = int(sub_qc.visit_number.iloc[0])
        except:
            print('{} does not have a visit # from CRF... cannnot process'.format(file))
            continue

        save_path = '../data/processed/epochs/'
        save_name = '{}_visit{}_60s_epoch.csv'.format(file.split('.')[0], str(sub_qc_visit))
        if os.path.exists(os.path.join(save_path, save_name)):
            continue
        try:
            geneActiv_epoch_generator(path+file, save_name)
        except:
            print('{} could not be processed'.format(file))
        try:
            plot_GA_Epochs(save_path+save_name)
        except:
            print('Could not plot {}'.format(save_name))