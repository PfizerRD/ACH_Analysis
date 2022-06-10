from pathlib import Path

import pandas as pd
import numpy as np
import skdh
import os
import matplotlib as mpl
mpl.use('MacOSX')
import matplotlib.pyplot as plt
from datetime import datetime
import pytz
from scipy.signal import find_peaks
from matplotlib.widgets import Slider, Button
from tqdm import tqdm

plt.style.use("ggplot")

EST = pytz.timezone("US/Eastern")


# def get_manual_synchronization(ax6_data, sync_path):
def get_manual_synchronization(ga_time, ga_accel, pkm_time, pkm_press_sum):
    """
    Get the synchronization between GeneActiv and PKMass
    Returns
    -------
    sync : float
        Synchronization offset in seconds
    """
    acc_mag = np.linalg.norm(ga_accel, axis=1)
    N = int(75 * 50)  # 30s x 50hz # gives you the amount of data

    i1 = np.argmin(np.abs(ga_time - pkm_time[0]))
    i2 = np.argmin(np.abs(ga_time - pkm_time[-1]))

    f, (ax, sliderax, buttonax) = plt.subplots(
        nrows=3,
        figsize=(10, 5),
        gridspec_kw={'height_ratios': [1, 0.2, 0.1]},
    )
    ax.set_title('PKMas - GeneActiv Alignment')
    ax.set_xlabel('Time [s]')

    pkmas_line, = ax.plot(pkm_time, pkm_press_sum / pkm_press_sum.max())
    lims = ax.get_xlim()
    # ax6_line, = ax.plot(ga_time[i1 - N:i2 + N], acc_mag[i1 - N:i2 + N])
    ax6_line, = ax.plot(ga_time[i1 - N:i2 + N], acc_mag[i1 - N:i2 + N])

    ax.set_xlim(lims)

    sync_slider = Slider(
        ax=sliderax,
        label='Sync Offset [s]',
        valmin=-100.0,
        valmax=100.0,
        valinit=0
    )

    button = Button(buttonax, "Set to 0.0")

    def reset(event):
        sync_slider.set_val(0.0)

    button.on_clicked(reset)

    def update(val):
        ax6_line.set_xdata(ga_time[i1 - N:i2 + N] + sync_slider.val)
        f.canvas.draw_idle()

    sync_slider.on_changed(update)

    plt.show(block=True)

    return sync_slider.val


def synchronize_geneactive_gaitrite(geneactive_file, gaitrite_sensor_file, task_file, const_offset=5.0):
    """
    Get the synchronization between a GeneActive and GaitRite recording

    Parameters
    ----------
    geneactive_file : str, Path-like
        Path to a GeneActive file
    gaitrite_sensor_file : str, Path-like
        Path to the associated GaitRite sensor file. This is the file containing
        the pressure sensor level response.
    task_file : str, Path-like
        Name of the file to save the resulting segment of GeneActive task data to.
    const_offset : float, optional
        Constant number of extra seconds to get in the GeneActiv recording. Default is 5.0 seconds

    Returns
    -------
    offset : float
        The offset between the GaitRite time and the GeneActive timestamps. Subtract this time
        from the GaitRite timestamps to get the actual time.
    """
    ga = skdh.io.ReadBin().predict(file=geneactive_file)

    gr_header = pd.read_csv(gaitrite_sensor_file, header=None, names=['meta', 'val'], nrows=10, usecols=(0, 1),
                            index_col=0)
    gr = pd.read_csv(gaitrite_sensor_file, skiprows=11)

    # get the timestamp
    gr_timestamp = pd.to_datetime(gr_header.loc['Test Time', 'val'], utc=True)
    gr_time = int(gr_timestamp.to_datetime64()) / 1e9

    # groupby time and sum
    press = gr.groupby('Time (sec.)', as_index=False).sum('Level')
    press['time'] = press['Time (sec.)'] + gr_time

    # get the offset
    print(np.ceil(len(ga['time']) / 2))
    offset = get_manual_synchronization(ga['time'], ga['accel'], press['time'].values,
                                        press['Level'].values)  # edited for manual alignment of errored signal
    # offset = get_manual_synchronization(ga['time'], ga['accel'], press['time'].values, press['Level'].values)

    # get indices for the final geneactiv recording
    i1 = np.argmin(np.abs(ga['time'] - (gr_time - offset - const_offset)))
    i2 = np.argmin(np.abs(ga['time'] - (press['time'].values[-1] - offset + const_offset)))

    # get the task recording data subset
    task_time = ga['time'][i1:i2]
    task_accel = ga['accel'][i1:i2]

    # plot the data for any visual inspection of alignment
    f, ax = plt.subplots(figsize=(13, 5))
    ax.plot(press['time'].values - offset, press['Level'] / press['Level'].max(), label='GR Press. Sum.')
    ax.plot(task_time, np.linalg.norm(task_accel, axis=1), label='GA Accel.')
    ax.set_xlabel('Time')
    ax.set_ylabel('Signal')
    ax.legend()
    f.tight_layout()
    save_path = '../data/gaitrite_20220610/'
    f.savefig(Path(save_path + task_file).with_suffix(".png"))

    if not os.path.exists(save_path): os.mkdir(save_path)
    # save the data
    np.savez(save_path+task_file, time=task_time, accel=task_accel, fs=ga['fs'])

    return offset

if __name__ == '__main__':
    qc_file = '/Users/psaltd/Documents/GitHub/ACH_Analysis/src/results/C4181001_GA_QC_20220610.csv'
    qc_df = pd.read_csv(qc_file)
    gaitrite_qcs = qc_df[~qc_df.pkmas_filename.isna()]

    gaitrite_path = '/Users/psaltd/Documents/GitHub/ACH_Analysis/data/gaitrite_20220608/'
    ga_filepath = '/Users/psaltd/Desktop/achondroplasia/data/raw_zone/c4181001/sensordata/'
    ga_segments = []
    offsets = []
    ga_files = []
    for index, row in tqdm(gaitrite_qcs.iterrows()):
        #print(row)
        geneA_name = os.path.join(ga_filepath, row.filename)
        geneA_save_name = row.filename.strip('.bin') + '_gait_task'
        # if os.path.exists(
        #     '/Users/psaltd/Desktop/achondroplasia/data/GENEActiv_GAITRite_Alignment/' + geneA_save_name + '.npz'): continue
        if os.path.exists('../data/gaitrite_20220610/' + geneA_save_name + '.npz'): continue
        gaitrite_name = row.pkmas_filename
        if len(gaitrite_name) > 70:
            print(gaitrite_name)
            tmp_list = gaitrite_name.strip('[').strip(']')
            gaitrite_names = tmp_list.split(', ')
            for name in gaitrite_names:
                name = name.strip(']').strip('[')
                which_vis = '_'.join(name.split('_')[1:]).strip(".csv'")
                geneA_save_name = '{}_v{}_{}'.format(row.filename.strip('.bin'), which_vis, 'gait_task')
                if os.path.exists('../data/gaitrite_20220610/' + geneA_save_name + '.npz'): continue
                gait_name_final = os.path.join(gaitrite_path, name.strip("'"))
                offset = synchronize_geneactive_gaitrite(geneA_name,
                                                         gait_name_final,
                                                         geneA_save_name)
                offsets.append(offset)
                ga_segments.append(geneA_save_name)
                ga_files.append(geneA_name)
                print(offset, geneA_save_name)
        else:
            # print(gaitrite_name.__len__())
            gait_name_final = os.path.join(gaitrite_path, gaitrite_name)
            # This file from GBR-03-002 is not able to be read with SKDH
            if geneA_name == '/Users/psaltd/Desktop/achondroplasia/data/raw_zone/c4181001/sensordata/GBR-03-002_left wrist_059546_2022-03-15 13-36-12.bin':
                continue
            offset = synchronize_geneactive_gaitrite(geneA_name,
                                                     gait_name_final,
                                                     geneA_save_name)
            offsets.append(offset)
            ga_files.append(geneA_name)
            ga_segments.append(geneA_save_name)
            print(offset, geneA_save_name)

    # gaitrite_qcs['geneactiv_gait_segments'] = ga_segments
    # gaitrite_qcs['offsets'] = offsets
    new_df = pd.DataFrame({'ga_files': ga_files, 'ga_segments': ga_segments, 'offsets': offsets})
    new_df['filename'] = [x.split('/')[-1] for x in new_df.ga_files.values]

    gaitrite_qcs = gaitrite_qcs.merge(new_df)
    gaitrite_qcs.to_csv('./results/C4181001_gaitrite_alignment_20220610.csv', index=False)

    # [-1.9354838709677438, 'DNK-01-023_left wrist_059554_2021-09-09 11-25-58_visit1_test1_PKMAS_carpet_sensor_gait_task']
    # [-1.1612903225806406, 'DNK-01-023_left wrist_059555_2022-01-10 07-53-01_visit2_test2_PKMAS_carpet_sensor_gait_task']
    # [-0.6451612903225907, 'DNK-01-023_left wrist_059555_2022-01-10 07-53-01_visit2_test3_PKMAS_carpet_sensor_gait_task']
    # [-0.6451612903225907, 'DNK-01-023_left wrist_059555_2022-01-10 07-53-01_visit2_test1_PKMAS_carpet_sensor_gait_task']
    # [-1.9354838709677438, 'DNK-01-023_left wrist_059554_2021-09-09 11-25-58_visit1_test1_PKMAS_carpet_sensor_gait_task']
    # [-1.4193548387096655, 'DNK-01-023_left wrist_059554_2021-09-09 11-25-58_visit1_test2_PKMAS_carpet_sensor_gait_task']
    # [-1.6774193548387188, 'DNK-01-023_back_059553_2021-09-09 12-19-54_visit1_test1_PKMAS_carpet_sensor_gait_task']
    # [-1.1612903225806406, 'DNK-01-023_back_059553_2021-09-09 12-19-54_visit1_test2_PKMAS_carpet_sensor_gait_task']