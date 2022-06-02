from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Slider, Button

import skdh

plt.style.use('ggplot')

def get_manual_synchronization(ga_time, ga_accel, pkm_time, pkm_press_sum):   
    """
    Get the synchronization between GeneActiv and PKMass
    
    Parameters
    ----------
    ga_time : numpy.ndarray
        Unix seconds timestamps
    ga_accel : numpy.ndarray
        (n, 3) array of acceleration
    pkm_time : numpy.ndarray
        Unix seconds timestamps for the GaitRite data
    pkm_press_sum : numpy.ndarray
        (n, ) array of pressure sums for the gaitrite mat

    Returns
    -------
    sync : float
        Synchronization offset in seconds
    """
    acc_mag = np.linalg.norm(ga_accel, axis=1)
    N = int(75 * 50)  # 30s x 50hz

    i1 = np.argmin(np.abs(ga_time - pkm_time[0]))
    i2 = np.argmin(np.abs(ga_time - pkm_time[-1]))

    f, (ax, sliderax, buttonax) = plt.subplots(
        nrows=3,
        figsize=(10, 5),
        gridspec_kw={'height_ratios': [1, 0.2, 0.1]},
    )
    ax.set_title('PKMAS - GeneActiv Alignment')
    ax.set_xlabel('Time [s]')

    pkmas_line, = ax.plot(pkm_time, pkm_press_sum / pkm_press_sum.max())
    lims = ax.get_xlim()
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
    
    gr_header = pd.read_csv(gaitrite_sensor_file, header=None, names=['meta', 'val'], nrows=10, usecols=(0, 1), index_col=0)
    gr = pd.read_csv(gaitrite_sensor_file, skiprows=11)

    # get the timestamp
    gr_timestamp = pd.to_datetime(gr_header.loc['Test Time', 'val'], utc=True)
    gr_time = int(gr_timestamp.to_datetime64()) / 1e9

    # groupby time and sum
    press = gr.groupby('Time (sec.)', as_index=False).sum('Level')
    press['time'] = press['Time (sec.)'] + gr_time
    
    # get the offset
    offset = get_manual_synchronization(ga['time'], ga['accel'], press['time'].values, press['Level'].values)

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
    f.savefig(Path(task_file).with_suffix(".png"))

    # save the data
    np.savez(task_file, time=task_time, accel=task_accel, fs=ga['fs'])
    
    return offset


def get_update_sync_dataframe(sync, data_path):
    tmp = pd.DataFrame(
        {
            'ga_file': np.sort(list(data_path.rglob("*.bin"))),
        }
    )

    sync = sync.merge(tmp, on=['ga_file'], how='outer')

    # make sure that processed is still a boolean column
    sync['processed'] = sync['processed'].replace(np.nan, False).astype(bool)

    return sync


def main():
    DATA_PATH = Path("../data/gaitrite/")
    
    # create a storage for the offsets
    # check if it exists. If it does exist, load it
    # if it does not exist, create the columns we need
    p = Path("geneactive_gaitrite_offsets.csv")
    if p.exists():
        sync_df = pd.read_csv(p)
        # make sure that the path-names are Path
        sync_df['ga_file'] = sync_df['ga_file'].apply(Path)
    else:
        sync_df = pd.DataFrame(
            columns=[
                "ga_file",
                "gr_sensor_file",
                "ga_task_file",
                "offset",
                "processed",
            ],
            dtype="str"
        ).astype({"offset": "float", "processed": "bool"})

    # update the dataframe if necessary
    sync_df = get_update_sync_dataframe(sync_df, DATA_PATH)
    
    # create the path for the GaitRite sensor file
    sync_df['gr_sensor_file'] = sync_df['ga_file'].apply(
        lambda x: x.with_name(
            f"{x.stem.split('_')[0].replace('-', '_')}_PKMAS_carpet_sensor.csv"
        )
    )
    # create the desired output for the GeneActive task file
    sync_df['ga_task_file'] = sync_df['ga_file'].apply(
        lambda x: x.with_name(
            f"{x.stem.split('_')[0].replace('-', '_')}_geneactive_gait_task.npz"
        )
    )

    for idx, row in sync_df.iterrows():
        if row.gr_sensor_file == '../data/gaitrite/DNK_01_014_PKMAS_carpet_sensor.csv':
            continue
        if row.processed:
            continue
        
        print(f"[{idx + 1}/{sync_df.shape[0]}] {row.ga_file}")

        # get the offset and generate the task file
        sync_df.loc[idx, 'offset'] = synchronize_geneactive_gaitrite(
            row.ga_file, row.gr_sensor_file, row.ga_task_file
        )
        # save that the file as processed
        sync_df.loc[idx, 'processed'] = True

    sync_df.to_csv(p, index=False)

if __name__ == "__main__":
    main()
