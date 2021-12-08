import pandas
import numpy as np
from datetime import datetime, date, time, timedelta
import io
from collections import OrderedDict
from tqdm import tqdm


def twos_comp(val, bits):

    if( (val&(1<<(bits-1))) != 0 ):
        val = val - (1<<bits)
    return val

def parse_header(header):
    header_info = OrderedDict()
    header_info["start_datetime"] = header[21][11:]

    # print(header_info["start_datetime"])

    if header_info["start_datetime"] == "0000-00-00 00:00:00:000":
        header_info["start_datetime_python"] = datetime.strptime("0001-01-01", "%Y-%m-%d")
    else:
        header_info["start_datetime_python"] = datetime.strptime(header_info["start_datetime"], "%Y-%m-%d %H:%M:%S:%f")

    header_info["device_id"] = header[1].split(":")[1]
    header_info["firmware"] = header[4][24:]
    header_info["calibration_date"] = header[5][17:]

    header_info["x_gain"] = float(header[47].split(":")[1])
    header_info["x_offset"] = float(header[48].split(":")[1])
    header_info["y_gain"] = float(header[49].split(":")[1])
    header_info["y_offset"] = float(header[50].split(":")[1])
    header_info["z_gain"] = float(header[51].split(":")[1])
    header_info["z_offset"] = float(header[52].split(":")[1])

    header_info["number_pages"] = int(header[57].split(":")[1])
    # Turns out the frequency might be written European style (, instead of .)
    splitted = header[19].split(":")
    sans_hz = splitted[1].replace(" Hz", "")
    comma_safe = sans_hz.replace(",", ".")
    header_info["frequency"] = float(comma_safe)
    header_info["epoch"] = timedelta(seconds=1) / int(header_info["frequency"])

    return header_info

def read_bin(file):
    load_start = datetime.now()

    header = OrderedDict()
    channels = []
    #ts = Time_Series("")

    f = open(file, "rb")
    data = io.BytesIO(f.read())
    # print("File read in")

    # First 59 lines contain header information
    first_lines = [data.readline().strip().decode() for i in range(59)]
    # print(first_lines)
    header_info = parse_header(first_lines)#, "GeneActiv", "")

    # print(header_info)

    n = header_info["number_pages"]
    obs_num = 0
    ts_num = 0
    # Data format contains 300 XYZ values per page
    num = 300
    x_values = np.empty(int(num * n))
    y_values = np.empty(int(num * n))
    z_values = np.empty(int(num * n))

    # We will timestamp every 1 second of data to the nearest second
    # 300 / frequency = number of timestamps per page
    timestamps_per_page = int(num / header_info["frequency"])
    num_timestamps = (timestamps_per_page * header_info["number_pages"]) + 1

    ga_timestamps = np.empty(int(num_timestamps), dtype=type(header_info["start_datetime_python"]))
    ga_indices = np.empty(int(num_timestamps))

    # For each page
    for i in tqdm(range(n)):

        # xs,ys,zs,times = read_block(data, header_info)
        lines = [data.readline().strip().decode() for l in range(9)]
        page_time = datetime.strptime(lines[3][10:29],
                                      "%Y-%m-%d %H:%M:%S")  # + timedelta(microseconds=int(lines[3][30:])*1000)

        ga_timestamps[ts_num] = page_time
        ga_indices[ts_num] = obs_num

        for k in range(timestamps_per_page):
            ga_timestamps[ts_num + 1] = page_time + (timedelta(seconds=1) * (k + 1))
            ga_indices[ts_num + 1] = obs_num + (int(header_info["frequency"]) * (k + 1))
            ts_num += 1

        # For each 12 byte measurement in page (300 of them)
        for j in range(num):
            # time = page_time + (j * header_info["epoch"])

            block = data.read(12)

            x = int(block[0:3], 16)
            y = int(block[3:6], 16)
            z = int(block[6:9], 16)

            x, y, z = twos_comp(x, 12), twos_comp(y, 12), twos_comp(z, 12)
            # print(x,y,z)
            x_values[obs_num] = x
            y_values[obs_num] = y
            z_values[obs_num] = z
            # time_values[obs_num] = time
            obs_num += 1

        excess = data.read(2)

    # Timestamp the final observation
    ga_timestamps[-1] = page_time + (num * (timedelta(seconds=1) / header_info["frequency"]))
    ga_indices[-1] = obs_num
    ga_indices = ga_indices.astype(int)

    x_values = np.array([(x * 100.0 - header_info["x_offset"]) / header_info["x_gain"] for x in x_values])
    y_values = np.array([(y * 100.0 - header_info["y_offset"]) / header_info["y_gain"] for y in y_values])
    z_values = np.array([(z * 100.0 - header_info["z_offset"]) / header_info["z_gain"] for z in z_values])

    #x_channel = Channel("X")
    #y_channel = Channel("Y")
    #z_channel = Channel("Z")

    x_channel.set_contents(x_values, ga_timestamps, timestamp_policy="sparse")
    y_channel.set_contents(y_values, ga_timestamps, timestamp_policy="sparse")
    z_channel.set_contents(z_values, ga_timestamps, timestamp_policy="sparse")

    for c in [x_channel, y_channel, z_channel]:
        c.indices = ga_indices
        c.frequency = header_info["frequency"]

    channels = [x_channel, y_channel, z_channel]
    header = header_info

if __name__ == '__main__':
    f = '/Users/psaltd/Downloads/10031001_back_055673_2021-06-02 13-53-56.bin'
    read_bin(f)
