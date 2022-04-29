import pandas as pd
from helpers import *
from datetime import *

def run_inventory():
    date = datetime.today().strftime('%Y%m%d')
    files = get_ACH_files_AWS()
    groupedInv = files.groupby(['subject', 'file_type', 'device_location']).count()['s3_obj']
    groupedInv = groupedInv.reset_index()
    groupedInv.columns = ['subject', 'file_type', 'device_location', 'file_count']
    piv_groupedInv = groupedInv.pivot(index='subject', columns=['file_type', 'device_location'], values = 'file_count')
    piv_groupedInv['total_files'] = piv_groupedInv.sum(axis = 1)
    piv_groupedInv = piv_groupedInv.reset_index()
    save_files(piv_groupedInv, 'C4181001_device_data_inventory')
    # groupedInv.to_csv('/Users/psaltd/Desktop/achondroplasia/QC/C4181001_device_data_inventory_{}.csv'.format(date),
    #                   index = False)

if __name__ == '__main__':
    run_inventory()



