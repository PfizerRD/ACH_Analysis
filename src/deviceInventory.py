import pandas as pd
from helpers import *
from datetime import *

def run_inventory():
    '''
    Create a inventory table for all the study data, grouped by individual subjects and device types.

    :return: None - table is saved as a .csv file
    '''
    # Get current date
    date = datetime.today().strftime('%Y%m%d')
    # grab files from AWS as a dataframe
    files = get_ACH_files_AWS()
    # Group the inventory list by subject filetype and device loc
    groupedInv = files.groupby(['subject', 'file_type', 'device_location']).count()['s3_obj']
    groupedInv = groupedInv.reset_index() #reset index

    # set new columns for grouped inventory dataframe
    groupedInv.columns = ['subject', 'file_type', 'device_location', 'file_count']

    #pivot table in order to get file type and location as columns with subject as index. File count are the values.
    piv_groupedInv = groupedInv.pivot(index='subject', columns=['file_type', 'device_location'], values = 'file_count')

    # new column for total files
    piv_groupedInv['total_files'] = piv_groupedInv.sum(axis = 1)
    piv_groupedInv = piv_groupedInv.reset_index()

    # save the files
    save_files(piv_groupedInv, 'C4181001_device_data_inventory')


if __name__ == '__main__':
    run_inventory()



