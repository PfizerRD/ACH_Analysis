from helpers import *
from datetime import *

date = datetime.today().strftime('%Y%m%d')
files = get_ACH_files_AWS()
groupedInv = files.groupby(['subject', 'file_type', 'device_location']).count()['s3_obj']
groupedInv = groupedInv.reset_index()
groupedInv.columns = ['subject', 'file_type', 'device_location', 'file_count']
groupedInv.to_csv('/Users/psaltd/Desktop/achondroplasia/QC/C4181001_device_data_inventory_{}.csv'.format(date),
                  index = False)

