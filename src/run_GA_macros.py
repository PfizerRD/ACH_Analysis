import os
import sys

sys.path.insert(1, '/Users/bluesky/Documents/GitHub/gas_analytics/utility/')
from helpers import *

sys.path.insert(1, '/Users/bluesky/Documents/GitHub/dmtiDevices/geneActiv/')
from geneActiv_macros import *

def geneactiv_macros(macrotype, position):
    path = '/Volumes/Promise_Pegasus/ach/processed/geneactiv_epoch/'
    files = os.listdir(path)
    save_path = '/Volumes/Promise_Pegasus/ach/processed/{}/'.format(macrotype)
    #########

    #########

    for f in tqdm(files):
        # Isik does not want lumbar files
        if not position in f:
            continue
        # if f == ['DNK-01-012_left wrist_visit2_epoch60s.csv']:
        # # if f in ['DNK-01-012_left wrist_visit2_epoch60s.csv', 'DNK-01-013_left wrist_visit1_epoch60s.csv',
        # #          'GBR-01-002_left wrist_visit3_epoch60s.csv', 'GBR-01-004_left wrist_visit1_epoch60s.csv']:
        #     continue
        if os.path.exists(save_path):
            pass
        else:
            os.mkdir(save_path)
        ##for debugging
        [subject, devloc, visit, end] = f.split('_')
        # if subject == 'GBR-01-003':
        #     continue
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
    geneactiv_macros('Everyday_living_macro', 'wrist')
    #geneactiv_macros('Sleep_macro', 'wrist')