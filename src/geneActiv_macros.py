import xlwings as xw
import pandas as pd
import os
import time
from tqdm import tqdm
import argparse
#from helpers import get_gaMacro_files_AWS, downloads

def geneactiv_everyday_living_macro_csv(filename, save_name):
    macro = 'GENEActiv_Everyday_Living_Overview.xlsm'

    wb = xw.Book(macro)
    time.sleep(5)
    #Import data
    task1 = "ImportDataFile"
    ExcelMacro = wb.macro(task1)
    app = xw.apps.active
    app.DisplayAlerts = False

    ExcelMacro.run(filename)

    # Generate Plots
    task2 = 'Data_analysis'
    genplots = wb.macro(task2)
    genplots.run()

    full_data = wb.sheets[1]

    full_data.book.save(save_name)
    app.quit()

    time.sleep(10)

def geneactiv_sleep_macro_csv(filename, save_name):
    macro = 'GENEActiv_Sleep_Macro.xlsm'

    wb = xw.Book(macro)
    time.sleep(5)
    # Import data
    task1 = "ImportDataFile"
    ExcelMacro = wb.macro(task1)
    app = xw.apps.active
    app.DisplayAlerts = False
    ExcelMacro.run(filename)

    # Generate Plots
    task2 = 'Data_analysis'
    genplots = wb.macro(task2)
    genplots.run()

    full_data = wb.sheets[1]
    full_data.book.save(save_name)
    time.sleep(15)
    app.quit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process GA Macros')
    parser.add_argument('--macrotype', type=str, default='Everyday_living_macro',
                        help='enter the type of macro (Everyday_living_macro or Sleep_macro)')
    parser.add_argument('--position', type=str, default='wrist',
                        help='position of GA for macro processing (wrist or back)')
    args = parser.parse_args()

    real_path = os.path.dirname(os.path.realpath(__file__)).split('/src')[0]
    #path = '../data/processed/epochs/'
    path = os.path.join(real_path, 'data/processed/epochs/')
    files = os.listdir(path)
    #save_path = '../data/processed/macros/'
    save_path = os.path.join(real_path, 'data/processed/macros/')
    if not os.path.exists(save_path):
        os.mkdir(save_path)
    else:
        pass

    #########
    macrotype = args.macrotype
    #########
    for f in sorted(files):
        # if f in ['GBR-01-004_left wrist_059540_2021-07-28 11-05-40_visit9_60s_epoch.csv',
        #     'DNK-01-012_left wrist_059552_2022-01-12 13-12-11_visit13_60s_epoch.csv']:
        #     continue
        if not args.position in f:
            continue
        #save_name = '{}_{}.csv'.format(f, macrotype)
        save_name = '{}_{}.xls'.format(f.split('.')[0], macrotype)
        if os.path.exists(save_path+save_name):
            print('{} already exists'.format(save_name))
            continue
        else:
            print('{} is being processed...'.format(f))
            if macrotype == 'Sleep_macro':
                geneactiv_sleep_macro_csv(path + f, save_path + save_name)
            if macrotype == 'Everyday_living_macro':
                geneactiv_everyday_living_macro_csv(path + f, save_path + save_name)
        time.sleep(3)

        print('Done!')