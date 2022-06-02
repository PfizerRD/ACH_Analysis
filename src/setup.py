import os

os.system('pwd')

os.mkdir('../data/')
os.mkdir('../data/raw/')
os.mkdir('../data/CRF/')
os.mkdir('../data/processed/')
os.mkdir('../data/processed/epochs/')
os.mkdir('../data/processed/macros/')
os.mkdir('../data/processed/GENEActiv_Raw_Activity/')

print('installing SKDH')
os.system('conda install -c conda-forge scikit-digital-health')

print('Data directories have been setup')