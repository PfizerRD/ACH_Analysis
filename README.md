# ACH_Analysis
 C4181001 Data Science

 This package contains scripts for processing the C4181001 Study data. 

## Adjust Permissions
If you are using a Windows, skip this step. If you are using a Mac, you will need to adjust your permissions to allow
terminal to access excel in order to use the macro script.

    Go to system preferences > security and privacy > automation, and make sure terminal (or your IDE)
    has microsoft excel selected.

# Required packages:
1. Setup required folders

> cd ~/ACH_Analysis
> python src/setup.py

2. Install required packages

> pip install -r requirements.txt

FOR QC run:

1. Make sure you have authorized a new session using 'samlutil'
> $ samlutil

2. Run device inventory. This will give us an understanding of how many
files we have per subject. Results will be in the src/results folder

> python deviceInventory.py

3. Next run GENEActiv QC. This will create a file which contains all the relevant
information we want to check in the GENEActiv files for QC.
   
> python GENEActiv_QC.py

4. Run PKMAS QC - TBD

5. Convert GENEActiv files to Epoch + Wearability graphs

> python src/geneActiv_epoch_generator.py

6. Run macros on epoch files generated in step 5

> python src/geneActiv_macros.py

