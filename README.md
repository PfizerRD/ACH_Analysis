# ACH_Analysis
 C4181001 Data Science

 This package contains scripts for processing the C4181001 Study data. 
 

TODO In package (4/29/22):
1. Write Script to check filenameing conventions - use RegEx
2. Script to make visuals for GA QC
3. Make PKMAS QC
4. Verify all start times match


FOR QC run:

1. Make sure you have authorized a new session using 'samlutil'
> $ samlutil

2. Run device inventory. This will give us an understanding of how many
files we have per subject.

> python deviceInventory.py 

results will be in the src/results folder

3. Next run GENEActiv QC. This will create a file which contains all the relevant
information we want to check in the GENEActiv files for QC.
   
> python GENEActiv_QC.py

4. Run PKMAS QC - TBD

