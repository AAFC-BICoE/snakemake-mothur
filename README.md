# snakemake-mothur

Example of a snakemake workflow implementation of the mothur MiSeq SOP.
Please see http://www.mothur.org/wiki/MiSeq_SOP

Data files are excluded from this repository because many files they are too large for GitHub.
They can be downloaded from: 
  http://www.mothur.org/w/images/d/d6/MiSeqSOPData.zip
  http://www.mothur.org/w/images/9/98/Silva.bacteria.zip
  http://www.mothur.org/w/images/5/59/Trainset9_032012.pds.zip
  
More up-to-date URLs may be available at the MiSeq SOP link above if these do not work.
  
All files should be placed in the data/ folder of this repository. 
Make sure the silva.bacteria and Trainset files are in data/ and not in a subdirectory or mothur will not be able to find them!

Here is an image of the current workflow implemented in the Snakefile. This image was generated using snakemake.

![muthur workflow](dag.final.png?raw=true "Mothur Workflow")
