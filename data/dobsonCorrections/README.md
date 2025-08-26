# WOUDC Dobson Cross Section Correction

This document provides instructions and technical notes for applying the Dobson Cross Section correction to Totalozone data using the Dobson Instrument.

Consistent use of the Dobson cross section helps reduce biases between Dobson and Brewer instrument total ozone column (TOC) measurements, improving the accuracy of ozone monitoring programs.

## Setup

```bash
cd ./woudc-data-registry/data/dobsonCorrections

python3 step1_get_Teff_allstations_TEMIS.py
python3 step2_calc_abs_coef_TEMIS.py

cd ../..
```

## Usage

```bash
# Correct both AD and CD data from TotalOzone Dobson Data
woudc-data-registry correction dobson-correction /path/to/dir --mode [test|ops]

# Use --code to correct a specific code (AD or CD)
woudc-data-registry correction dobson-correction /path/to/dir --code [AD|CD] --mode [test|ops]

# Use --weeklyingest to output files in a folder structure similar to incoming folders
woudc-data-registry correction dobson-correction /path/to/dir --mode [test|ops] --weeklyingest
```
