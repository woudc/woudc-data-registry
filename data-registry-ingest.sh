#1/bin/bash

# The purpose of this script is to ingest all of the operational WOUDC Archive datasets and rename the generated ingest reports to avoid overwriting
# Inital setup and table initialization of WOUDC Data Registry is required to run this script

TODAY=`date +"%Y-%m-%d"`

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/TotalOzone_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/TotalOzone_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_2.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/TotalOzone_2.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/TotalOzone_2.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Broad-band_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Broad-band_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_2.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Broad-band_2.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Broad-band_2.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Lidar_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Lidar_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Lidar_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Multi-band_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Multi-band_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Multi-band_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/RocketSonde_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/RocketSonde_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/RocketSonde_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/OzoneSonde_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/OzoneSonde_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/OzoneSonde_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzoneObs_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/TotalOzoneObs_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/TotalOzoneObs_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Spectral_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Spectral_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Spectral_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Spectral_2.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/Spectral_2.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/Spectral_2.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_1.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/UmkehrN14_1.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/UmkehrN14_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_2.0_1 -y -r /apps/data/wdr-ingest/$TODAY
mv /apps/data/wdr-ingest/$TODAY/operator-report.csv /apps/data/wdr-ingest/$TODAY/UmkehrN14_2.0_1.csv
mv /apps/data/wdr-ingest/$TODAY/run_report /apps/data/wdr-ingest/$TODAY/UmkehrN14_2.0_1_run_report

## Table generation process into registry for totalozone, uv-index and ozonesonde
woudc-data-registry product totalozone generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product uv-index generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product ozonesonde generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
