# The purpose of this script is to ingest all of the operational WOUDC Archive datasets and rename the generated ingest reports to avoid overwriting
# Inital setup and table initialization of WOUDC Data Registry is required to run this script
# Replace YYYY-MM-DD with today's date before proceeding

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzone_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzone_1.0_1_run_report

# woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_2.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
# mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzone_2.0_1.csv
# mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzone_2.0_1_run_report

# woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
# mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Broad-band_1.0_1.csv
# mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Broad-band_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_2.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Broad-band_2.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Broad-band_2.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Lidar_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Lidar_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Lidar_1.0_1_run_report

# woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Multi-band_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
# mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Multi-band_1.0_1.csv
# mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Multi-band_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/RocketSonde_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/RocketSonde_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/RocketSonde_1.0_1_run_report

# woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/OzoneSonde_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
# mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/OzoneSonde_1.0_1.csv
# mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/OzoneSonde_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/TotalOzoneObs_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzoneObs_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/TotalOzoneObs_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Spectral_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Spectral_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Spectral_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/Spectral_2.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/Spectral_2.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/Spectral_2.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_1.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/UmkehrN14_1.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/UmkehrN14_1.0_1_run_report

woudc-data-registry data ingest /apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_2.0_1 -y -r /apps/data/wdr-ingest/YYYY-MM-DD
mv /apps/data/wdr-ingest/YYYY-MM-DD/operator-report.csv /apps/data/wdr-ingest/YYYY-MM-DD/UmkehrN14_2.0_1.csv
mv /apps/data/wdr-ingest/YYYY-MM-DD/run_report /apps/data/wdr-ingest/YYYY-MM-DD/UmkehrN14_2.0_1_run_report

## Table generation process into registry for totalozone, uv-index and ozonesonde
woudc-data-registry product totalozone generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product uv-index generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product ozonesonde generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
