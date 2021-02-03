# The purpose of this script is to ingest all of the operational WOUDC Archive datasets and rename the generated ingest reports to avoid overwriting
# Inital setup and table initialization of WOUDC Data Registry is required to run this script

woudc-data-registry data ingest /path/to/TotalOzone_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/TotalOzone_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/TotalOzone_1.0_1_run_report

woudc-data-registry data ingest /path/to/TotalOzone_2.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/TotalOzone_2.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/TotalOzone_2.0_1_run_report

woudc-data-registry data ingest /path/to/Broad-band_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Broad-band_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Broad-band_1.0_1_run_report

woudc-data-registry data ingest /path/to/Broad-band_2.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Broad-band_2.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Broad-band_2.0_1_run_report

woudc-data-registry data ingest /path/to/Lidar_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Lidar_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Lidar_1.0_1_run_report

woudc-data-registry data ingest /path/to/Multi-band_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Multi-band_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Multi-band_1.0_1_run_report

woudc-data-registry data ingest /path/to/RocketSonde_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/RocketSonde_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/RocketSonde_1.0_1_run_report

woudc-data-registry data ingest /path/to/OzoneSonde_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/OzoneSonde_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/OzoneSonde_1.0_1_run_report

woudc-data-registry data ingest /path/to/TotalOzoneObs_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/TotalOzoneObs_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/TotalOzoneObs_1.0_1_run_report

woudc-data-registry data ingest /path/to/Spectral_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Spectral_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Spectral_1.0_1_run_report

woudc-data-registry data ingest /path/to/Spectral_2.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/Spectral_2.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/Spectral_2.0_1_run_report

woudc-data-registry data ingest /path/to/UmkehrN14_1.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/UmkehrN14_1.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/UmkehrN14_1.0_1_run_report

woudc-data-registry data ingest /path/to/UmkehrN14_2.0_1 -y -r /path/to/operator/report
mv /path/to/operator/report/operator-report.csv /path/to/operator/report/UmkehrN14_2.0_1.csv
mv /path/to/operator/report/run_report /path/to/operator/report/UmkehrN14_2.0_1_run_report
