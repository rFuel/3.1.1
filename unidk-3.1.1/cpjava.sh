#!/bin/bash

echo " "
sudo systemctl stop clamav-freshclam
sudo freshclam
sudo systemctl start clamav-freshclam
echo "Scanning for Malware...."
clamscan -r ~/rFuel/Deploy/3.1.1/java/*
echo " "
echo "---------------------------------------------------------------------------------------------------------------------"
echo " "
echo "Copying..."
cp ~/rFuel/Deploy/3.1.1/java/* ~/s3rfuel/core/3.1.1/staging/
echo "Done."
