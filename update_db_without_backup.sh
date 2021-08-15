#!/bin/bash

echo "Starting at `date`"

cd /var/www/OPUS-API/update-opusapi

echo -e "\nCreating opusdata.db ..."

python3 readopusdata.py

echo -e "\nopusdata.db created"

cd /var/www/OPUS-API/
cp update-opusapi/opusdata.db .

echo "Finished at `date`"
