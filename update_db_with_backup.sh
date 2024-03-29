#!/bin/bash

echo "Starting at `date`"

cd /var/www/OPUS-API/update-opusapi
bu_name=backups/$(date +"%Y%m%d%H%M%S")_opusdata.db

echo -e "\nCreating backup db to $bu_name"

mv opusdata.db $bu_name

echo -e "\nCreating opusdata.db ..."

python3 readopusdata.py

echo -e "\nopusdata.db created"

cd /var/www/OPUS-API/
cp update-opusapi/opusdata.db .

echo "Finished at `date`"
