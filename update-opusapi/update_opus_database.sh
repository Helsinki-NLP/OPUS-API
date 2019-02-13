#!/bin/bash

module load python-env/3.5.3

echo -e "\nReading file structure from /proj/nlpl/data/OPUS/ ..."

ls -Rpsl /proj/nlpl/data/OPUS > opusdata.txt

echo `grep -c ^ opusdata.txt` "lines read"

echo -e "\nCreating opusdata.db ..."

mv opusdata.db backups/$(date +"%Y%m%d%H%M%S")_opusdata.db

python3 readopusdata.py

echo -e "\nopusdata.db created"

#scp -i ~/.ssh/pouta opusdata.db cloud-user@vm1617.kaj.pouta.csc.fi:~/opusdata-new.db
#
#ssh -i ~/.ssh/pouta cloud-user@vm1617.kaj.pouta.csc.fi \
#"sudo mv /var/www/opusdata.db /var/www/opusdata_backup/$(date +"%Y%m%d%H%M%S")_opusdata.db
#sudo mv ~/opusdata-new.db /var/www/opusdata.db
#sudo chmod 774 /var/www/opusdata.db
#sudo systemctl restart apache2
#exit"
#
#echo -e "\nOPUS-API database updated on vm1617"
#
