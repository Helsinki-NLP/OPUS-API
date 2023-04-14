
SHELL := bash

update-db-with-backup:
	echo "Starting at `date`"
	echo -e "\nCreating backup db to $bu_name"
	mv update-opusapi/opusdata.db update-opusapi/backups/$(date +"%Y%m%d%H%M%S")_opusdata.db
	echo -e "\nCreating opusdata.db ..."
	cd update-opusapi && opus_get -u -db opusdata.db -q
	echo -e "\nopusdata.db created"
	cp update-opusapi/opusdata.db .
	echo "Finished at `date`"
