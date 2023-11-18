#!/bin/bash

bu_name=backups/$(date +"%Y%m%d%H%M%S")_opusdata.db

echo -e "\nCreating backup db to $bu_name"

mv opusdata.db $bu_name

echo -e "\nCreating opusdata.db ..."

opus_get --update --database opusdata.db --suppress_prompts # --warnings

echo -e "\nopusdata.db created"

