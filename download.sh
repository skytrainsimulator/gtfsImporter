#!/bin/bash
set -o errexit

rm -rf work/
mkdir work
mkdir work/gtfs/
echo "Downloading GTFS..."
curl -o work/google_transit.zip https://gtfs-static.translink.ca/gtfs/google_transit.zip
echo "Downloaded!"
echo "Unzipping..."
unzip work/google_transit.zip -d work/gtfs/
echo "Done!"
