#!/usr/bin/env bash
#
# Description: This script uses curl to pull epa data for the region bounded in the BBOX variable and puts it into csv files in the location defined in epa_dir
#
# API documentation: https://docs.airnowapi.org/Data/docs

API_KEY="" # Valid airnow API key goes here

epa_dir="./epa" # where the files end up

parameters="OZONE,PM25,PM10,CO,NO2,SO2"
BBOX="-85.825195,38.324420,-83.023682,39.825413"
dataType="B" # A, C, or B. AQI, Concentration, or Both
format="text/csv" #  text/csv, application/json, application/xml, application/vnd.google-earth.kml (KML)
verbose="1" # gives us monitor names
monitorType="0" # permanent (0), mobile (1), both (2)
includerawconcentrations="1" # field that includes raw hourly measurements for Ozone, PM2.5, and PM10.


# Loops through years and months downloading data if available. If a csv for a time period already exists, it skips that time period and moves to the previous one. At the moment it downloads a months worth of data at time which may not be possible depending on the area specified in BBOX. Too big and you get an access error.
for i in {2024..2009}; do
  for j in {12..1}; do
    [ "$i" -ge "$(date +'%Y')" ] && [ "$j" -gt "$(date +'%M')" ] && continue
   
    startDate="${i}-$(printf "%02d" $j)-01T00"
    endMonth="$((j%12 + 1))"
    if [ "$j" -eq "12" ]; then
      endDate="$((i+1))-$(printf "%02d" $endMonth)-01T00"
    else
      endDate="${i}-$(printf "%02d" $endMonth)-01T00"
    fi

    filename="${startDate}_to_${endDate}.csv"
    echo "$filename"
    [ -e "$filename" ] && continue

    curl "https://www.airnowapi.org/aq/data/?startDate=${startDate}&endDate=${endDate}&parameters=${parameters}&BBOX=${BBOX}&dataType=${dataType}&format=${format}&verbose=${verbose}&monitorType=${monitorType}&includerawconcentrations=${includerawconcentrations}&API_KEY=${API_KEY}" > "${epa_dir}/${filename}"

    sleep 4m
  done
done
