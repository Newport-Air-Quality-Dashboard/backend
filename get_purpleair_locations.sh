#!/usr/bin/env bash 
#
# Description: A script that queries the PurpleAir api to get the latitude and longitude of sensors whose IDs are known for geostatistical analysis.

pa_dir="./purpleair"
out_dir="./csv"

API_KEY="" # Valid PurpleAir api key

fields="name,latitude,longitude"

Sensors=(
        184605 184723 184671 184721 184711 184237 184699 184725 184259 175247 184799 # ReNewport Sensors
        90915 40051 188555 103282 111898 120311 111898 206143 83761 3118 168231 206653 184837 35399 102568 102632 35225 175389 184659 218157 130719 183777 183773 168209 160405 175263 183761 177011 205841 188039 175231 127915 127885 176557 175413 175257 184707 218157 184659 175389 175231
)

mkdir -p "${pa_dir}/locations/"

for i in "${Sensors[@]}"; do
    curl -X GET "https://api.purpleair.com/v1/sensors/${i}?fields=${fields}" -H "X-API-Key: ${API_KEY}" > "${pa_dir}/locations/$i".json
done

