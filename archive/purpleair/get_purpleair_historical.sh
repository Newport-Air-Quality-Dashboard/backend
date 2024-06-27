#!/usr/bin/env bash

# Description: This script is kind of a mess because of the point limit on pulling purpleair data. These sensors were manually selected from the purple air map and broken into different arrays to be able to selectively download the data from them on different api keys to try to avoid going over quota. It's not fancy, it's scrappy but it worked.

# Valid Purple Air API_KEY goes in config.sh
source ./config.sh

pa_dir="./cf_1"

average="60"
# fields="humidity,temperature,pressure,pm2.5_atm,pm10.0_atm"
# fields="pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,humidity_a,humidity_b,temperature_a,temperature_b,pressure_a,pressure_b"
# fields="pm2.5_10minute_a"
fields="pm2.5_cf_1_a,pm2.5_cf_1_b"

ReNewport=(184605 184723 184671 184721 184711 184237 184699 184725 184259 175247 184799)
Others=(90915 40051 188555 103282 120311 111898 83761
        206653 184837 35399 102568 102632 35225 184659 218157 130719 3118 168231 206143
        183777 183773 168209 160405 175263 183761 177011 205841
        127915 127885 
        176557 175413 175257 184707 175389 188039 175231)

mkdir -p "$pa_dir"
for i in "${Others[@]}"; do
    curl -X GET "https://api.purpleair.com/v1/sensors/${i}/history/csv?&average=${average}&fields=${fields}&start_timestamp=1717639200" -H "X-API-Key: ${API_KEY}" >> "${pa_dir}/${i}_pm2.5_10minute_ab".csv
    sleep 5s
done
