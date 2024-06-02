#!/usr/bin/env bash

# Description: This script is kind of a mess because of the point limit on pulling purpleair data. These sensors were manually selected from the purple air map and broken into different arrays to be able to selectively download the data from them on different api keys to try to avoid going over quota. It's not fancy, it's scrappy but it worked.

API_KEY="" # Valid PurpleAir API Key goes here

pa_dir="./purpleair"

average="60"
fields="humidity,temperature,pressure,pm2.5_atm,pm10.0_atm"

ReNewport=(184605 184723 184671 184721 184711 184237 184699 184725 184259 175247 184799)
Others1=(90915 40051 188555 103282 111898 120311 111898 206143 83761 3118 168231) 
Others2=(206653 184837 35399 102568 102632 35225 175389 184659 218157 130719)
Others3=(183777 183773 168209 160405 175263 183761 177011 205841 188039 175231)
Others4=(127915 127885 176557 175413 175257 184707 218157 184659 175389)
Others5=(175231)

for i in "${ReNewport[@]}"; do
    curl -X GET "https://api.purpleair.com/v1/sensors/${i}/history/csv?&average=${average}&fields=${fields}" -H "X-API-Key: ${API_KEY}" >> "${pa_dir}/${i}".csv
done
