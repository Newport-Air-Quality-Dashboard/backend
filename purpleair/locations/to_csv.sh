#!/usr/bin/env bash

echo "sensor_index,name,latitude,longitude" > sensors.csv

for i in src/*.json; do
  jq -r '.sensor | "\(.sensor_index),\(.name),\(.latitude),\(.longitude)"' "${i}" >> locations.csv 
done

