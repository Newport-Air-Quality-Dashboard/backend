#!/usr/bin/env bash

echo "sensor_index,name,latitude,longitude" > sensors.csv

for i in *.json; do
  jq -r '.sensor | "\(.sensor_index),\(.name),\(.latitude),\(.longitude)"' "${i}" >> sensors.csv 
done

