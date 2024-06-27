#!/usr/bin/env bash
filename="avg_combined_sorted_deduped.csv"

echo "time_stamp,sensor_index,humidity,temperature,pressure,pm2.5_atm,pm10.0_atm" > "$filename"
sed '/time_stamp,/d' src/*.csv | sort -u >> "$filename"


