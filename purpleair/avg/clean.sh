#!/usr/bin/env bash

sed '/time_stamp,/d' src/*.csv | sort -u > avg_combined_sorted_deduped.csv


