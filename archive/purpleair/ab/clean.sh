#!/usr/bin/env bash

sed '/time_stamp,/d' src/*.csv | sort -u > ab_combined_sorted_deduped.csv


