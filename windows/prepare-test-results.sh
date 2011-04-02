#!/bin/sh

while read LINE; do

    START=$(echo $LINE | awk '{print $2}')
    time java -jar rainycloud_2.8.1-assembly-1.1.jar  -e "$LINE" --hcaf data/hcaf.csv.gz --hspen data/hspen.csv.gz --hspec test/out-"$START".csv.gz

done <huge-ranges.txt