#!/bin/bash

while read -r INPUT; do
  echo $INPUT | jq
  echo OK
done