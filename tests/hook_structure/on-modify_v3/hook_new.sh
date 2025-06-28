#!/usr/bin/env sh

read -r base
read -r new

echo "$new" | jq --compact-output '.description |= (. + " (extended by on-modify hook)")'
