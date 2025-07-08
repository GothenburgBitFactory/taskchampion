#!/usr/bin/env sh

read -r added_task
echo "$added_task" | jq --compact-output '.description |= (. + " (extended by on-add hook)")'
