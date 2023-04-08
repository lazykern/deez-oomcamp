#!/bin/bash

cd ./kafka
docker compose down

cd ../spark
docker compose down

cd ../
docker compose down
