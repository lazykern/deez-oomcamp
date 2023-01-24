#!/bin/bash

docker pull postgres

docker run --name pegasus --rm -e POSTGRES_PASSWORD=docker -d -p 5432:5432 postgres:latest

docker exec -it pegasus psql -U postgres -c "CREATE DATABASE taxi"
