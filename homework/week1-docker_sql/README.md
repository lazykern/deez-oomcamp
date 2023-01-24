# HW 1

## File descriptions

- `docker_setup.sh`: Setup postgres docker container

```
sh docker_setup.sh
```

- `create_table.py`: Create table using psycopg2 as a client. Queries can be found in `create_table.sql`

```
python create_table.py
```

- `data_ingest.sh`: Ingest data using COPY statement

```
sh data_ingest.sh
```

- `python.Dockerfile`: Dockerfile for python:3.9 image
- `queries.sql`: Queries I used to answer the questions
