# pgfdb

## Docker

Building Docker image:

```sh
docker build -t pgfdb .
```

Start Docker container:

```sh
docker run --name pgfdb -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgfdb
```
