# pgfdb

## Docker

Building Docker image:

```sh
docker build -t pgfdb .
```

Start Docker container:

```sh
# Workaround for OrbStack mounting fdb.cluster as a directory rather than file
cat /usr/local/etc/foundationdb/fdb.cluster > fdb.cluster
docker run --net=host -v $(pwd)/fdb.cluster:/etc/foundationdb/fdb.cluster -e POSTGRES_PASSWORD=postgres pgfdb
```
