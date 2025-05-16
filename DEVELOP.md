# pgfdb

## Docker

Build Docker image and pushing to ghcr.io:

```sh
docker build --platform linux/amd64,linux/arm64 -t ghcr.io/fabianlindfors/pgfdb:X.X.X -t ghcr.io/fabianlindfors/pgfdb:latest --annotation "org.opencontainers.image.source=https://github.com/fabianlindfors/pgfdb" --push .
```

Start Docker container:

```sh
# Workaround for OrbStack mounting fdb.cluster as a directory rather than file
cat /usr/local/etc/foundationdb/fdb.cluster > fdb.cluster
docker run --net=host -v $(pwd)/fdb.cluster:/etc/foundationdb/fdb.cluster -e POSTGRES_PASSWORD=postgres ghcr.io/fabianlindfors/pgfdb:latest
```
