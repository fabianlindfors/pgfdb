<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/elephants-dark.png">
  <img alt="pgfdb" src="assets/elephants.png" width="150" align="right">
</picture>

# pgfdb

pgfdb is an experimental project that turns Postgres into a distributed, fault-tolerant and horizontally scalable database using [FoundationDB](https://www.foundationdb.org). pgfdb isn't just Postgres-compatible, it _is_ Postgres, meaning it can support all your favorite Postgres features and any standard client can connect to it. If you aren't familiar with FoundationDB, check out [the docs on its features](https://apple.github.io/foundationdb/features.html).

In short, pgfdb aims to upgrade Postgres with:

- [Distributed and strictly serializable transactions](https://apple.github.io/foundationdb/features.html#acid-transactions) across all your data, achieved by replacing Postgres' transaction system with FoundationDB's
- [Horizontal scalability](https://apple.github.io/foundationdb/scalability.html) with automatic sharding. Simply add more nodes and your data will be rebalanced automatically. No need to manually partition your tables.
- [Automatic replication](https://apple.github.io/foundationdb/features.html#replicated-storage) for durability and read performance. No need to configure read replicas or route requests in your client.
- [Fault tolerance](https://apple.github.io/foundationdb/fault-tolerance.html) that handles node failures and availability zone outages without any intervention. No need to manually configure replication and failovers.
- Simplified operations. No more VACUUM or complicated version upgrades. Native backups to object storage with point-in-time restore.
- [High performance](https://apple.github.io/foundationdb/performance.html) that has been battle tested by companies like [Apple](https://arxiv.org/abs/1901.04452), [Snowflake](https://www.snowflake.com/en/blog/how-foundationdb-powers-snowflake-metadata-forward/) and [Deepseek](https://github.com/deepseek-ai/3FS).

Keep reading if you want to try it out yourself! Also feel free to [reach out](mailto:fabian@flapplabs.se) if you are interested in chatting about pgfdb.

Given that pgfdb is highly experimental there are many limitations at the moment, some of which are detailed under [Limitations](#limitations).

## Try it out

### Installing

First of all, we need to get a FoundationDB cluster running. The simplest way is to follow the getting started guide for either [Mac](https://apple.github.io/foundationdb/getting-started-mac.html) or [Linux](https://apple.github.io/foundationdb/getting-started-linux.html). Even if you have a production FoundationDB cluster running already, do not use it with pgfdb. The project is still experimental and should not be used with production data.

Once we have FoundationDB running, we can start up pgfdb and connect to it. The project is currently only distributed as a Docker image. Run the following to start the container:

```sh
docker run --name pgfdb --net=host -v /etc/foundationdb/fdb.cluster:/etc/foundationdb/fdb.cluster -e POSTGRES_PASSWORD=postgres fabianlindfors/pgfdb
# => ...
# => ... database system is ready to accept connections
```

You should see some output from Postgres starting up and we're now ready to use it! Open up your favorite Postgres client and connect to `localhost:5432` with username, password and database name all set to `postgres`.

One simple way to do this is to use `psql` on the container we just started:

```sh
docker exec -it pgfb psql -h localhost -U postgres
# => psql (17.4 (Debian 17.4-1.pgdg120+2))
# => Type "help" for help.
#
# => postgres=#
```

### Using

Let's start with a simple call to the built in `pgfdb_is_healthy()` function, which returns true if the connection to FoundationDB is working:

```sql
SELECT fdb_is_healthy();
--
--  fdb_is_healthy
-- ----------------
--  t
```

Let's create a new table and insert some data. PRIMARY KEY is currently not supported so we'll create an index manually:

```sql
CREATE TABLE users(
    id UUID DEFAULT gen_random_uuid(),
    name TEXT
);
CREATE INDEX id_idx ON users USING pgfdb_idx(id);
INSERT INTO users(name) VALUES
    ('George Costanza'),
    ('Elaine Benes'),
    ('Cosmo Kramer');
```

The table and index data will live entirely in FoundationDB, and we can of course read it back as well:

```sql
SELECT * FROM users;
--                   id                  |      name
-- --------------------------------------+-----------------
--  62bdec0f-82e7-4a0b-b551-43825a4db83f | George Costanza
--  96f3d0f0-a586-4f99-b2a7-af28d0fae2d3 | Elaine Benes
--  f9796391-f90b-4596-9178-fb2b0aada832 | Cosmo Kramer
```

An efficient index read will also work. Note that the query planner has not yet been integrated with so we must force Postgres to use our index:

```sql
SET enable_seqscan=0;
SELECT * FROM users WHERE id = '62bdec0f-82e7-4a0b-b551-43825a4db83f';
--                   id                  |      name
-- --------------------------------------+-----------------
--  62bdec0f-82e7-4a0b-b551-43825a4db83f | George Costanza
```

UPDATE work just as expected:

```sql
UPDATE users SET name = 'Art Vandelay' WHERE id = '62bdec0f-82e7-4a0b-b551-43825a4db83f';
SELECT * FROM users WHERE id = '62bdec0f-82e7-4a0b-b551-43825a4db83f';
--                   id                  |     name
-- --------------------------------------+--------------
--  62bdec0f-82e7-4a0b-b551-43825a4db83f | Art Vandelay
```

Thanks to FoundationDB we can run serializable, fully ACID, distributed transactions with the same syntax as we are used to in Postgres:

```sql
BEGIN;
DELETE FROM users WHERE id = '62bdec0f-82e7-4a0b-b551-43825a4db83f';
SELECT * FROM users;
--                   id                  |      name
-- --------------------------------------+-----------------
--  96f3d0f0-a586-4f99-b2a7-af28d0fae2d3 | Elaine Benes
--  f9796391-f90b-4596-9178-fb2b0aada832 | Cosmo Kramer
ROLLBACK;

SELECT * FROM users;
--                   id                  |      name
-- --------------------------------------+-----------------
--  62bdec0f-82e7-4a0b-b551-43825a4db83f | George Costanza
--  96f3d0f0-a586-4f99-b2a7-af28d0fae2d3 | Elaine Benes
--  f9796391-f90b-4596-9178-fb2b0aada832 | Cosmo Kramer
```

## Limitations

- No primary keys
- No query planner
- Performance not great
- 5s transaction limt

## License

pgfdb is currently closed source and only distributed as a Docker image for evaluation. Depending on how the project goes, it might be open sourced in the future although that remains to be seen. If you have any thoughts on this, [get in touch](mailto:fabian@flapplabs.se)!
