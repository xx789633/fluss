
## Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./sql-client
```

**Note**:
To simplify this guide, three temporary tables have been pre-created with `faker` connector to generate data.
You can view their schemas by running the following commands:

```sql title="Flink SQL"
SHOW CREATE TABLE source_customer;
```

```sql title="Flink SQL"
SHOW CREATE TABLE source_order;
```

```sql title="Flink SQL"
SHOW CREATE TABLE source_nation;
```


## Create Fluss Tables
### Create Fluss Catalog
Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

:::info
By default, catalog configurations are not persisted across Flink SQL client sessions.
For further information how to store catalog configurations, see [Flink's Catalog Store](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/#catalog-store).
:::

### Create Tables
Running the following SQL to create Fluss tables to be used in this guide:
```sql  title="Flink SQL"
CREATE TABLE fluss_customer (
    `cust_key` INT NOT NULL,
    `name` STRING,
    `phone` STRING,
    `nation_key` INT NOT NULL,
    `acctbal` DECIMAL(15, 2),
    `mktsegment` STRING,
    PRIMARY KEY (`cust_key`) NOT ENFORCED
);
```

```sql  title="Flink SQL"
CREATE TABLE fluss_nation (
  `nation_key` INT NOT NULL,
  `name`       STRING,
   PRIMARY KEY (`nation_key`) NOT ENFORCED
);
```
