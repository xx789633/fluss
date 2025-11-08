The data for the `datalake_enriched_orders` table is stored in Fluss (for real-time data) and {props.name} (for historical data).

When querying the `datalake_enriched_orders` table, Fluss uses a union operation that combines data from both Fluss and {props.name} to provide a complete result set -- combines **real-time** and **historical** data.

If you wish to query only the data stored in {props.name}—offering high-performance access without the overhead of unioning data—you can use the `datalake_enriched_orders$lake` table by appending the `$lake` suffix.
This approach also enables all the optimizations and features of a Flink {props.name} table source, including system table such as `datalake_enriched_orders$lake$snapshots`.


```sql  title="Flink SQL"
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
```


```sql  title="Flink SQL"
-- query snapshots
SELECT snapshot_id, operation FROM datalake_enriched_orders$lake$snapshots;
```

**Sample Output:**
```shell
+---------------------+-----------+
|         snapshot_id | operation |
+---------------------+-----------+
| 7792523713868625335 |    append |
| 7960217942125627573 |    append |
+---------------------+-----------+
```
**Note:** Make sure to wait for the configured `datalake.freshness` (~30s) to complete before querying the snapshots, otherwise the result will be empty.

Run the following SQL to do analytics on {props.name} data:
```sql  title="Flink SQL"
-- to sum prices of all orders
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders$lake;
```
**Sample Output:**
```shell
+-----------+
| sum_price |
+-----------+
| 432880.93 |
+-----------+
```

To achieve results with sub-second data freshness, you can query the table directly, which seamlessly unifies data from both Fluss and {props.name}:

```sql  title="Flink SQL"
-- to sum prices of all orders (combining fluss and lake data)
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders;
```

**Sample Output:**
```shell
+-----------+
| sum_price |
+-----------+
| 558660.03 |
+-----------+
```

You can execute the real-time analytics query multiple times, and the results will vary with each run as new data is continuously written to Fluss in real-time.

Finally, you can use the following command to view the files stored in {props.name}: