---
sidebar_label: Auto-Increment Column
title: Auto-Increment Column
sidebar_position: 6
---

# Auto-Increment Column

## Introduction

In Fluss, the auto increment column is a feature that automatically generates a unique numeric value, commonly used to create unique identifiers for each row of data.
Each time a new record is inserted, the auto increment column automatically assigns an incrementing value, eliminating the need for manually specifying the number.

One application scenario is of the auto-increment column is to accelerate the counting of distinct values in a high-cardinality column:
an auto-increment column can be used to represent the unique value column in a dictionary.
Compared to directly counting distinct STRING values, counting distinct integer values of the auto-increment column can sometimes improve the query speed by several times or even tens of times.

## Basic features

### Uniqueness

Fluss guarantees table-wide uniqueness for values it generates in the auto-increment column.

### Monotonicity
In order to improve the performance of allocating auto-incremented IDs, each table bucket on TabletServers caches some auto-incremented IDs locally.
In this situation, Fluss cannot guarantee that the values for the auto-increment column are strictly monotonic.
It can only be ensured that the values roughly increase in chronological order.

:::note
The number of auto-incremented IDs cached by the TabletServers is determined by the parameter table.auto-increment.cache-size, which defaults to 100,000.
:::

For example, create a table named test_tbl3 with 2 buckets and insert five rows of data as follows:

```sql
CREATE TABLE test_tbl
(
    id BIGINT,
    number BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'auto-increment.fields' = 'number',
    'bucket.num' = '2'
);

INSERT INTO test_tbl VALUES (1);
INSERT INTO test_tbl VALUES (2);
INSERT INTO test_tbl VALUES (3);
INSERT INTO test_tbl VALUES (4);
INSERT INTO test_tbl VALUES (5);
```

The auto-incremented IDs in the table test_tbl do not monotonically increase, because the two table buckets cache auto-incremented IDs, [1, 100000] and [100001, 200000], respectively.

```sql
SELECT * FROM test_tbl ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 100002 |
|    4 |      2 |
|    5 | 100003 |
+------+--------+
```

## Limits
- Auto-increment columns can only be used in primary key tables.
- Explicitly specifying values for the auto-increment column is not allowed. The value for an auto-increment column can only be implicitly assigned.
- A table can have only one auto-increment column.
- The auto-increment column must be of type BIGINT or INT.
- Fluss does not support specifying the starting value and step size for the auto-increment column.
