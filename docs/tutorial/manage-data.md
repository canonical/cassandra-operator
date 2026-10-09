---
myst:
  html_meta:
    description: "Use the cqlsh client to create a keyspace, define a table, and read and write data in a Charmed Apache Cassandra cluster."
---

(tutorial-manage-data)=

# 5. Manage data with cqlsh

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

In this section we use the admin `operator` user and the `cqlsh` client to create a keyspace, define a
table, and write and read some data. We keep this `cqlsh` session handy for the scaling exercises in
the next chapter.

All the commands below connect over TLS using the `cqlshrc` file we created in the
[encryption chapter](tutorial-enable-encryption). Open a shell:

```shell
charmed-cassandra.cqlsh --ssl \
  --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

You should land at the `operator@cqlsh>` prompt.

## Create a keyspace

A keyspace is the top-level container for data in Cassandra, roughly analogous to a database in a
relational system. Crucially, the keyspace defines how data is replicated across the nodes in the
cluster through its *replication factor*.

Create a keyspace called `tutorial` with a replication factor of `3`. With three nodes in the cluster,
this means every node holds a full copy of the data:

```text
operator@cqlsh> CREATE KEYSPACE tutorial
            ... WITH replication = {
            ...   'class': 'SimpleStrategy',
            ...   'replication_factor': 3
            ... };
```

Confirm it was created:

```text
operator@cqlsh> DESCRIBE KEYSPACE tutorial;

CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;
```

```{note}
Setting the replication factor equal to the number of nodes is convenient for this tutorial because
it guarantees that each node holds every row. In production you typically keep the replication factor
fixed (for example, `3`) while the cluster grows, so that data is spread across a subset of nodes
rather than copied to all of them.
```

## Create a table

Switch into the new keyspace and create a simple table to store a list of members of the Fellowship:

```text
operator@cqlsh> USE tutorial;
operator@cqlsh:tutorial> CREATE TABLE members (
                     ...   id int PRIMARY KEY,
                     ...   name text,
                     ...   race text
                     ... );
```

## Add data

Insert a few rows:

```text
operator@cqlsh:tutorial> INSERT INTO members (id, name, race) VALUES (1, 'frodo', 'hobbit');
operator@cqlsh:tutorial> INSERT INTO members (id, name, race) VALUES (2, 'gandalf', 'maia');
operator@cqlsh:tutorial> INSERT INTO members (id, name, race) VALUES (3, 'aragorn', 'human');
```

## Read data

Read the rows back:

```text
operator@cqlsh:tutorial> SELECT * FROM members;

 id | name    | race
----+---------+--------
  1 |   frodo | hobbit
  2 | gandalf |   maia
  3 | aragorn |  human

(3 rows)
```

```{note}
`SELECT *` without a `WHERE` clause scans every partition across the whole cluster. It is fine on
this tiny tutorial table, but on a production deployment it is an expensive operation that should be
avoided. Real queries normally restrict the rows they read, for example with `WHERE id = 1`.
```

The data is now stored in the cluster and, thanks to the replication factor of `3`, replicated to
every node. We can demonstrate this with the `CONSISTENCY ALL` setting, which forces a read to
contact *every* replica and only succeed if they all respond. Since each node holds a full copy, the
read still returns all three rows:

```text
operator@cqlsh:tutorial> CONSISTENCY ALL;
Consistency level set to ALL.
operator@cqlsh:tutorial> SELECT * FROM members;

 id | name    | race
----+---------+--------
  1 |   frodo | hobbit
  2 | gandalf |   maia
  3 | aragorn |  human

(3 rows)
```

Reset the consistency level back to the default before moving on:

```text
operator@cqlsh:tutorial> CONSISTENCY QUORUM;
Consistency level set to QUORUM.
```

Leave the shell with `exit`.

## What's next?

In the next section we will scale the cluster out and confirm that our data is automatically
replicated to the new node, add more data, then scale back in and confirm that nothing is lost.
