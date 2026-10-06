---
myst:
  html_meta:
    description: "Scale a Charmed Apache Cassandra cluster out and in with Juju, and verify with cqlsh that data is replicated to new nodes and preserved on removal."
---

(tutorial-scale)=

# 6. Scale your cluster

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

One of the main reasons to run Apache Cassandra is its ability to scale horizontally while keeping
data available. In this section we scale the cluster out, confirm the data we wrote earlier has been
replicated to the new node, add more data, then scale back in and confirm nothing was lost.

A Cassandra cluster is also called a *ring*: a peer-to-peer set of nodes where every node is equal.
When nodes are added or removed, the charm drives the operation while Apache Cassandra itself
automatically redistributes (streams) the data, so the ring stays balanced and the replication
factor is honoured.

## Inspect the ring

The `nodetool` utility reports the state of the ring. It uses a separate internal user,
`charmed-operator`, whose password is in the `nodetool-password` field of the `cassandra-peers`
secret. Run it on any unit through `juju ssh`:

```shell
juju ssh cassandra/0 sudo snap run charmed-cassandra.nodetool \
  -u charmed-operator -pw "$(juju show-secret --reveal cassandra-peers.cassandra.app --format json | jq -r '.[].content.Data."nodetool-password"')" \
  status
```

<details> <summary> Output example</summary>

```text
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)  Host ID                               Rack
UN  10.166.144.10  120.5 KiB  16      100.0%            1f3f0e1e-...                          rack1
UN  10.166.144.11  118.9 KiB  16      100.0%            9a2b7c4d-...                          rack1
UN  10.166.144.12  121.2 KiB  16      100.0%            5e8d1a2c-...                          rack1
```

</details>

`UN` means the node is **U**p and in the **N**ormal state. With three nodes and a replication factor
of `3`, each node owns `100.0%` of the data.

## Scale out

Add a fourth node to the cluster:

```shell
juju add-unit cassandra -n 1
```

Monitor the progress with `watch juju status --color`. The new unit shows `waiting for cluster to
start` while it bootstraps and joins the ring, then becomes `active`/`idle`. As it joins, Cassandra
streams data to it automatically.

<details> <summary> Output example</summary>

```text
App        Version  Status  Scale  Charm      Channel  Rev  Exposed  Message
cassandra  5.0.5    active      4  cassandra  5/edge    42  no

Unit          Workload  Agent  Machine  Public address  Ports     Message
cassandra/0*  active    idle   0        10.166.144.10   9042/tcp
cassandra/1   active    idle   1        10.166.144.11   9042/tcp
cassandra/2   active    idle   2        10.166.144.12   9042/tcp
cassandra/3   active    idle   5        10.166.144.15   9042/tcp
```

</details>

Run `nodetool status` again, and you will see the fourth node in the ring, carrying its own share of
the load, which confirms that data was streamed to it:

```shell
juju ssh cassandra/0 sudo snap run charmed-cassandra.nodetool \
  -u charmed-operator -pw "$(juju show-secret --reveal cassandra-peers.cassandra.app --format json | jq -r '.[].content.Data."nodetool-password"')" \
  status
```

<details> <summary> Output example</summary>

```text
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)  Host ID                               Rack
UN  10.166.144.10  135.1 KiB  16      73.8%             1f3f0e1e-...                          rack1
UN  10.166.144.11  132.7 KiB  16      76.2%             9a2b7c4d-...                          rack1
UN  10.166.144.12  134.4 KiB  16      74.9%             5e8d1a2c-...                          rack1
UN  10.166.144.15  98.3 KiB   16      75.1%             b7c3f9a1-...                          rack1
```

</details>

With four nodes and a replication factor of `3`, each node now owns roughly `75%` of the data instead
of `100%`.

## Verify data was replicated

To prove the new node serves our data, connect `cqlsh` **directly to the new node's address** by
passing it as a positional argument (this overrides the `hostname` in `cqlshrc`). Capture the new
unit's address the same way as before:

```shell
NEW_CASSANDRA_IP=$(juju show-unit cassandra/3 --format json | jq -r '."cassandra/3"."public-address"')
```

```shell
charmed-cassandra.cqlsh --ssl "$NEW_CASSANDRA_IP" \
  --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

Read the rows we wrote earlier:

```text
operator@cqlsh> SELECT * FROM tutorial.members;

 id | name    | race
----+---------+--------
  1 |   frodo | hobbit
  2 | gandalf |   maia
  3 | aragorn |  human

(3 rows)
```

The data we inserted on a three-node cluster is now available from the brand-new fourth node.

## Add more data

While still connected (to any node), insert another row:

```text
operator@cqlsh> INSERT INTO tutorial.members (id, name, race) VALUES (4, 'legolas', 'elf');
operator@cqlsh> SELECT * FROM tutorial.members;

 id | name    | race
----+---------+--------
  1 |   frodo | hobbit
  2 | gandalf |   maia
  3 | aragorn |  human
  4 | legolas |    elf

(4 rows)
```

Leave the shell with `exit`.

## Scale in

Now remove the fourth node. The charm *decommissions* it, streaming its data to the remaining nodes
before it leaves the ring, so no data is lost:

```shell
juju remove-unit cassandra/3
```

```{caution}
Before removing a unit, check the replication factor of your keyspaces. If removing a unit would bring
the number of nodes below a keyspace's replication factor, that keyspace may lose data or become
unavailable. Here we go from four nodes back to three, which still satisfies our replication factor of
`3`.
```

```{note}
Only one unit can be removed at a time. During removal, units show statuses related to
decommissioning while data is streamed off the departing node.
```

Wait for the cluster to return to three `active`/`idle` units, then check the ring again. The fourth
node is gone and the three remaining nodes again own `100.0%` of the data:

```shell
juju ssh cassandra/0 sudo snap run charmed-cassandra.nodetool \
  -u charmed-operator -pw "$(juju show-secret --reveal cassandra-peers.cassandra.app --format json | jq -r '.[].content.Data."nodetool-password"')" \
  status
```

## Verify data was preserved

Connect to one of the remaining nodes and read the table:

```shell
charmed-cassandra.cqlsh --ssl "$CASSANDRA_IP" \
  --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

```text
operator@cqlsh> SELECT * FROM tutorial.members;

 id | name    | race
----+---------+--------
  1 |   frodo | hobbit
  2 | gandalf |   maia
  3 | aragorn |  human
  4 | legolas |    elf

(4 rows)
```

All four rows are still there, including the one we added while the cluster had four nodes. Scaling in
preserved the data. Leave the shell with `exit`.

## What's next?

You have now scaled the cluster out and in while keeping your data safe. In the final section we will
clean up the environment.
