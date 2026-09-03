---
myst:
  html_meta:
    description: "Manage Charmed Apache Cassandra units - scale clusters by adding or removing units, verify the topology with nodetool, and recover lost nodes."
---

(how-to-manage-units)=

# How to manage units

This guide includes step-by-step instructions on how to scale a Charmed Apache Cassandra cluster
by adding or removing units, and how to verify the cluster topology.

For general Juju unit management process, see the [Juju documentation](https://canonical.com/juju/docs/juju-cli/3.6/howto/manage-units/).

The charm automates most of the node management operations, ensuring that the cluster topology remains consistent and data is replicated correctly.

## Scaling

### Add units

To scale-out the Charmed Apache Cassandra application, add more units:

```shell
juju add-unit cassandra -n <number_of_units_to_add>
```

See the `juju add-unit` [command reference](https://canonical.com/juju/docs/juju-cli/latest/reference/juju-cli/list-of-juju-cli-commands/add-unit/).

The implementation of `add-unit` allows adding multiple units at once, but unit initialization occurs one at a time: each new unit bootstraps and joins the cluster before the next one starts. Expect the total time to grow linearly with the number of added units.

Monitor the progress with:

```shell
juju status
```

New units show `waiting for cluster to start` while they join, and become `active/idle` once they are full members of the cluster.

### Remove units

To decrease the number of Apache Cassandra nodes, remove some existing units from the Charmed Apache Cassandra application:

```shell
juju remove-unit <unit-name>
```

See the `juju remove-unit` [command reference](https://canonical.com/juju/docs/juju-cli/latest/reference/juju-cli/list-of-juju-cli-commands/remove-unit/).

```{note}
Only one unit can be removed at a time. Support for removing multiple units simultaneously is not yet available.
```

```{caution}
Before removing a unit, check the replication factor of your keyspaces. If removing the unit brings the number of nodes below the replication factor of a keyspace, that keyspace may become unavailable or lose data.
```

A Cassandra cluster is also called a *ring*: a set of peer nodes with no master, where every node can serve the same functionality as any other. For more details, see the
[Cassandra architecture documentation](https://cassandra.apache.org/doc/latest/cassandra/architecture/architecture.html).

During removal, the charm decommissions the node: data stored on the removed unit is streamed to the remaining nodes before the node leaves the ring, so no data is lost.

## Admin utility scripts

Apache Cassandra ships with the `nodetool` utility to do various administrative tasks such as checking the cluster status, draining a node, or repairing keyspaces.

The most important commands are exposed via the [Charmed Apache Cassandra snap](https://snapcraft.io/charmed-cassandra), accessible via `charmed-cassandra.nodetool <command>`.

To run the commands, you need to provide authentication information. The `nodetool` credentials are stored on every unit and can be used by running the commands within the unit itself, e.g. via the `juju ssh` command:

```shell
juju ssh cassandra/0 sudo snap run charmed-cassandra.nodetool \
  -u charmed-operator -pw "$(juju show-secret --reveal cassandra-peers.cassandra.app --format json | jq -r '.[].content.Data."nodetool-password"')" status
```

```{note}
The `nodetool` username is always `charmed-operator` — a separate internal user, distinct from the `operator` CQL user used to connect with `cqlsh`. The password is stored in the `nodetool-password` field of the `cassandra-peers` application secret.
```

### Cluster status

To check the state of the Cassandra ring, run `nodetool status` on any unit:

```shell
juju ssh cassandra/0 sudo snap run charmed-cassandra.nodetool \
  -u charmed-operator -pw "$(juju show-secret --reveal cassandra-peers.cassandra.app --format json | jq -r '.[].content.Data."nodetool-password"')" status
```

The output lists every node in the cluster with its state (`UN` means up and normal), address, and load:

```text
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)  Host ID                               Rack
UN  10.166.144.10  1.2 MiB    16      100.0%            1f3f0e1e-...                          rack1
UN  10.166.144.11  1.1 MiB    16      100.0%            9a2b7c4d-...                          rack1
UN  10.166.144.12  1.3 MiB    16      100.0%            5e8d1a2c-...                          rack1
```

### Recover from a lost node

If a unit's machine is lost or permanently fails, remove the unit with `juju remove-unit` as above. The charm also automatically removes nodes that are down and unknown to the cluster, and repairs the `system_auth` keyspace to keep authentication data consistent across the remaining nodes.
