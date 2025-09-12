# Charmed Apache Cassandra

This repository contains a charmed operator for deploying [Apache Cassandra](https://cassandra.apache.org/_/cassandra-basics.html) on virtual machines via the [Juju orchestration engine](https://juju.is/).

## Overview

This operator provides an [Apache Cassandra](https://cassandra.apache.org/) database with support for both single-node and multi-node deployments. It automates the deployment and lifecycle management of Cassandra clusters, ensuring smooth operation from single-node setups to multi-node.

Built as a Juju charm, the operator manages Cassandra on Ubuntu and provides out-of-the-box integrations that make it suitable for enterprise workloads:

* **Scalable deployments**: easily bootstrap single-node or multi-node Cassandra clusters.
* **Upgrades made safe**: charm refreshes and Cassandra software upgrades with minimal disruption.
* **Observability by default**: seamless integration with the Canonical Observability Stack (COS) for metrics, logs, and dashboards.
* **Secure communications**: built-in TLS support for encrypted traffic between nodes.
* **Authentication**: automatic generation of initial system credentials and secure password rotation with Juju secrets.

## Basic usage

### Deployment

Build a Cassandra charm:

```shell
charmcraft pack
```

Bootstrap a [LXD controller](https://juju.is/docs/olm/lxd#heading--create-a-controller) and create a new Juju model:

```shell
juju add-model sample-model
```

To deploy a single unit of Cassandra:

```shell
juju deploy cassandra_ubuntu@24.04-amd64.charm cassandra
```

The config option `--config profile=testing` can be used for testing purposes. This restricts a Cassandra node to minimum memory usage.

Apache Cassandra is typically deployed as a cluster to provide horizontal scalability and fault tolerance. In Juju, each Cassandra node is represented by a unit.

To start a Cassandra cluster with multiple nodes, set the desired number of units using the `-n` option:

```shell
juju deploy cassandra_ubuntu@24.04-amd64.charm cassandra -n <number_of_units>
```

The seed node will always be the leader unit in the cluster.

### Node management

#### Adding nodes

To add more nodes, use the `juju add-unit` command:

```shell
juju add-unit cassandra -n <number_of_units_to_add>
```

The implementation of `add-unit` allows adding multiple units at once, but unit initialization will occur one at a time.

#### Removing nodes

Node removal support is in progress.

### Connecting

Authentication is enabled by default.
To retrieve the password for the default `operator` user:

```shell
juju show-secret --reveal "cassandra-peers.<application name>.app" --format json \
  | jq -r '.[].content.Data."operator-password"'
  ```

Once you have the password, connect to the cluster using `cqlsh`:

```shell
cqlsh <unit-ip> -u operator -p "<password>"
```

Now you can read/write data to Cassandra:

```shell
Connected to cassandra at 10.166.144.207:9042
[cqlsh 6.2.0 | Cassandra 5.0.5 | CQL spec 3.4.7 | Native protocol v5]
Use HELP for help.
operator@cqlsh> CREATE KEYSPACE hello
   ... WITH replication = {
   ...   'class': 'SimpleStrategy',
   ...   'replication_factor': 1
   ... };
operator@cqlsh> DESCRIBE KEYSPACE hello;

CREATE KEYSPACE hello WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
};
```

## Password rotation

The Cassandra charm supports password rotation for the default `operator` user by leveraging **Juju secrets**.

1. **Check the current password**:

```shell
juju show-secret --reveal cassandra-peers.cassandra.app | grep operator
# operator-password: a474ikLqA7KscI49zuH1O03bDTI42yJX
```

2. **Create a new Juju secret with the updated password**:

```shell
juju add-secret mypass operator=abcd123456
# secret:d2te3fe3rarc4b9fuj70
```

3. **Grant Cassandra access to the new secret**:

```shell
juju grant-secret mypass cassandra
```

4. **Update Cassandra to use the new secret**:

```shell
juju config cassandra system-users=secret:d2te3fe3rarc4b9fuj70
```

5. **Verify that the password has been rotated**:

```shell
juju show-secret --reveal cassandra-peers.cassandra.app | grep operator
# operator-password: abcd123456
```

> **Note**: Once rotated, all clients must use the new password to connect.

## Integrations (Relations)

Supported [integrations](https://juju.is/docs/olm/relations):

### `tls-certificates` interface

See the [encryption tutorial](docs/how-to/encryption.md) for detailed instructions on adding the `tls-certificates` relation and managing encryption.

### `metrics` interface

See the [monitoring tutorial](docs/how-to/monitoring.md) for detailed instructions on adding the `metrics` relation and integrating with the `cos` charm.

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/cassandra-operator/blob/main/CONTRIBUTING.md) for developer guidance.

Also, if you truly enjoy working on open-source projects like this one, check out the [career options](https://canonical.com/careers/all) we have at [Canonical](https://canonical.com/).

## License

Charmed Apache Cassandra is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/cassandra-operator/blob/main/LICENSE) for more information.
