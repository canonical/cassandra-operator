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

Bootstrap a [lxd controller](https://juju.is/docs/olm/lxd#heading--create-a-controller) and create a new Juju model:

```shell
juju add-model sample-model
```


To deploy a single unit of Cassandra, run the following command:

```shell
juju deploy cassandra_ubuntu@24.04-amd64.charm cassandra 
```

Config option `--config profile=testing` can be used for testing purpuses. This will restrict Cassandra node to use minimum memory.

Apache Cassandra is typically deployed as a cluster to provide horizontal scalability and fault tolerance. In Juju, each Cassandra node is represented by a unit.

To start a Cassandra cluster with multiple nodes, set the desired number of units using the `-n` option:

```shell
juju deploy cassandra_ubuntu@24.04-amd64.charm cassandra -n <number_of_units>
```

The seed-node is always will be the leader unit in the cluster.


### Node management

#### Add node

To add more nodes one can use the `juju add-unit` functionality i.e.

```shell
juju add-unit cassandra -n <number_of_units_to_add>
```

The implementation of `add-unit` allows the operator to add more than one unit, at a time. But unit initialization will happen only at one in a time.

#### Remove node

Nodes removal is in progress.


### Password rotation

TODO

## Integrations (Relations)

Supported [integrations](https://juju.is/docs/olm/relations):

#### `tls-certificates` interface

TODO

#### `metrics` interface:

TODO

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/cassandra-operator/blob/main/CONTRIBUTING.md) for developer guidance. 

Also, if you truly enjoy working on open-source projects like this one, check out the [career options](https://canonical.com/careers/all) we have at [Canonical](https://canonical.com/). 

## License

Charmed Apache Cassandra is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/cassandra-operator/blob/main/LICENSE) for more information.
