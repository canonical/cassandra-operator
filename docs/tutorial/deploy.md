---
myst:
  html_meta:
    description: "Deploy a Charmed Apache Cassandra cluster with Juju, retrieve the operator password, and connect to the cluster with cqlsh."
---

(tutorial-deploy)=

# 2. Deploy Apache Cassandra

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

To deploy Charmed Apache Cassandra, all you need to do is run the following command, which fetches
[Apache Cassandra](https://charmhub.io/cassandra) from [Charmhub](https://charmhub.io/) and deploys
it to your model.

For this tutorial we will deploy a cluster of three nodes using the `testing` profile, which keeps
the resource footprint small. Three or more nodes are recommended to keep data highly available:

```shell
juju deploy cassandra -n 3 --config profile=testing --channel 5/edge
```

Juju will now fetch Charmed Apache Cassandra and begin deploying it to your LXD cloud. You can track
the progress by running:

```shell
watch juju status --color
```

During bootstrap, units pass through several transient maintenance and waiting statuses, such as
`installing Cassandra`, `waiting for internal TLS setup`, `waiting for cluster to start`, `waiting for
Cassandra to start`, and `repairing system_auth keyspace`. This process can take several minutes
depending on the resources available on your machine.

Wait until all units show `active`/`idle` status:

<details> <summary> Output example</summary>

```text
Model     Controller  Cloud/Region         Version  SLA          Timestamp
tutorial  overlord    localhost/localhost  3.6.13   unsupported  12:31:09Z

App        Version  Status  Scale  Charm      Channel  Rev  Exposed  Message
cassandra  5.0.5    active      3  cassandra  5/edge    42  no

Unit          Workload  Agent  Machine  Public address  Ports     Message
cassandra/0*  active    idle   0        10.166.144.10   9042/tcp
cassandra/1   active    idle   1        10.166.144.11   9042/tcp
cassandra/2   active    idle   2        10.166.144.12   9042/tcp

Machine  State    Address        Inst id        Base          AZ  Message
0        started  10.166.144.10  juju-9d7e4e-0  ubuntu@24.04      Running
1        started  10.166.144.11  juju-9d7e4e-1  ubuntu@24.04      Running
2        started  10.166.144.12  juju-9d7e4e-2  ubuntu@24.04      Running
```

</details>

To exit the `watch` screen, press `Ctrl+C`.

Each node exposes the Apache Cassandra native transport protocol on port `9042`, which is the port
`cqlsh` and client applications use to connect.

## Access the cluster

Authentication is enabled by default. The charm automatically generates a password for the built-in
`operator` user and stores it in a Juju secret.

All sensitive configuration data used by Charmed Apache Cassandra, such as passwords and TLS
certificates, is stored in Juju secrets. See the
[Juju secrets documentation](https://canonical.com/juju/docs/juju-cli/3.6/reference/secret/) for more
information.

The `operator` password lives in the application secret of the `cassandra-peers` relation. The secret
label follows the pattern `cassandra-peers.<application-name>.app`, so for our `cassandra` application
it is `cassandra-peers.cassandra.app`. Retrieve the password with:

```shell
juju show-secret --reveal cassandra-peers.cassandra.app --format json \
  | jq -r '.[].content.Data."operator-password"'
```

This prints the current `operator` password, for example:

```text
a474ikLqA7KscI49zuH1O03bDTI42yJX
```

Make note of this value, we will use it to connect in the next steps.

## Connect with cqlsh

`cqlsh` is the official command-line client for Apache Cassandra. The
[`charmed-cassandra`](https://snapcraft.io/charmed-cassandra) snap bundles `cqlsh` (as well as the
`nodetool` admin utility we use later), so install it locally:

```shell
sudo snap install charmed-cassandra --edge
```

Retrieve the address of a unit (here `cassandra/0`) with `juju show-unit`, parsing it out with `jq`
and storing it in a variable:

```shell
CASSANDRA_IP=$(juju show-unit cassandra/0 --format json | jq -r '."cassandra/0"."public-address"')
```

Connect as the `operator` user, passing in that address:

```shell
charmed-cassandra.cqlsh "$CASSANDRA_IP" -u operator -p "<operator-password>"
```

<details> <summary> Output example</summary>

```text
[cqlsh 6.1.0 | Cassandra 5.0.5 | CQL spec 3.4.7 | Native protocol v5]
Use HELP for help.
operator@cqlsh>
```

</details>

You now have an interactive CQL shell connected to the cluster. To confirm the cluster is healthy,
list the keyspaces that ship with Cassandra:

```text
operator@cqlsh> DESCRIBE KEYSPACES;

system_auth  system_schema  system        system_distributed
system_views system_traces  system_virtual_schema
```

Leave the shell for now by typing `exit`:

```text
operator@cqlsh> exit
```

## What's next?

So far we have connected over an unencrypted channel. In a real deployment, traffic between nodes and
between clients and nodes should be encrypted. In the next section we will enable TLS encryption
across the cluster.
