---
myst:
  html_meta:
    description: "Deploy Charmed Apache Cassandra on VMs - set up a Juju controller and model, deploy production or testing clusters, and connect with cqlsh."
---

(how-to-deploy)=

# How to deploy Charmed Apache Cassandra

This guide provides deployment instructions for the **IAAS/VM** operator using the Juju CLI.

To deploy a Charmed Apache Cassandra cluster, it is necessary to:

1. Set up a Juju controller
2. Set up a Juju model
3. Deploy Charmed Apache Cassandra
4. Connect to the cluster

If you already have a Juju controller and a Juju model, you can skip the associated steps.

## Juju controller setup

Make sure you have a Juju controller accessible from your local environment using the
[Juju client snap](https://snapcraft.io/juju).

List available controllers:

```shell
juju list-controllers
```

Make sure that the controller's back-end cloud is **not** Kubernetes-based.
To retrieve the cloud list:

```shell
juju list-clouds
```

To switch to another controller if needed:

```shell
juju switch <controller>
```

If there are no suitable controllers, create a new one:

```shell
juju bootstrap <cloud> <controller>
```

where `<cloud>` is the cloud to deploy the controller to (for example, `localhost` for a LXD
cloud). For more information on how to set up a new cloud, see the
[How to manage clouds](https://canonical.com/juju/docs/juju-cli/latest/howto/manage-clouds/index.html)
guide in Juju documentation.

## Juju model setup

Switch to an existing Juju model:

```shell
juju switch <model-name>
```

Or create a new Juju model:

```shell
juju add-model <model>
```

## Deploy Charmed Apache Cassandra

```{note}
Charmed Apache Cassandra is still under active development and is only available on the `5/edge`
channel. Due to the lack of a stable release it is not yet recommended for production environments.
```

To deploy Charmed Apache Cassandra:

```shell
juju deploy cassandra -n <units> --config profile=<profile> --channel 5/edge
```

The charm supports two profiles:

* `production` (default) — tunes Cassandra for maximum performance and allocates half of the
  available RAM on the host
* `testing` — minimises resource requirements for very-small, non-production test and staging
  clusters

```{warning}
The `production` profile typically needs **at least** 32 GB of RAM.
```

To maintain high-availability of the data, `3+` units are recommended.

To change the profile on a running deployment:

```shell
juju config cassandra profile=testing
```

Monitor the status of the deployment:

```shell
watch juju status
```

During bootstrap, units briefly show maintenance and waiting statuses such as
`installing Cassandra`, `waiting for Cassandra to start`, and `waiting for cluster to start`.
The deployment should be complete once all the units show `active` and `idle` status.

## Test by connecting

Authentication is enabled by default. To retrieve the password for the default `operator` user:

```shell
juju show-secret --reveal "cassandra-peers.<application name>.app" --format json \
  | jq -r '.[].content.Data."operator-password"'
```

Once you have the password, connect to the cluster using `cqlsh`:

```shell
cqlsh <unit-ip> -u operator -p "<password>"
```

<details>

<summary> Output example</summary>

```text
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
```

</details>
