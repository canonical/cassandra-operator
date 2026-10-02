---
myst:
  html_meta:
    description: "Integrate a client application with Charmed Apache Cassandra using the Data Integrator charm to generate scoped credentials and a keyspace via a relation."
---

(tutorial-integrate-with-client-applications)=

# 4. Integrate with a client application

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

The recommended way to create and manage client users is through another charm: the
[Data Integrator Charm](https://charmhub.io/data-integrator). This lets us encode users directly in
the Juju model, and generate scoped credentials and a dedicated keyspace automatically using a
relation, without ever touching the admin `operator` user.

```{note}
Relations, which the Juju documentation also calls
[integrations](https://canonical.com/juju/docs/juju-cli/3.6/reference/relation/), let two charms
exchange information and interact with one another. Creating a relation between Charmed Apache
Cassandra and the Data Integrator automatically generates a username and password, creates a
keyspace, and assigns the relevant permissions on that keyspace. This is the simplest way to create
and manage client users in Charmed Apache Cassandra.
```

## The Data Integrator charm

The [Data Integrator charm](https://charmhub.io/data-integrator) is a bare-bones charm for the
central management of database and messaging users. It supports many data platforms (Apache
Cassandra, MongoDB, MySQL, PostgreSQL, Apache Kafka, OpenSearch, and more) with a consistent and
robust user experience.

Deploy the Data Integrator and tell it which keyspace to request through the `keyspace-name`
configuration option:

```shell
juju deploy data-integrator --config keyspace-name=tutorial_app
```

The charm starts in a `blocked` state with a message such as `Please relate the data-integrator with
the desired product`. This is expected, as it has nothing to integrate with yet.

To create the user and keyspace, integrate the Data Integrator with Charmed Apache Cassandra:

```shell
juju integrate data-integrator cassandra
```

Wait for the status to become `active`/`idle` using `watch juju status --color`.

<details> <summary> Output example</summary>

```text
Model     Controller  Cloud/Region         Version  SLA          Timestamp
tutorial  overlord    localhost/localhost  3.6.13   unsupported  13:02:41Z

App                       Version  Status  Scale  Charm                     Channel        Rev  Exposed  Message
cassandra                 5.0.5    active      3  cassandra                 5/edge          42  no
data-integrator                    active      1  data-integrator           latest/stable  362  no
self-signed-certificates           active      1  self-signed-certificates  1/edge         317  no

Unit                         Workload  Agent  Machine  Public address  Ports     Message
cassandra/0*                 active    idle   0        10.166.144.10   9042/tcp
cassandra/1                  active    idle   1        10.166.144.11   9042/tcp
cassandra/2                  active    idle   2        10.166.144.12   9042/tcp
data-integrator/0*           active    idle   3        10.166.144.13
self-signed-certificates/0*  active    idle   4        10.166.144.14
```

</details>

## Retrieve the credentials

Once the integration is set, retrieve the generated credentials with the `get-credentials` action:

```shell
juju run data-integrator/leader get-credentials
```

This outputs something like:

```yaml
cassandra:
  endpoints: 10.166.144.10,10.166.144.11,10.166.144.12
  password: 3xHk9fRpQ2vLmN8sT4wZ
  tls-ca: |-
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  username: user_6xRzpv4iGLMqVsok_relation_6
ok: "True"
```

Note the values for `username`, `password`, and `endpoints`. These credentials belong to a user that
is scoped to the `tutorial_app` keyspace only. This is exactly how a real client application would
obtain its credentials, and because the user is scoped, it cannot touch data in other keyspaces or
perform cluster-wide administration.

```{note}
Because TLS is enabled from the previous chapter, the credentials also include a `tls-ca` field (shown
truncated above) carrying the CA a client needs to trust the cluster.
```

## Rotate or remove the user

Credentials are managed entirely through the relation. To rotate them, remove and re-add the
integration:

```shell
juju remove-relation data-integrator cassandra
juju integrate data-integrator cassandra
```

Running `get-credentials` again returns a new username and password. To remove the user and its
access entirely, simply remove the relation:

```shell
juju remove-relation data-integrator cassandra
```

## What's next?

The Data Integrator shows how client applications receive scoped credentials. In the next section we
will switch to the admin `operator` user and use `cqlsh` directly to create our own keyspace and work
with data, giving us full control over the replication factor for the scaling exercises that follow.
