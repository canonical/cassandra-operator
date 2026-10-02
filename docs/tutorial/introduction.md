---
myst:
  html_meta:
    description: "Learn to deploy and operate Charmed Apache Cassandra - from bootstrapping a controller to enabling TLS, integrating clients, and scaling a cluster."
---

# Tutorial

The Charmed Apache Cassandra Operator delivers automated operations management from
[Day 0 to Day 2](https://codilime.com/blog/day-0-day-1-day-2-the-software-lifecycle-in-the-cloud-age/)
on the [Apache Cassandra](https://cassandra.apache.org/) distributed database. It is an open source,
end-to-end, production-ready data platform [on top of Juju](https://juju.is/).

As a first step, this tutorial shows you how to get Charmed Apache Cassandra up and running, but it
does not stop there. Through this tutorial you will learn a range of operations, from deploying a
cluster to enabling TLS encryption, integrating client applications, working with data using the
`cqlsh` command-line client, and scaling the cluster in and out while keeping your data safe.

This tutorial targets the **VM** charm `cassandra` running on a local LXD cloud.

```{note}
Charmed Apache Cassandra is under active development and is currently published on the `5/edge`
channel only. It is not yet recommended for production environments.
```

In this tutorial, we will walk through how to:

- Set up your local environment using LXD and Juju, and bootstrap a controller
- Deploy Charmed Apache Cassandra with only a few commands
- Enable TLS encryption for peer-to-peer and client-to-node communication
- Create a scoped client user automatically through a relation with the Data Integrator charm
- Create a keyspace and read and write data using the `cqlsh` client
- Scale the cluster out and in, and confirm that data is replicated and preserved
- Clean up your environment safely

While this tutorial intends to guide and teach you as you deploy Charmed Apache Cassandra, it will be
most beneficial if you already have familiarity with:

- Basic Unix shell commands
- General database concepts such as replication, keyspaces, and user management

## Minimum requirements

Before we start, make sure your machine meets the following requirements:

- Ubuntu 24.04 LTS (Noble) or later
- 8 GB of RAM
- 2 CPU cores
- At least 20 GB of available storage
- Access to the internet for downloading the required snaps and charms

```{note}
This tutorial uses the `testing` profile, which tunes Cassandra for a minimal resource footprint so
that a multi-node cluster fits comfortably on a single developer machine. Production deployments
should use the default `production` profile on dedicated hosts.
```
