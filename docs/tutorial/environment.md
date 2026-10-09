---
myst:
  html_meta:
    description: "Set up your environment for Charmed Apache Cassandra - install LXD and Juju, bootstrap a controller, and create a model."
---

(tutorial-environment)=

# 1. Set up the environment

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

For this tutorial, we need to set up the environment with two main components, plus some extra
command-line tooling:

- [LXD](https://github.com/canonical/lxd) - a system container and virtual machine manager that
  provides the local cloud our cluster runs on
- [Juju](https://github.com/juju/juju) - lets us deploy and manage Charmed Apache Cassandra and
  related applications
- [jq](https://github.com/jqlang/jq) - a command-line JSON processor used to parse command output

## Prepare the cloud

The fastest, simplest way to get started with Charmed Apache Cassandra is to set up a local LXD
cloud. Each Cassandra node runs inside an LXD container managed by Juju. While this tutorial covers
the basics of LXD, you can [learn more about LXD here](https://canonical.com/lxd/docs/stable-5.21/).

LXD comes pre-installed on Ubuntu 24.04 LTS. Verify that LXD is installed by entering the command
`which lxd`. This will output `/snap/bin/lxd` or, on some systems, `/usr/sbin/lxd`.

Although LXD is already installed, we need to run `lxd init` to perform post-installation tasks. For
this tutorial the default parameters are preferred, and the network bridge should be set to have no
IPv6 addresses since Juju does not support IPv6 addresses with LXD:

```shell
lxd init --auto
lxc network set lxdbr0 ipv6.address none
```

You can list all LXD containers by entering the command `lxc list`. At this point of the tutorial
none should exist, so you will only see this output:

```text
+------+-------+------+------+------+-----------+
| NAME | STATE | IPV4 | IPV6 | TYPE | SNAPSHOTS |
+------+-------+------+------+------+-----------+
```

## Install and prepare Juju

[Juju](https://juju.is/) is an Operator Lifecycle Manager (OLM) for clouds, bare metal, LXD and
Kubernetes. We will use it to deploy and manage Charmed Apache Cassandra. As with LXD, Juju is
installed from a snap package:

```shell
sudo snap install juju --channel 3.6/stable
```

Install `jq`, a JSON processor used to parse Juju output in later steps:

```shell
sudo snap install jq
```

## Bootstrap a Juju controller

Juju already has built-in knowledge of LXD and how it works, so there is no additional cloud setup
needed. A Juju controller will be created, which will in turn manage the operations of Charmed Apache
Cassandra.

All we need to do is bootstrap a Juju controller named `overlord` onto the `localhost` (LXD) cloud.
This bootstrapping process can take several minutes depending on the resources available on your
machine:

```shell
juju bootstrap localhost overlord
```

The Juju controller runs inside its own LXD container. To verify this, check the list of containers:

```shell
lxc list
```

<details> <summary> Output example</summary>

```text
+---------------+---------+-----------------------+------+-----------+-----------+
|     NAME      |  STATE  |         IPV4          | IPV6 |   TYPE    | SNAPSHOTS |
+---------------+---------+-----------------------+------+-----------+-----------+
| juju-<id>     | RUNNING | 10.166.144.1 (eth0)   |      | CONTAINER | 0         |
+---------------+---------+-----------------------+------+-----------+-----------+
```

where `<id>` is a unique combination of numbers and letters such as `9d7e4e-0`.

</details>

## Create a model

A controller can manage multiple models. A model hosts applications such as Charmed Apache Cassandra.
Create a model named `tutorial`:

```shell
juju add-model tutorial
```

Check the status of the model you just created:

```shell
juju status
```

<details> <summary> Output example</summary>

```text
Model     Controller  Cloud/Region         Version  SLA          Timestamp
tutorial  overlord    localhost/localhost  3.6.29   unsupported  12:10:54Z

Model "admin/tutorial" is empty.
```

</details>

With the environment ready, we can move on to deploying Charmed Apache Cassandra.
