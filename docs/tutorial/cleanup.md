---
myst:
  html_meta:
    description: "Safely clean up your Charmed Apache Cassandra tutorial environment - remove the model, and optionally the Juju controller and LXD."
---

(tutorial-cleanup)=

# 7. Clean up your environment

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

In this tutorial we deployed Charmed Apache Cassandra, enabled TLS encryption, integrated a client
application, worked with data using `cqlsh`, and scaled the cluster in and out. This final section
shows how to tear everything down safely.

## Remove the tutorial model

```{caution}
Removing a Juju model destroys every application in it along with their storage. This deletes all the
data we created. Only do this when you are finished with the tutorial.
```

To remove Charmed Apache Cassandra and everything else in the `tutorial` model:

```shell
juju destroy-model tutorial --destroy-storage --no-prompt --force
```

This removes all applications in the model (Charmed Apache Cassandra, the Data Integrator, and the
self-signed certificates provider). Your Juju controller and any other models remain intact for future
use.

## (Optional) Remove the client snap

If you no longer need the `cqlsh` and `nodetool` clients locally, remove the `charmed-cassandra` snap.
This also removes the `ca.cert` and `cqlshrc` files we created under its configuration directory:

```shell
sudo snap remove charmed-cassandra --purge
```

## (Optional) Remove Juju and the cloud

If you do not need Juju anymore and want to free up additional resources, you can remove the Juju
controller and Juju itself.

```{caution}
Removing the Juju controller as shown below means you lose access to any other applications and models
hosted on it.
```

Check the list of controllers:

```shell
juju controllers
```

Remove the controller created in this tutorial:

```shell
juju destroy-controller overlord --destroy-all-models --destroy-storage
```

To remove Juju altogether:

```shell
sudo snap remove juju --purge
```

### Clean up LXD

If you also want to remove the LXD containers and free up all resources, list any remaining
containers:

```shell
lxc list
```

Delete unnecessary containers:

```shell
lxc delete <container-name> --force
```

If you want to uninstall LXD completely:

```shell
sudo snap remove lxd --purge
```

```{warning}
Only remove LXD if you are not using it for other purposes. LXD may be managing other containers or
virtual machines on your system.
```

## What's next?

In this tutorial we deployed Apache Cassandra, secured it with TLS, created users and data, and scaled
the cluster while keeping data safe. If you are looking for what to do next, you can:

- Read the [how-to guides](how-to-index) for deploying, scaling, securing, and monitoring a cluster
- Explore other Charmed offerings from
  [Canonical's Data Platform team](https://canonical.com/data)
- [Report](https://github.com/canonical/cassandra-operator/issues) any problems you encountered
- [Give us your feedback](https://matrix.to/#/#charmhub-data-platform:ubuntu.com)
- [Contribute to the code base](https://github.com/canonical/cassandra-operator)
