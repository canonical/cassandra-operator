---
myst:
  html_meta:
    description: "Manage Charmed Apache Cassandra passwords - retrieve the operator password and rotate it using Juju secrets."
---

(how-to-manage-passwords)=

# How to manage passwords

This guide shows how to retrieve the current password for the default `operator` user and
how to rotate it.

Authentication is enabled by default in Charmed Apache Cassandra.
The charm automatically generates the initial password and stores it in a Juju secret.

## Prerequisites

* A running Charmed Apache Cassandra deployment — see [How to deploy Charmed Apache Cassandra](how-to-deploy).

## Retrieve the operator password

The password for the default `operator` user is stored in the application secret of the `cassandra-peers` relation:

```shell
juju show-secret --reveal "cassandra-peers.<application name>.app" --format json \
  | jq -r '.[].content.Data."operator-password"'
```

## Rotate the operator password

The charm supports password rotation for the default `operator` user by leveraging **Juju secrets**.

1. Check the current password:

   ```shell
   juju show-secret --reveal cassandra-peers.cassandra.app | grep operator
   # operator-password: a474ikLqA7KscI49zuH1O03bDTI42yJX
   ```

2. Create a new Juju secret with the updated password:

   ```shell
   juju add-secret mypass operator=abcd123456
   # secret:d2te3fe3rarc4b9fuj70
   ```

3. Grant Cassandra access to the new secret:

   ```shell
   juju grant-secret mypass cassandra
   ```

4. Update Cassandra to use the new secret:

   ```shell
   juju config cassandra system-users=secret:d2te3fe3rarc4b9fuj70
   ```

5. Verify that the password has been rotated:

   ```shell
   juju show-secret --reveal cassandra-peers.cassandra.app | grep operator
   # operator-password: abcd123456
   ```

```{note}
Once rotated, all clients must use the new password to connect.
```
