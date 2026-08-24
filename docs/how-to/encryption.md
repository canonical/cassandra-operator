---
myst:
  html_meta:
    description: "Enable TLS encryption for Charmed Apache Cassandra - secure peer-to-peer and client-to-node communication and rotate certificates."
---

(how-to-tls-encryption)=

# How to enable TLS encryption

This guide includes step-by-step instructions on how to enable TLS encryption for peer-to-peer
and client-to-node communication in a Charmed Apache Cassandra cluster.

This charm implements the **Requirer** side of the [tls-certificates](https://charmhub.io/integrations/tls-certificates) relation. Therefore, any charm implementing the **Provider** side could be used.

All units within a cluster share the same CA certificate file, but each unit has a distinct private key.

## Prerequisites

For this guide, we will need an active Charmed Apache Cassandra application.
Follow the [How to deploy Charmed Apache Cassandra](how-to-deploy) guide to set up the environment.

## Enable TLS encryption for peer-to-peer communication

To enable peer-to-peer TLS encryption, you should first deploy a TLS certificates Provider charm.
For this guide, we will be using the `self-signed-certificates` charm.

```{warning}
Using self-signed certificates is not recommended for production systems.
Instead follow your organisations best-practices for managing TLS certificates.
Please refer to [this post](https://charmhub.io/topics/security-with-x-509-certificates)
for an overview of the TLS certificates Providers charms and some guidance on how to choose
the right charm for your use case.
```

To deploy the `self-signed-certificates` application:

```shell
juju deploy self-signed-certificates --channel=edge --config ca-common-name="Test CA"
```

To enable peer-to-peer TLS encryption with Charmed Apache Cassandra, integrate the Charmed Apache Cassandra application to the `tls-certificates` provider application via the `peer-certificates` relation interface:

```shell
juju integrate cassandra:peer-certificates self-signed-certificates
```

While certificates are being issued and distributed, units show waiting and maintenance statuses such as `waiting for internal TLS setup` and `rotating peer tls`. Once the process completes, units return to `active/idle`.

## Enable TLS encryption for client-to-node communication

To enable client-to-node TLS encryption, integrate the Charmed Apache Cassandra application to the `tls-certificates` provider application via the `client-certificates` relation interface:

```shell
juju integrate cassandra:client-certificates self-signed-certificates
```

While certificates are being issued and distributed, units show waiting and maintenance statuses such as `waiting for TLS setup` and `rotating client tls`. Once the process completes, units return to `active/idle`.

## Connect to the cluster

Authentication is enabled by default.
To retrieve the password for the default `operator` user:

```shell
juju show-secret --reveal "cassandra-peers.<application name>.app" --format json \
  | jq -r '.[].content.Data."operator-password"'
```

### Verify client TLS

First, attempt to connect **without specifying TLS certificates**:

```shell
cqlsh <unit-ip> -u operator -p "<password>"
```

This should result in an error:

```text
Warning: Using a password on the command line interface can be insecure.
Recommendation: use the credentials file to securely provide the password.

Connection error: ('Unable to connect to any servers',
  {'10.166.144.168:9042': ConnectionResetError(104, 'Connection reset by peer')})
```

And in the Apache Cassandra logs you will see:

```text
WARN  [epollEventLoopGroup-5-6] ... SSLException in client networking with peer /10.166.144.168:42604
io.netty.handler.ssl.NotSslRecordException: not an SSL/TLS record
```

This confirms that Apache Cassandra requires a secure TLS connection.

### Retrieving the root CA

Fetch the root CA from the self-signed certificate operator:

```shell
juju run self-signed-certificates/0 get-ca-certificate --format yaml | yq '.self-signed-certificates/0.results.ca-certificate' > ca.cert
```

The CA needs to be used to verify the certificate provided by the Apache Cassandra servers in the TLS handshake.

### Connecting using `cqlsh`

First of all, install the `charmed-cassandra` snap in the local environment

```shell
sudo snap install charmed-cassandra --edge
```

The `charmed-cassandra` snap bundles also the `cqlsh` client to be used to connect to the Apache Cassandra endpoint. Since the snap is strictly confined, the `ca.cert` file needs to be copied to a location that is readable by the snap processes, e.g. `/var/snap/charmed-cassandra/current/etc/cassandra/` where configuration files are generally stored.  

In the same location, also create a `cqlshrc` configuration file for `cqlsh`:

```ini
[authentication]
username = operator
password = <password>

[connection]
hostname = <unit-ip>
port = 9042

[ssl]
certfile = /var/snap/charmed-cassandra/current/etc/cassandra/ca.cert
validate = true
```

Connect to Apache Cassandra with:

```shell
cqlsh --ssl --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

The `cqlsh` client should connect and show the prompt where CQL queries can be run.

## (Optional) Rotate certificates

When the TLS certificates Provider charm renews the certificates (for example, when they are close to expiry, or when the CA is rotated), the Cassandra charm picks up the new certificates automatically.

During the rotation, units show maintenance statuses such as `rotating peer tls` or `rotating client tls`, and return to `active/idle` once the new certificates are in place. No manual action is required.

## Disable TLS encryption

To disable TLS encryption, remove the relations with the `tls-certificates` provider application:

```shell
juju remove-relation cassandra:peer-certificates self-signed-certificates
juju remove-relation cassandra:client-certificates self-signed-certificates
```

```{note}
Removing the relations causes a rolling restart of the units as they switch back to unencrypted communication.
```
