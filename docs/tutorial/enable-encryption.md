---
myst:
  html_meta:
    description: "Enable TLS encryption for Charmed Apache Cassandra using self-signed certificates, and reconnect securely with cqlsh over SSL."
---

(tutorial-enable-encryption)=

# 3. Enable encryption

This is a part of the [Charmed Apache Cassandra Tutorial](index.md).

[TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) encrypts data exchanged between
applications, securing it as it travels over the network. Typically, enabling TLS within a highly
available database, and between the database and its clients, requires domain-specific knowledge and
a high level of expertise. Fortunately, that knowledge has been encoded into Charmed Apache
Cassandra, so configuring TLS requires minimal effort on your end.

Juju relations are particularly useful for enabling TLS. Charmed Apache Cassandra implements the
**Requirer** side of the [tls-certificates](https://charmhub.io/integrations/tls-certificates)
relation, so any charm implementing the **Provider** side can supply certificates. The relation
centralises certificate management, handling provisioning, requests, and renewal, and lets you use
different providers such as self-signed certificates or external services like Let's Encrypt.

Charmed Apache Cassandra supports two separate TLS channels, each exposed as its own relation
endpoint:

- `peer-certificates` - encrypts peer-to-peer traffic between the nodes in the cluster
- `client-certificates` - encrypts client-to-node traffic, for example connections from `cqlsh`

```{note}
In this tutorial we distribute [self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate)
signed by a single root CA. This is for testing and demonstration only. Self-signed certificates are
not recommended for a production cluster. For guidance on choosing a provider, see the
[Security with X.509 certificates](https://charmhub.io/topics/security-with-x-509-certificates) page.
```

## Deploy a certificate provider

Before enabling TLS on Charmed Apache Cassandra, deploy the `self-signed-certificates` charm:

```shell
juju deploy self-signed-certificates --channel 1/stable --config ca-common-name="Tutorial CA"
```

Wait for the charm to settle into an `active`/`idle` state, as shown by `juju status`.

## Enable peer-to-peer encryption

To encrypt traffic between the cluster nodes, integrate Charmed Apache Cassandra with the certificate
provider over the `peer-certificates` endpoint:

```shell
juju integrate cassandra:peer-certificates self-signed-certificates
```

While certificates are being issued and distributed, units show waiting and maintenance statuses such
as `waiting for internal TLS setup` and `waiting for peer tls rotation to complete`. Once the process
completes, the units return to `active`/`idle`.

## Enable client-to-node encryption

To encrypt traffic between clients and the cluster, integrate over the `client-certificates`
endpoint:

```shell
juju integrate cassandra:client-certificates self-signed-certificates
```

Again the units briefly show statuses such as `waiting for TLS setup` and `waiting for client tls
rotation to complete` before returning to `active`/`idle`.

## Reconnect with cqlsh over SSL

Now that client-to-node encryption is enabled, the cluster rejects plaintext connections. If you try
to connect the way we did in the previous chapter, it fails:

```shell
charmed-cassandra.cqlsh "$CASSANDRA_IP" -u operator -p "<operator-password>"
```

```text
Connection error: ('Unable to connect to any servers',
  {'10.166.144.10:9042': ConnectionResetError(104, 'Connection reset by peer')})
```

This confirms that Apache Cassandra now requires a secure TLS connection.

### Retrieve the root CA

`cqlsh` needs the root CA to verify the certificate the Cassandra nodes present during the TLS
handshake. Fetch it from the `self-signed-certificates` charm.

Since the `charmed-cassandra` snap is strictly confined, the CA must live in a location the snap can
read. Write it directly into the snap's configuration directory:

```shell
juju run self-signed-certificates/0 get-ca-certificate --format json \
  | jq -r '."self-signed-certificates/0".results."ca-certificate"' \
  | sudo tee /var/snap/charmed-cassandra/current/etc/cassandra/ca.cert
```

### Create a cqlshrc file

In the same directory, create a `cqlshrc` configuration file that tells `cqlsh` which credentials to
use and where to find the CA. Retrieve the `operator` password again if you need it:

```shell
juju show-secret --reveal cassandra-peers.cassandra.app --format json \
  | jq -r '.[].content.Data."operator-password"'
```

Then create `/var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc` with the following content,
substituting the password and the `<CASSANDRA-IP>` address captured earlier:

```ini
[authentication]
username = operator
password = <operator-password>

[connection]
hostname = <CASSANDRA-IP>
port = 9042

[ssl]
certfile = /var/snap/charmed-cassandra/current/etc/cassandra/ca.cert
validate = true
```

`cqlsh` refuses to use a `cqlshrc` that other users can read, so restrict its permissions to your
user only:

```shell
sudo chown $USER /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
sudo chmod 600 /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

### Connect

Connect to the cluster over TLS by pointing `cqlsh` at the `cqlshrc` file and passing the `--ssl`
flag:

```shell
charmed-cassandra.cqlsh --ssl \
  --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

<details> <summary> Output example</summary>

```text
[cqlsh 6.1.0 | Cassandra 5.0.5 | CQL spec 3.4.7 | Native protocol v5]
Use HELP for help.
operator@cqlsh>
```

</details>

The connection now succeeds over an encrypted channel. The `hostname` set in `cqlshrc` is the default
target, but you can always connect to a specific node by passing its address as a positional
argument, which is handy once the cluster has more nodes:

```shell
charmed-cassandra.cqlsh --ssl "$CASSANDRA_IP" \
  --cqlshrc /var/snap/charmed-cassandra/current/etc/cassandra/cqlshrc
```

Leave the shell with `exit`. We will reuse this `cqlshrc` file for every `cqlsh` command in the rest
of the tutorial.

```{note}
If you ever want to go back to unencrypted connections, remove the relations with
`juju remove-relation cassandra:client-certificates self-signed-certificates` and
`juju remove-relation cassandra:peer-certificates self-signed-certificates`. This triggers a rolling
restart as the units switch back to plaintext communication.
```

## What's next?

The cluster is now secured. In the next section we will integrate a client application to generate a
scoped user and a dedicated keyspace automatically through a relation.
