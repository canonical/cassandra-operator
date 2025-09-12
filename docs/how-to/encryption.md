# Managing encryption

The Apache Cassandra charm has encryption enabled by default.
All units within a cluster share the same CA certificate file, but each unit has a distinct private key.

This charm implements the **Requirer** side of the [tls-certificates](https://charmhub.io/integrations/tls-certificates) relation. Therefore, any charm implementing the **Provider** side can be used.
To enable TLS encryption, you must first deploy a TLS certificates Provider charm.


## Deploy a tls provider charm

For testing purposes, you can use the `self-signed-certificates` charm.
However, this setup is **not recommended** for production clusters.

Deploy the `self-signed-certificates` charm with:

```shell
juju deploy self-signed-certificates --channel=edge
```

Configure the CA common name:

```shell
juju config self-signed-certificates ca-common-name="Test CA"
```

For an overview of available TLS certificate Provider charms and guidance on choosing the right one for your use case, see [this guide](https://charmhub.io/topics/security-with-x-509-certificates).

## Relate the charms

To enable **peer-to-peer TLS encryption**, relate Cassandra to the `:peer-certificates` endpoint:

```shell
juju integrate <tls-certificates>:certificates cassandra:peer-certificates
```

To enable **client-to-node TLS encryption**, relate Cassandra to the `:client-certificates` endpoint:

```shell
juju integrate <tls-certificates>:certificates cassandra:client-certificates
```

where `<tls-certificates>` is the name of the deployed TLS certificates Provider charm.

## Connect to the cluster

Authentication is enabled by default.
To retrieve the password for the default `operator` user:

```shell
juju show-secret --reveal "cassandra-peers.<application name>.app" --format json \
  | jq -r '.[].content.Data."operator-password"'
```

### Verify client tls

First, attempt to connect **without specifying TLS certificates**:

```shell
cqlsh <unit-ip> -u operator -p "<password>"
```

This should result in an error:

```
Warning: Using a password on the command line interface can be insecure.
Recommendation: use the credentials file to securely provide the password.

Connection error: ('Unable to connect to any servers',
  {'10.166.144.168:9042': ConnectionResetError(104, 'Connection reset by peer')})
```

And in the Cassandra logs you will see:

```
WARN  [epollEventLoopGroup-5-6] ... SSLException in client networking with peer /10.166.144.168:42604
io.netty.handler.ssl.NotSslRecordException: not an SSL/TLS record
```

This confirms that Cassandra requires a secure TLS connection.

### Retrieving client certificates

Fetch the client private key and signed certificate from a unit:

```shell
juju ssh <unit-name> "cat /var/snap/charmed-cassandra/current/etc/cassandra/tls/client-private.key" > ./client.key
juju ssh <unit-name> "cat /var/snap/charmed-cassandra/current/etc/cassandra/tls/client-unit.pem" > ./client.pem
```

### Configuring `cqlsh`

To properly use these files, create a `cqlshrc` configuration file for `cqlsh`:

```ini
[authentication]
username = operator
password = <password>

[connection]
hostname = <unit-ip>
port = 9042

[ssl]
certfile = ./client.pem
validate = true
userkey = ./client.key
usercert = ./client.pem
```

Connect to Cassandra with:

```shell
cqlsh --ssl --cqlshrc=./cqlshrc
```

## Disabling tls

To disable TLS, remove the relations:

```shell
juju remove-relation <tls-certificates>:certificates cassandra:peer-certificates
juju remove-relation <tls-certificates>:certificates cassandra:client-certificates
```

where `<tls-certificates>` is the name of the TLS certificates Provider charm deployed.
