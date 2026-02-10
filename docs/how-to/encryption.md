# Managing encryption

The Apache Cassandra charm has encryption enabled by default.
All units within a cluster share the same CA certificate file, but each unit has a distinct private key.

This charm implements the **Requirer** side of the [tls-certificates](https://charmhub.io/integrations/tls-certificates) relation. Therefore, any charm implementing the **Provider** side can be used.
To enable TLS encryption, you must first deploy a TLS certificates Provider charm.

## Deploy a TLS provider charm

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

For an overview of available TLS certificate Provider charms and guidance on choosing the right one
for your use case, see the [X.509 certificates guide](https://charmhub.io/topics/security-with-x-509-certificates).

## Relate the charms

To enable **peer-to-peer TLS encryption**, relate Charmed Apache Cassandra to the `:peer-certificates` endpoint:

```shell
juju integrate <tls-certificates>:certificates cassandra:peer-certificates
```

To enable **client-to-node TLS encryption**, relate Charmed Apache Cassandra to the `:client-certificates` endpoint:

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
juju run <tls-certificates>/0 get-ca-certificate --format yaml | yq '.<tls-certificates>/0.results.ca-certificate' > ca.cert
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

## Disabling TLS

To disable TLS, remove the relations:

```shell
juju remove-relation <tls-certificates>:certificates cassandra:peer-certificates
juju remove-relation <tls-certificates>:certificates cassandra:client-certificates
```

where `<tls-certificates>` is the name of the TLS certificates Provider charm deployed.
