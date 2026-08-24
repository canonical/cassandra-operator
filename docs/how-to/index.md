---
myst:
  html_meta:
    description: "Charmed Apache Cassandra how-to guides - step-by-step instructions for deploying, scaling, securing, and monitoring Cassandra clusters."
---

(how-to-index)=

# How-to guides

These guides provide step-by-step instructions for common operational tasks with Charmed Apache Cassandra.

## Deployment

- {doc}`deploy` - Deploy a cluster and connect to it with `cqlsh`

## Operations

- {doc}`manage-units` - Scale the cluster up and down and verify the cluster topology

## Security

- {doc}`encryption` - Enable TLS encryption for secure communications between nodes
- {doc}`manage-passwords` - Retrieve and rotate the `operator` password

## Observability

- {doc}`monitoring` - Set up monitoring with the Canonical Observability Stack (COS)

```{toctree}
:hidden:

Deploy <deploy>
Manage units <manage-units>
Manage passwords <manage-passwords>
TLS encryption <encryption>
Monitoring <monitoring>
```
