---
myst:
  html_meta:
    description: "Step-by-step tutorial for Charmed Apache Cassandra - bootstrap a controller, deploy a cluster, enable TLS, integrate a client, use cqlsh, and scale."
---

(tutorial-introduction)=

```{include} introduction.md

```

(tutorial-index)=

## Step-by-step guide

Here is an overview of the steps required, with links to the individual tutorials that deal with
each one:

- [Set up the environment](tutorial-environment)
- [Deploy Charmed Apache Cassandra](tutorial-deploy)
- [Enable encryption](tutorial-enable-encryption)
- [Integrate with a client application](tutorial-integrate-with-client-applications)
- [Manage data with cqlsh](tutorial-manage-data)
- [Scale your cluster](tutorial-scale)
- [Clean up your environment](tutorial-cleanup)

```{toctree}
---
titlesonly:
maxdepth: 2
hidden:
---
1. Set up the environment<environment.md>
2. Deploy Apache Cassandra<deploy.md>
3. Enable encryption<enable-encryption.md>
4. Integrate with a client<integrate-with-client-applications.md>
5. Manage data with cqlsh<manage-data.md>
6. Scale your cluster<scale.md>
7. Clean up your environment<cleanup.md>
```
