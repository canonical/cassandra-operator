# Contributing workflow

To make contributions to this charm, you'll need a working [development setup](https://juju.is/docs/sdk/dev-setup).

You can, but not required to, create an environment for development with `tox`:

```shell
tox devenv -e integration
source venv/bin/activate
```

## Testing

This project uses `tox` for managing test environments. There are some pre-configured environments
that can be used for linting, formatting and testing code when you're preparing contributions to the charm:

```shell
tox run -e format                 # update your code according to linting rules
tox run -e lint                   # verify your code according to linting rules
tox run -e unit                   # run unit tests
tox run -e integration-charm      # run charm integration test
tox run -e integration-config     # run config integration test
tox run -e integration-multinode  # run multinode integration test
tox run -e integration-scaling    # run scaling integration test
```

## Build the charm

Build the charm in this git repository using:

```shell
charmcraft pack
```

<!-- You may want to include any contribution/style guidelines in this document>
