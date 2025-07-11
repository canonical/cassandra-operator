# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


from unittest.mock import MagicMock

from managers.config import ConfigManager


def test_render_env_preserves_existing_vars():
    """`render_env` should preserve existing environment variables."""
    workload = MagicMock(cassandra_paths=MagicMock(env=MagicMock()))
    config_manager = ConfigManager(workload=workload, cluster_name="", listen_address="", seeds=[])

    workload.cassandra_paths.env.read_text.return_value = (
        "EXTRA_VAR=extra_value\nPATH=/custom/path\n"
    )

    config_manager.render_env(cassandra_limit_memory_mb=1024)

    workload.cassandra_paths.env.read_text.assert_called()
    workload.cassandra_paths.env.write_text.assert_called()
    result = workload.cassandra_paths.env.write_text.call_args[0][0]
    assert "EXTRA_VAR=extra_value" in result
    assert "PATH=/custom/path" in result
    assert "MAX_HEAP_SIZE=1024M" in result
    assert "HEAP_NEWSIZE=512M" in result
