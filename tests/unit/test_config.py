import tempfile
from pathlib import Path

import pytest

from managers.config import ConfigManager


class MockCassandraPaths:
    """Mock object for cassandra paths."""

    def __init__(self, temp_dir: Path):
        self.commitlog_directory = temp_dir / "commitlog"
        self.data_file_directory = temp_dir / "data"
        self.hints_directory = temp_dir / "hints"
        self.saved_caches_directory = temp_dir / "caches"
        self.config = temp_dir / "cassandra.yaml"
        self.env = temp_dir / "cassandra-env.sh"

        # Create directories and files
        for directory in [
            self.commitlog_directory,
            self.data_file_directory,
            self.hints_directory,
            self.saved_caches_directory,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

        # Create initial env file
        self.env.write_text("EXISTING_VAR=existing_value\nANOTHER_VAR=another_value\n")


class MockWorkload:
    """Mock workload for testing."""

    def __init__(self, temp_dir: Path):
        self.cassandra_paths = MockCassandraPaths(temp_dir)


@pytest.fixture
def temp_workload():
    """Create temporary workload for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        workload = MockWorkload(temp_path)
        yield workload


@pytest.fixture
def config_manager(temp_workload):
    """Create ConfigManager instance."""
    return ConfigManager(temp_workload)


def test_render_env_with_memory_limit(config_manager):
    """Test env rendering with memory limit."""
    config_manager.render_env(cassandra_limit_memory_mb=2048)

    env_content = config_manager.workload.cassandra_paths.env.read_text()

    assert "EXISTING_VAR=existing_value" in env_content
    assert "ANOTHER_VAR=another_value" in env_content

    assert "MAX_HEAP_SIZE=2048M" in env_content
    assert "HEAP_NEWSIZE=1024M" in env_content

    # Change limits

    config_manager.render_env(cassandra_limit_memory_mb=4096)

    env_content = config_manager.workload.cassandra_paths.env.read_text()

    assert "EXISTING_VAR=existing_value" in env_content
    assert "ANOTHER_VAR=another_value" in env_content

    assert "MAX_HEAP_SIZE=4096M" in env_content
    assert "HEAP_NEWSIZE=2048M" in env_content

    assert "MAX_HEAP_SIZE=2048M" not in env_content
    assert "HEAP_NEWSIZE=1024M" not in env_content


def test_render_env_preserves_existing_vars(config_manager):
    """Test that render_env preserves existing environment variables."""
    # Add some extra variables to the env file
    original_content = config_manager.workload.cassandra_paths.env.read_text()
    extra_content = original_content + "EXTRA_VAR=extra_value\nPATH=/custom/path\n"
    config_manager.workload.cassandra_paths.env.write_text(extra_content)

    config_manager.render_env(cassandra_limit_memory_mb=2048)

    final_content = config_manager.workload.cassandra_paths.env.read_text()

    # All original variables should be preserved
    assert "EXISTING_VAR=existing_value" in final_content
    assert "ANOTHER_VAR=another_value" in final_content
    assert "EXTRA_VAR=extra_value" in final_content
    assert "PATH=/custom/path" in final_content

    # New heap variables should be added
    assert "MAX_HEAP_SIZE=2048M" in final_content
    assert "HEAP_NEWSIZE=1024M" in final_content
