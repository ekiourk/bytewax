"""`pytest` config for `pytests/`.

This sets up our fixtures and logging.

"""

import os
from datetime import datetime, timezone

import pytest
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import cluster_main, run_main
from bytewax.tracing import setup_tracing
from pytest import fixture


@fixture(scope="session")
def kafka_server():
    """Provide a Kafka broker URL.

    Resolution order:
    1. If `TEST_KAFKA_BROKER` is set to a non-empty value, use it as-is.
    2. Otherwise spin up a Kafka container via `testcontainers` (works
       both locally and on CI runners that have Docker, e.g. linux GHA).
    3. If Docker isn't available (e.g. on macOS/Windows GHA runners
       without Docker, or in environments without the daemon running),
       skip with a clear reason.

    """
    broker = os.environ.get("TEST_KAFKA_BROKER", "").strip()
    if broker:
        yield broker
        return

    # Deferred imports so the suite still runs without `testcontainers`
    # or the underlying `docker` library.
    try:
        from docker.errors import DockerException  # noqa: PLC0415
        from testcontainers.kafka import KafkaContainer  # noqa: PLC0415
    except ImportError:
        pytest.skip("`testcontainers` or `docker` not installed; skip Kafka tests")

    try:
        # Disable auto-topic-creation on the broker: tests that need
        # topics create them explicitly via AdminClient (see `tmp_topic`),
        # and `test_input_raises_on_topic_not_exist` relies on the
        # broker actually rejecting a missing-topic read instead of
        # silently materializing the topic.
        kafka_container = KafkaContainer("confluentinc/cp-kafka:7.6.0").with_env(
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"
        )
        with kafka_container as kafka:
            yield kafka.get_bootstrap_server()
    except DockerException as e:
        pytest.skip(f"Docker not available: {e}")


@fixture(params=["run_main", "cluster_main-1thread", "cluster_main-2thread"])
def entry_point_name(request):
    """Run a version of the test for each execution point.

    You probably want to use the `entry_point` fixture to get a
    callable instead of the name here.

    There will be `"run_main"` for single in-thread, and
    `"cluster_main-2thread"` for launching 2 worker sub-threads.

    """
    return request.param


def _wrapped_cluster_main1x2(*args, **kwargs):
    return cluster_main(*args, [], 0, worker_count_per_proc=2, **kwargs)


def _wrapped_cluster_main1x1(*args, **kwargs):
    return cluster_main(*args, [], 0, **kwargs)


@fixture
def entry_point(entry_point_name):
    """Run a version of this test for each execution point.

    See `entry_point_name` for options.

    """
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "cluster_main-1thread":
        return _wrapped_cluster_main1x1
    elif entry_point_name == "cluster_main-2thread":
        return _wrapped_cluster_main1x2
    else:
        msg = "unknown entry point name: {request.param!r}"
        raise ValueError(msg)


@fixture
def recovery_config(tmp_path):
    """Generate a recovery config.

    It will point to a single partition recovery store.

    """
    init_db_dir(tmp_path, 1)
    yield RecoveryConfig(str(tmp_path))


@fixture
def now():
    """Get the current `datetime` in UTC."""
    yield datetime.now(timezone.utc)


def pytest_addoption(parser):
    """Add a `--bytewax-log-level` CLI option to pytest.

    This will control the `setup_tracing` log level.

    """
    parser.addoption(
        "--bytewax-log-level",
        action="store",
        choices=["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
    )


def pytest_configure(config):
    """This will run on pytest init."""
    log_level = config.getoption("--bytewax-log-level")
    if log_level:
        setup_tracing(log_level=log_level)
