import inspect

from app.ports.position_repository import PositionRepository


def test_repository_defines_list_method():
    """Verify the repository Protocol defines async list method."""
    method = getattr(PositionRepository, "list_latest_user_positions")

    assert inspect.iscoroutinefunction(method)
