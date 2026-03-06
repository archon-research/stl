import inspect

from app.ports.position_repository import PositionRepository


def test_repository_defines_list_method():
    """Verify the repository Protocol defines async list method with chain_id parameter."""
    method = getattr(PositionRepository, "list_latest_user_positions")

    assert inspect.iscoroutinefunction(method)

    sig = inspect.signature(method)
    params = list(sig.parameters.keys())
    assert "protocol_id" in params
    assert "chain_id" in params
    assert "limit" in params
