from pathlib import Path

import pytest

from app.risk_engine.mapping import MappingError, load_asset_mapping

TESTDATA = Path(__file__).parent / "testdata"


class TestLoadAssetMapping:
    def test_loads_valid_json(self) -> None:
        mapping = load_asset_mapping(TESTDATA / "valid_mapping.json")

        assert mapping == {"aUSDC": "aave_ausdc", "spUSDC": "sparklend_spusdc"}

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(MappingError, match="not found"):
            load_asset_mapping(tmp_path / "does_not_exist.json")

    def test_raises_on_malformed_json(self) -> None:
        with pytest.raises(MappingError, match="not valid JSON"):
            load_asset_mapping(TESTDATA / "malformed_mapping.json")

    def test_raises_on_wrong_value_type(self) -> None:
        with pytest.raises(MappingError, match="wrong shape"):
            load_asset_mapping(TESTDATA / "wrong_shape_mapping.json")

    def test_raises_on_non_object_root(self, tmp_path: Path) -> None:
        path = tmp_path / "list.json"
        path.write_text('["a", "b"]')
        with pytest.raises(MappingError, match="wrong shape"):
            load_asset_mapping(path)

    def test_empty_object_is_valid(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.json"
        path.write_text("{}")
        assert load_asset_mapping(path) == {}
