from pathlib import Path

import pytest

from app.risk_engine.mapping import MappingError, load_asset_mapping

TESTDATA = Path(__file__).parent / "testdata"


class TestLoadAssetMapping:
    def test_loads_valid_mapping(self) -> None:
        result = load_asset_mapping(TESTDATA / "valid_mapping.json")

        assert result == [
            (1, bytes.fromhex("Bcca60bB61934080951369a648Fb03DF4F96263C"), "aave_ausdc"),
            (1, bytes.fromhex("59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB"), "sparklend_spusdc"),
        ]

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
        assert load_asset_mapping(path) == []

    def test_raises_on_missing_colon_in_key(self, tmp_path: Path) -> None:
        path = tmp_path / "bad_key.json"
        path.write_text('{"10xAbCd": "some_rating"}')
        with pytest.raises(MappingError, match="invalid composite key"):
            load_asset_mapping(path)

    def test_raises_on_non_integer_chain_id(self, tmp_path: Path) -> None:
        path = tmp_path / "bad_chain.json"
        path.write_text('{"abc:0xAbCd": "some_rating"}')
        with pytest.raises(MappingError, match="invalid composite key"):
            load_asset_mapping(path)

    def test_raises_on_missing_0x_prefix(self, tmp_path: Path) -> None:
        path = tmp_path / "no_prefix.json"
        path.write_text('{"1:AbCd": "some_rating"}')
        with pytest.raises(MappingError, match="invalid composite key"):
            load_asset_mapping(path)

    def test_raises_on_non_hex_address(self, tmp_path: Path) -> None:
        path = tmp_path / "bad_hex.json"
        path.write_text('{"1:0xGGGG": "some_rating"}')
        with pytest.raises(MappingError, match="invalid composite key"):
            load_asset_mapping(path)

    def test_raises_on_wrong_address_length(self, tmp_path: Path) -> None:
        path = tmp_path / "short_addr.json"
        path.write_text('{"1:0xAbCd": "some_rating"}')
        with pytest.raises(MappingError, match="address must be 20 bytes"):
            load_asset_mapping(path)

    def test_raises_on_duplicate_address_different_case(self, tmp_path: Path) -> None:
        addr_lower = "0x" + "ab" * 20
        addr_upper = "0x" + "AB" * 20
        path = tmp_path / "dup.json"
        path.write_text(f'{{  "1:{addr_lower}": "rating_a",  "1:{addr_upper}": "rating_b"}}')
        with pytest.raises(MappingError, match="duplicate receipt token"):
            load_asset_mapping(path)

    def test_raises_on_exact_duplicate_json_key(self, tmp_path: Path) -> None:
        addr = "0x" + "ab" * 20
        # Write raw bytes to bypass Python dict dedup — JSON allows repeated keys.
        path = tmp_path / "dup_exact.json"
        path.write_text(f'{{"1:{addr}": "rating_a", "1:{addr}": "rating_b"}}')
        with pytest.raises(MappingError, match="duplicate key"):
            load_asset_mapping(path)
