from app.domain.proxy_kind import ProxyKind, classify_proxy

_SPARK_SUB_PROXY = "0x3300f198988e4c9c63f75df86de36421f06af8c4"
_GROVE_SUB_PROXY = "0x1369f7b2b38c76b6478c0f0e66d94923421891ba"
_SPARK_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_GROVE_ALM = "0x491edfb0b8b608044e227225c715981a30f3a44e"


def test_classify_proxy_returns_sub_proxy_for_known_sub_proxies():
    assert classify_proxy(_SPARK_SUB_PROXY) is ProxyKind.SUB_PROXY
    assert classify_proxy(_GROVE_SUB_PROXY) is ProxyKind.SUB_PROXY


def test_classify_proxy_returns_alm_for_alm_proxies():
    assert classify_proxy(_SPARK_ALM) is ProxyKind.ALM
    assert classify_proxy(_GROVE_ALM) is ProxyKind.ALM


def test_classify_proxy_returns_alm_for_unknown_addresses():
    assert classify_proxy("0x" + "ab" * 20) is ProxyKind.ALM


def test_classify_proxy_is_case_insensitive():
    assert classify_proxy(_SPARK_SUB_PROXY.upper()) is ProxyKind.SUB_PROXY
    assert classify_proxy("0X" + _GROVE_SUB_PROXY[2:].upper()) is ProxyKind.SUB_PROXY
