from app.services.data_provenance_service import DataProvenanceService


def test_get_sources_returns_copy_not_internal_list() -> None:
    service = DataProvenanceService()

    sources = service.get_sources()
    original_count = len(sources)
    sources.pop()

    assert len(sources) == original_count - 1
    assert len(service.get_sources()) == original_count


def test_get_source_by_host_is_case_insensitive() -> None:
    service = DataProvenanceService()

    source = service.get_source_by_host("HTTPS://INFO.SKYECO.COM/REQUIRED-RISK-CAPITAL")

    assert source is not None
    assert source.name == "Star Agents Risk Capital & Requirements Monitor"


def test_get_sources_by_role_performs_substring_match() -> None:
    service = DataProvenanceService()

    results = service.get_sources_by_role("oracle")

    assert len(results) >= 1
    assert any("oracle" in source.role.lower() for source in results)


def test_required_sources_exist() -> None:
    service = DataProvenanceService()
    sources = service.get_sources()

    assert any(s.name == "STL Allocation Index" and s.host == "Same app (internal API)" for s in sources)
    assert any(
        s.name == "Star Agents Risk Capital & Requirements Monitor"
        and s.host == "https://info.skyeco.com/required-risk-capital"
        for s in sources
    )
    # The dashboard's risk-capital figures are self-computed on-chain.
    assert any(s.name == "Self-computed Risk Capital (gap_sweep)" for s in sources)
