from app.services.data_provenance_service import DataProvenanceService, SourceAccessModel


def test_get_sources_returns_copy_not_internal_list() -> None:
    service = DataProvenanceService()

    sources = service.get_sources()
    original_count = len(sources)
    sources.pop()

    assert len(sources) == original_count - 1
    assert len(service.get_sources()) == original_count


def test_get_source_by_host_is_case_insensitive() -> None:
    service = DataProvenanceService()

    source = service.get_source_by_host("HTTPS://INFO-SKY.BLOCKANALITICA.COM/STAR-MONITORING/RISK-CAPITAL")

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
        and s.host == "https://info-sky.blockanalitica.com/star-monitoring/risk-capital"
        for s in sources
    )
    # The dashboard's risk-capital figures are self-computed on-chain.
    assert any(s.name == "Self-computed Risk Capital (gap_sweep)" for s in sources)


def test_both_reference_hosts_are_registered_as_indexed_not_live() -> None:
    # Two hosts, two questions, and neither is read per request any more. The
    # registry publishing one of them as a live read is what a reader checks to
    # know how fresh a reference figure can be.
    service = DataProvenanceService()
    by_name = {s.name: s for s in service.get_sources()}

    for name in ("Star Agents Risk Capital & Requirements Monitor", "Sky Internal Balance-Sheet Feed"):
        source = by_name.get(name)
        assert source is not None, name
        assert source.caveat is not None
        assert "15 minutes" in source.caveat
        assert "per request" in source.caveat


def test_anchorage_custody_source_registered_as_closed_offchain() -> None:
    service = DataProvenanceService()

    source = next((s for s in service.get_sources() if s.name == "Anchorage Custody API"), None)

    assert source is not None
    assert source.access_model is SourceAccessModel.CLOSED
    assert "custody" in source.role.lower()
    assert source.caveat is not None and "15" in source.caveat
