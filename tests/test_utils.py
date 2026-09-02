"""Unit tests for biodata_cache.utils module."""

from biodata_cache.utils import (
    BDC_VERSION,
    apply_first_name_map,
    build_first_name_map,
    duckdb_query,
    normalize_experimenters,
    normalize_instrument_id,
    normalize_name,
    parse_experimenters,
)


def test_zs_version_is_string():
    assert isinstance(BDC_VERSION, str)
    assert len(BDC_VERSION) > 0


def test_duckdb_query_binds_parameters():
    result = duckdb_query("SELECT ? AS value", ["literal' value"])

    assert result.to_dict("records") == [{"value": "literal' value"}]


def test_normalize_instrument_id_underscore_separator():
    assert normalize_instrument_id("AIND_MESO2_20240115") == "MESO2"


def test_normalize_instrument_id_with_location_prefix():
    assert normalize_instrument_id("HQ_NP3_20231005") == "NP3"


def test_normalize_instrument_id_hyphen_separator():
    assert normalize_instrument_id("HQ-NP3_20231005") == "NP3"


def test_normalize_instrument_id_already_short():
    assert normalize_instrument_id("MESO2") == "MESO2"


def test_normalize_instrument_id_none():
    assert normalize_instrument_id(None) == ""


def test_normalize_instrument_id_iso_date():
    assert normalize_instrument_id("AIND_MESO2_2024-01-15") == "MESO2"


def test_normalize_instrument_id_short_year():
    assert normalize_instrument_id("AIND_NP3_231005") == "NP3"


def test_normalize_instrument_id_strips_internal_separators():
    assert normalize_instrument_id("AIND_NP-3_20240115") == "NP3"


def test_normalize_instrument_id_empty_string():
    assert normalize_instrument_id("") == ""


def test_normalize_name_dots():
    assert normalize_name("anna.katelyn.mcdougal") == "Anna Katelyn Mcdougal"


def test_normalize_name_single():
    assert normalize_name("nick.ponvert") == "Nick Ponvert"


def test_normalize_name_extra_spaces():
    assert normalize_name("  john  doe  ") == "John Doe"


def test_normalize_name_underscores():
    assert normalize_name("john_doe") == "John Doe"


def test_normalize_name_empty():
    assert normalize_name("") == ""


def test_normalize_name_already_clean():
    assert normalize_name("John Doe") == "John Doe"


def test_parse_experimenters_basic():
    assert parse_experimenters("nick.ponvert, anna.katelyn.mcdougal") == ["Nick Ponvert", "Anna Katelyn Mcdougal"]


def test_parse_experimenters_deduplicates():
    assert parse_experimenters("john.doe, John Doe") == ["John Doe"]


def test_parse_experimenters_none():
    assert parse_experimenters(None) == []


def test_parse_experimenters_empty():
    assert parse_experimenters("") == []


def test_parse_experimenters_single():
    assert parse_experimenters("nick.ponvert") == ["Nick Ponvert"]


def test_parse_experimenters_whitespace_only():
    assert parse_experimenters("   ") == []


def test_normalize_experimenters_list_of_names():
    assert normalize_experimenters(["nick.ponvert", "anna.katelyn.mcdougal"]) == [
        "Nick Ponvert",
        "Anna Katelyn Mcdougal",
    ]


def test_normalize_experimenters_comma_separated_element():
    assert normalize_experimenters(["nick.ponvert, anna.katelyn.mcdougal"]) == ["Nick Ponvert", "Anna Katelyn Mcdougal"]


def test_normalize_experimenters_deduplicates_across_elements():
    assert normalize_experimenters(["john.doe", "John Doe", "john.doe"]) == ["John Doe"]


def test_normalize_experimenters_empty_list():
    assert normalize_experimenters([]) == []


def test_normalize_experimenters_skips_none_and_empty():
    assert normalize_experimenters([None, "", "nick.ponvert"]) == ["Nick Ponvert"]


def test_normalize_experimenters_preserves_insertion_order():
    assert normalize_experimenters(["zoe.smith", "anna.jones"]) == ["Zoe Smith", "Anna Jones"]


def test_normalize_experimenters_merges_first_name_into_full_name():
    assert normalize_experimenters(["Huy", "Huy Lastname"]) == ["Huy Lastname"]


def test_normalize_experimenters_merges_first_name_order_independent():
    assert normalize_experimenters(["Huy Lastname", "Huy"]) == ["Huy Lastname"]


def test_normalize_experimenters_keeps_ambiguous_first_name():
    assert normalize_experimenters(["Huy", "Huy Lastname", "Huy Other"]) == ["Huy", "Huy Lastname", "Huy Other"]


def test_normalize_experimenters_keeps_first_name_with_no_full_name_match():
    assert normalize_experimenters(["Alice", "Bob Smith"]) == ["Alice", "Bob Smith"]


def test_build_first_name_map_unambiguous():
    assert build_first_name_map(["Huy", "Huy Lastname", "Alice Smith"]) == {"Huy": "Huy Lastname"}


def test_build_first_name_map_ambiguous():
    assert build_first_name_map(["Huy", "Huy Lastname", "Huy Other"]) == {}


def test_build_first_name_map_no_singles():
    assert build_first_name_map(["Alice Smith", "Bob Jones"]) == {}


def test_build_first_name_map_empty():
    assert build_first_name_map([]) == {}


def test_apply_first_name_map_replaces():
    assert apply_first_name_map(["Huy", "Alice Smith"], {"Huy": "Huy Lastname"}) == ["Huy Lastname", "Alice Smith"]


def test_apply_first_name_map_deduplicates():
    assert apply_first_name_map(["Huy", "Huy Lastname"], {"Huy": "Huy Lastname"}) == ["Huy Lastname"]


def test_apply_first_name_map_no_match():
    assert apply_first_name_map(["Alice", "Bob Smith"], {}) == ["Alice", "Bob Smith"]
