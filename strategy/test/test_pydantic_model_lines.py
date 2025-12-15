from strategy.models.k1 import pydantic_model as pm


def test_generic_line_key_handles_line_prefix_variants():
    assert pm.generic_line_key("line_11ZZ_other_income") == "line_11ZZ"
    assert pm.generic_line_key("line_x_custom_field") == "line_x"
    assert pm.generic_line_key("line_") == "line_"
    assert pm.generic_line_key("other") == "other"


def test_default_line_value_resolver_empty_options():
    assert pm.default_line_value_resolver("line", []) is None
