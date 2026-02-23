from detl.config import Config
from detl.exceptions import DetlException
import pytest

def test_fill_value_type_restrictions():
    # 1. Invalid Int
    with pytest.raises(DetlException) as exc:
        Config({"columns": {"test": {"dtype": "int", "on_null": {"tactic": "fill_value", "value": "A string"}}}})
    assert "must be a number" in str(exc.value)

    # 2. Invalid String
    with pytest.raises(DetlException) as exc:
        Config({"columns": {"test": {"dtype": "string", "on_null": {"tactic": "fill_value", "value": 123}}}})
    assert "must be a string" in str(exc.value)
    
    # 3. Invalid Boolean
    with pytest.raises(DetlException) as exc:
        Config({"columns": {"test": {"dtype": "boolean", "on_null": {"tactic": "fill_value", "value": "True"}}}})
    assert "must be a boolean" in str(exc.value)
    
    # 4. Invalid Date (must be string format)
    with pytest.raises(DetlException) as exc:
        Config({"columns": {"test": {"dtype": "date", "on_null": {"tactic": "fill_value", "value": 20240101}}}})
    assert "must be a string matching the defined format" in str(exc.value)
    
    # 5. Invalid in Defaults
    with pytest.raises(DetlException) as exc:
        Config({"conf": {"defaults": {"date": {"on_null": {"tactic": "fill_value", "value": True}}}}})
    assert "must be a string matching the defined format" in str(exc.value)
    
    # 6. Valid Paths
    Config({"columns": {"test": {"dtype": "date", "on_null": {"tactic": "fill_value", "value": "2024-01-01"}}}})
    Config({"columns": {"test": {"dtype": "string", "on_null": {"tactic": "fill_value", "value": "Empty"}}}})
    Config({"columns": {"test": {"dtype": "int", "on_null": {"tactic": "fill_value", "value": 0}}}})
    Config({"columns": {"test": {"dtype": "boolean", "on_null": {"tactic": "fill_value", "value": False}}}})
