import pytest
from detl.schema import Manifesto
from pydantic import ValidationError

def test_invalid_defaults_vandalism():
    # Similar to user's config: string using fill_median in defaults
    yaml_dict = {
        "conf": {
            "defaults": {
                "string": {
                    "on_null": {"tactic": "fill_median"}
                }
            }
        },
        "columns": {}
    }
    with pytest.raises(ValidationError, match="Tactic 'NullTactic.FILL_MEDIAN' in defaults cannot be used on dtype 'DType.STRING'"):
        Manifesto(**yaml_dict)

def test_invalid_defaults_constraint_vandalism():
    yaml_dict = {
        "conf": {
            "defaults": {
                "string": {
                    "constraints": {
                        "max_policy": {
                            "threshold": 10,
                            "violate_action": {"tactic": "drop_row"}
                        }
                    }
                }
            }
        },
        "columns": {}
    }
    with pytest.raises(ValidationError, match="min/max policy in defaults cannot be applied to 'DType.STRING'"):
        Manifesto(**yaml_dict)
