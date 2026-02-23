from detl.config import Config
from detl.exceptions import DetlException
import pytest

def test_idiot_proof_defaults():
    yaml_dict = {
        "conf": {
            "undefined_columns": "keep",
            "defaults": {
                "string": {
                    "on_null": {
                        "tactic": "fill_median"
                    }
                }
            }
        },
        "columns": {}
    }
    
    with pytest.raises(DetlException) as exc:
        Config(yaml_dict)
        
    assert "Tactic 'NullTactic.FILL_MEDIAN' in defaults cannot be used on dtype 'DType.STRING'" in str(exc.value)
