import pytest
from detl.schema import Manifesto
from pydantic import ValidationError

def test_empty_separator():
    yaml_dict = {
        "columns": {
            "role": {
                "dtype": "string",
                "constraints": {
                    "allowed_values": {
                        "source": "valid.csv",
                        "separator": "",
                        "violate_action": {"tactic": "drop_row"}
                    }
                }
            }
        }
    }
    with pytest.raises(ValidationError, match="Separator cannot be empty"):
        Manifesto(**yaml_dict)

def test_npy_separator_vandalism():
    yaml_dict = {
        "columns": {
            "role": {
                "dtype": "string",
                "constraints": {
                    "allowed_values": {
                        "source": "valid.npy",
                        "separator": ",",
                        "violate_action": {"tactic": "drop_row"}
                    }
                }
            }
        }
    }
    with pytest.raises(ValidationError, match="Cannot specify a 'separator' when using a .npy source file"):
        Manifesto(**yaml_dict)

def test_valid_csv_separator():
    yaml_dict = {
        "columns": {
            "role": {
                "dtype": "string",
                "constraints": {
                    "allowed_values": {
                        "source": "valid.csv",
                        "separator": ";",
                        "violate_action": {"tactic": "drop_row"}
                    }
                }
            }
        }
    }
    Manifesto(**yaml_dict) # Should not raise
