from detl.config import Config

def test_coerce_dup_rows_string():
    yaml_dict = {
        "conf": {
            "on_duplicate_rows": "drop_extras"
        },
        "columns": {}
    }
    config = Config(yaml_dict)
    assert config.manifest.conf.on_duplicate_rows.tactic == "drop_extras"
    
def test_coerce_dup_rows_dict():
    yaml_dict = {
        "conf": {
            "on_duplicate_rows": {
                "tactic": "fail",
                "subset": ["id"]
            }
        },
        "columns": {}
    }
    config = Config(yaml_dict)
    assert config.manifest.conf.on_duplicate_rows.tactic == "fail"
    assert config.manifest.conf.on_duplicate_rows.subset == ["id"]
