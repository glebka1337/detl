from pathlib import Path
from pydantic import ValidationError
from typing import Union, Dict, Any

from detl.schema.core import Manifesto
from detl.exceptions import ConfigError

class Config:
    """
    Pythonic Interface for declarative ETL Data Contracts.
    Wraps the underlying Pydantic `Manifesto`.
    """
    def __init__(self, spec: Union[str, Path, Dict[str, Any]]):
        self._manifest: Manifesto
        
        import yaml
        try:
            if isinstance(spec, (str, Path)):
                with open(spec, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                if data is None:
                    raise ConfigError(f"YAML file '{spec}' is empty or invalid.")
                if not isinstance(data, dict):
                    raise ConfigError(f"YAML file '{spec}' must resolve to a valid mapping (dictionary). Got: {type(data)}")
                self._manifest = Manifesto.model_validate(data)
            elif isinstance(spec, dict):
                self._manifest = Manifesto.model_validate(spec)
            else:
                raise ConfigError(f"Config must be initialized with a filepath (str/Path) or a dictionary. Got: {type(spec)}")
        except yaml.YAMLError as e:
            raise ConfigError(f"Failed to parse YAML syntax in '{spec}': {e}")
        except ValidationError as e:
            messages = []
            for err in e.errors():
                loc = ".".join([str(x) for x in err.get('loc', [])])
                if not loc:
                    messages.append(f"  - {{Root/Unknown}}: {err.get('msg', 'Unknown error')} (Input: {err.get('input', 'N/A')})")
                else:
                    messages.append(f"  - {{{loc}}}: {err.get('msg', 'Unknown error')}")
            err_msg = "\n".join(messages)
            raise ConfigError(f"Invalid Data Contract YAML:\n{err_msg}")

    @property
    def manifest(self) -> Manifesto:
        return self._manifest
