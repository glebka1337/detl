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
        
        try:
            if isinstance(spec, (str, Path)):
                self._manifest = Manifesto.parse_file(spec)
            elif isinstance(spec, dict):
                self._manifest = Manifesto(**spec)
            else:
                raise ConfigError(f"Config must be initialized with a filepath (str/Path) or a dictionary. Got: {type(spec)}")
        except ValidationError as e:
            messages = []
            for err in e.errors():
                loc = ".".join([str(x) for x in err['loc']])
                messages.append(f"  - [{loc}]: {err['msg']}")
            err_msg = "\n".join(messages)
            raise ConfigError(f"Invalid Data Contract YAML:\n{err_msg}")

    @property
    def manifest(self) -> Manifesto:
        return self._manifest
