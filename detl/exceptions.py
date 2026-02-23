class DetlException(Exception):
    """Base exception for all `detl` domain errors."""
    pass

class ConstraintViolationError(DetlException):
    """Raised when a data value violates a strict constraint policy configured with tactic 'fail'."""
    pass

class NullViolationError(DetlException):
    """Raised when a null/missing value is encountered but the column policy dictates tactic 'fail'."""
    pass

class DuplicateRowError(DetlException):
    """Raised when uniqueness checks fail and duplicate rows are detected under tactic 'fail'."""
    pass

class TypeCastingError(DetlException):
    """Raised when a column cannot be safely cast to the declared target DType."""
    pass

class ConfigError(DetlException):
    """Raised when the input configuration YAML or connection URI is malformed or invalid."""
    pass

class ConnectionConfigurationError(DetlException):
    """Raised when a Source or Sink Connector fails to establish a connection."""
    pass
