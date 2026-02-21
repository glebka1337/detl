from enum import Enum

class DType(str, Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"

class NullTactic(str, Enum):
    DROP_ROW = "drop_row"
    FAIL = "fail"
    FILL_VALUE = "fill_value"
    FILL_MEAN = "fill_mean"
    FILL_MEDIAN = "fill_median"
    FILL_MOST_FREQUENT = "fill_most_frequent"
    FFILL = "ffill"
    BFILL = "bfill"

class DupTactic(str, Enum):
    DROP_EXTRAS = "drop_extras"
    FAIL = "fail"
    KEEP = "keep"

class StringActionTactic(str, Enum):
    DROP_ROW = "drop_row"
    FAIL = "fail"
    FILL_VALUE = "fill_value"

class NumericActionTactic(str, Enum):
    DROP_ROW = "drop_row"
    FAIL = "fail"
    FILL_VALUE = "fill_value"
    FILL_MAX = "fill_max"
    FILL_MIN = "fill_min"
    FILL_MEAN = "fill_mean"
    FILL_MEDIAN = "fill_median"
