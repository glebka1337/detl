import io
import boto3
import polars as pl
from urllib.parse import urlparse
from detl.connectors.base import Source, Sink
from detl.exceptions import ConnectionConfigurationError

class S3Source(Source):
    def __init__(self, s3_uri: str, format: str = "parquet", aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, endpoint_url: str | None = None):
        self.s3_uri = s3_uri
        self.format = format.lower()
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        
    def _get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        
    def read(self) -> pl.DataFrame | pl.LazyFrame:
        if not self.s3_uri.startswith("s3://"):
            raise ConnectionConfigurationError("S3 URI must start with s3://")
            
        parsed = urlparse(self.s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        s3 = self._get_client()
        try:
            # We strictly use boto3 to bypass any specific storage_option rust implementations
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()
            if self.format == "parquet":
                return pl.read_parquet(io.BytesIO(data))
            elif self.format == "csv":
                return pl.read_csv(io.BytesIO(data))
            else:
                raise ConnectionConfigurationError(f"Unsupported S3 format: {self.format}")
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to read from S3 via boto3 ({self.s3_uri}): {e}")

class S3Sink(Sink):
    def __init__(self, s3_uri: str, format: str = "parquet", aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, endpoint_url: str | None = None, streaming: bool = True):
        self.s3_uri = s3_uri
        self.format = format.lower()
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.streaming = streaming

    def _get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        if not self.s3_uri.startswith("s3://"):
            raise ConnectionConfigurationError("S3 URI must start with s3://")
            
        parsed = urlparse(self.s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        s3 = self._get_client()
        out = io.BytesIO()
        try:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()
                
            if self.format == "parquet":
                df.write_parquet(out)
            elif self.format == "csv":
                df.write_csv(out)
            else:
                raise ConnectionConfigurationError(f"Unsupported S3 format: {self.format}")
                
            out.seek(0)
            s3.upload_fileobj(out, bucket, key)
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to write to S3 via boto3 ({self.s3_uri}): {e}")
