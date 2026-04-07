import pytest
from unittest.mock import patch, MagicMock
from delphi.session import get_spark_session_with_retry
from delphi.config import DelphiConfig, ConnectionConfig


def test_retry_on_transient_error():
    config = DelphiConfig(
        connection=ConnectionConfig(host="https://x.com", cluster_id="c1", auth_type="pat", token="t"),
        connection_retries=3,
    )
    with patch("delphi.session.get_spark_session") as mock_get, \
         patch("delphi.session.time.sleep"):
        mock_get.side_effect = [ConnectionError("starting"), ConnectionError("starting"), MagicMock()]
        session = get_spark_session_with_retry(config)
        assert mock_get.call_count == 3


def test_no_retry_on_permission_error():
    config = DelphiConfig(
        connection=ConnectionConfig(host="https://x.com", cluster_id="c1", auth_type="pat", token="t"),
        connection_retries=3,
    )
    with patch("delphi.session.get_spark_session") as mock_get:
        mock_get.side_effect = PermissionError("access denied")
        with pytest.raises(PermissionError):
            get_spark_session_with_retry(config)
        assert mock_get.call_count == 1


def test_exhausted_retries_raises():
    config = DelphiConfig(
        connection=ConnectionConfig(host="https://x.com", cluster_id="c1", auth_type="pat", token="t"),
        connection_retries=2,
    )
    with patch("delphi.session.get_spark_session") as mock_get, \
         patch("delphi.session.time.sleep"):
        mock_get.side_effect = ConnectionError("down")
        with pytest.raises(ConnectionError):
            get_spark_session_with_retry(config)
        assert mock_get.call_count == 2
