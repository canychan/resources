import pytest
from unittest.mock import MagicMock, patch
import ibm_db
import common
from DbMgr import DbMgr

@pytest.fixture
def mock_db_conn():
    with patch('ibm_db.connect') as mock_connect, \
         patch('ibm_db.exec_immediate') as mock_exec_immediate, \
         patch('ibm_db.fetch_assoc') as mock_fetch_assoc, \
         patch('ibm_db.close') as mock_close:
        
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_result = MagicMock()
        mock_exec_immediate.return_value = mock_result

        mock_fetch_assoc.side_effect = [
            {'column1': 'value1', 'column2': 'value2'},
            None
        ]

        yield mock_connect, mock_exec_immediate, mock_fetch_assoc, mock_close

def test_execute_success(mock_db_conn):
    mock_connect, mock_exec_immediate, mock_fetch_assoc, mock_close = mock_db_conn

    common.db_exe_max_tries = 3
    common.db_retry_interval = 1
    common.db_connect_str = "dummy_connection_string"

    db_mgr = DbMgr(common.db_connect_str)

    sql = "SELECT * FROM dummy_table"
    result = db_mgr.execute(sql)

    assert result == [{'column1': 'value1', 'column2': 'value2'}]

    mock_connect.assert_called_once_with(common.db_connect_str, "", "")
    mock_exec_immediate.assert_called_once_with(mock_connect.return_value, sql)
    mock_fetch_assoc.assert_called_with(mock_exec_immediate.return_value)
    mock_close.assert_called_once_with(mock_connect.return_value)

def test_execute_failure(mock_db_conn):
    mock_connect, mock_exec_immediate, mock_fetch_assoc, mock_close = mock_db_conn

    common.db_exe_max_tries = 3
    common.db_retry_interval = 1
    common.db_connect_str = "dummy_connection_string"

    mock_connect.side_effect = Exception("Connection failed")

    db_mgr = DbMgr(common.db_connect_str)

    sql = "SELECT * FROM dummy_table"
    result = db_mgr.execute(sql)

    assert result is None

    assert mock_connect.call_count == common.db_exe_max_tries
    mock_close.assert_called_with(mock_connect.return_value)
