"""Unit test cho db_utils — phần thuần Python, không cần DB."""
import pytest

from db_utils import jdbc_to_psycopg_params


class TestJdbcToPsycopgParams:
    def test_parse_standard_jdbc_url(self):
        params = jdbc_to_psycopg_params("jdbc:postgresql://postgres-db:5432/sales_db")
        assert params == {"host": "postgres-db", "port": 5432, "dbname": "sales_db"}

    def test_parse_localhost_url(self):
        params = jdbc_to_psycopg_params("jdbc:postgresql://localhost:5433/sales_db")
        assert params == {"host": "localhost", "port": 5433, "dbname": "sales_db"}

    def test_wrong_scheme_raises_value_error(self):
        with pytest.raises(ValueError):
            jdbc_to_psycopg_params("mysql://localhost:3306/db")

    def test_missing_port_raises_value_error(self):
        with pytest.raises(ValueError):
            jdbc_to_psycopg_params("jdbc:postgresql://postgres-db/sales_db")

    def test_empty_url_raises_value_error(self):
        with pytest.raises(ValueError):
            jdbc_to_psycopg_params("")
