# Databricks notebook source
import os
import pytest
from unittest.mock import MagicMock, patch

def get_file_extension(file_path: str) -> str:
    return file_path.split('.')[-1].lower()

def sanitize_column_name(col_name: str) -> str:
    return (col_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "_"))

def derive_table_name(file_name: str, target_database: str = "raw.default") -> str:
    file_stem = os.path.splitext(file_name)[0].lower().replace("-", "_").replace(" ", "_")
    return f"{target_database}.{file_stem}"


@pytest.mark.parametrize("file_path,expected_ext", [
    ("dataset/Products.csv", "csv"),
    ("dataset/Customer.XLSX", "xlsx"),
    ("dataset/Orders.json", "json"),
])
def test_file_extension_parsing(file_path, expected_ext):
    assert get_file_extension(file_path) == expected_ext


@pytest.mark.parametrize("input_col,expected_col", [
    ("Customer ID", "customer_id"),
    ("Product Name", "product_name"),
    ("Price (per unit).", "price_per_unit_"),
    ("Ship-Mode", "ship_mode"),
])
def test_column_name_sanitization(input_col, expected_col):
    assert sanitize_column_name(input_col) == expected_col


@pytest.mark.parametrize("file_name,target_db,expected_table", [
    ("Orders.json", "raw.default", "raw.default.orders"),
    ("Customer-Details.xlsx", "bronze.default", "bronze.default.customer_details"),
    ("Products List.csv", "raw.default", "raw.default.products_list"),
])
def test_derive_table_name(file_name, target_db, expected_table):
    assert derive_table_name(file_name, target_db) == expected_table


def test_list_files_recursive_mock():
    mock_item_file = MagicMock()
    mock_item_file.isDir.return_value = False
    mock_item_file.path = "dbfs:/dataset/Products.csv"

    mock_item_dir = MagicMock()
    mock_item_dir.isDir.return_value = True
    mock_item_dir.path = "dbfs:/dataset/subfolder"

    mock_sub_file = MagicMock()
    mock_sub_file.isDir.return_value = False
    mock_sub_file.path = "dbfs:/dataset/subfolder/Orders.json"

    def mock_ls_side_effect(path):
        if path == "dbfs:/dataset":
            return [mock_item_file, mock_item_dir]
        elif path == "dbfs:/dataset/subfolder":
            return [mock_sub_file]
        return []

    with patch("builtins.dbutils", create=True) as mock_dbutils:
        mock_dbutils.fs.ls.side_effect = mock_ls_side_effect
        
        def list_files_recursive(path: str):
            all_files = []
            for item in mock_dbutils.fs.ls(path):
                if item.isDir():
                    all_files.extend(list_files_recursive(item.path))
                else:
                    all_files.append(item.path)
            return all_files

        files = list_files_recursive("dbfs:/dataset")
        
        assert len(files) == 2
        assert "dbfs:/dataset/Products.csv" in files
        assert "dbfs:/dataset/subfolder/Orders.json" in files

    
    print("All unit tests Completeed successfully.")

# Execute the test suite
run_unit_tests()
