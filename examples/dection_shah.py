import pandas as pd
import openpyxl
import re
from datetime import datetime


# sTeep 1

class ExcelProcessor:
    def __init__(self):
        self.workbooks = {}

    def load_files(self, file_paths):
        for path in file_paths:
            try:
                xls = pd.ExcelFile(path)
                self.workbooks[path] = xls
                print(f"Loaded: {path}")
            except Exception as e:
                print(f"Failed to load {path}: {e}")

    def get_sheet_info(self):
        for path, xls in self.workbooks.items():
            print(f"\nFile: {path}")
            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet)
                    print(f"Sheet: {sheet} | Rows: {df.shape[0]}, Columns: {df.shape[1]}")
                except Exception as e:
                    print(f" Could not read {sheet}: {e}")

    def read_sheet(self, file_path, sheet_name):
        return self.workbooks[file_path].parse(sheet_name)


# sleep2  Data Type Detector

class DataTypeDetector:
    def detect_column_type(self, column):
        non_null = column.dropna()
        if non_null.empty:
            return "Unknown"

        sample = non_null.astype(str).sample(min(10, len(non_null)))

        if self._is_date(sample):
            return "Date"
        if self._is_number(sample):
            return "Number"
        return "String"

    def _is_date(self, sample):
        for val in sample:
            try:
                if val.isdigit() and 40000 <= int(val) <= 50000:
                    return True
                datetime.strptime(val, "%Y-%m-%d")
                return True
            except:
                continue
        return False

    def _is_number(self, sample):
        pattern = re.compile(r'^-?\(?[\$€₹]?\s?[\d,]+(\.\d+)?\)?[MBKmbk]?$')
        for val in sample:
            val = val.replace(",", "").replace(" ", "")
            if pattern.match(val) or re.match(r"^-?\d+(\.\d+)?$", val):
                continue
            else:
                return False
        return True


# Step 3 Format Parser 

class FormatParser:
    def parse_amount(self, value):
        if pd.isnull(value):
            return None
        value = str(value).strip()

        if value.startswith("(") and value.endswith(")"):
            value = "-" + value[1:-1]
        if value.endswith("-"):
            value = "-" + value[:-1]

        value = value.replace("$", "").replace("€", "").replace("₹", "")
        value = value.replace(",", "").replace(" ", "")

        multiplier = 1
        if value.lower().endswith("k"):
            multiplier = 1_000
            value = value[:-1]
        elif value.lower().endswith("m"):
            multiplier = 1_000_000
            value = value[:-1]
        elif value.lower().endswith("b"):
            multiplier = 1_000_000_000
            value = value[:-1]

        try:
            return float(value) * multiplier
        except:
            return None

    def parse_date(self, value):
        if pd.isnull(value):
            return None

        if isinstance(value, (int, float)) and 40000 <= value <= 50000:
            try:
                return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(value) - 2)
            except Exception:
                return None

        value = str(value).strip()
        date_formats = [
            "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
            "%d-%b-%Y", "%b %Y", "%d-%m-%Y",
            "%b-%y", "%d %B %Y"
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(value, fmt)
            except:
                continue

        quarter_match = re.match(r'Q([1-4])[\s\-]?(\d{2,4})', value, re.IGNORECASE)
        if quarter_match:
            q, y = quarter_match.groups()
            y = int("20" + y) if len(y) == 2 else int(y)
            month = (int(q) - 1) * 3 + 1
            return datetime(y, month, 1)

        return None


# Steo 4: Data Storage -

class FinancialDataStore:
    def __init__(self):
        self.datasets = {}
        self.indexes = {}

    def add_dataset(self, name, df, column_types):
        self.datasets[name] = df
        self.indexes[name] = {}
        for col, dtype in column_types.items():
            if dtype in ["Date", "Number"]:
                try:
                    df = df.dropna(subset=[col])
                    self.indexes[name][col] = df.set_index(col).sort_index()
                except Exception as e:
                    print(f"Could not index {col}: {e}")

    def query_by_range(self, dataset, column, min_val, max_val):
        if dataset in self.indexes and column in self.indexes[dataset]:
            try:
                index_df = self.indexes[dataset][column]
                return index_df.loc[min_val:max_val]
            except Exception as e:
                print(f"Range query error: {e}")
        return pd.DataFrame()

    def aggregate(self, dataset, group_by, measure, agg_func='sum'):
        if dataset not in self.datasets:
            return pd.DataFrame()
        df = self.datasets[dataset]
        return df.groupby(group_by)[measure].agg(agg_func)


# syep 5: Integration 

def run_parser():
    files = [
        r"C:\Users\PMLS\Desktop\sample\KH_Bank.XLSX",
        r"C:\Users\PMLS\Desktop\sample\Customer_Ledger_Entries_FULL.xlsx"
    ]

    processor = ExcelProcessor()
    detector = DataTypeDetector()
    parser = FormatParser()
    storage = FinancialDataStore()

    processor.load_files(files)
    processor.get_sheet_info()

    for file in files:
        xls = processor.workbooks[file]
        for sheet in xls.sheet_names:
            print(f"\nProcessing {file} | Sheet: {sheet}")
            df = processor.read_sheet(file, sheet)
            col_types = {}

            for col in df.columns:
                try:
                    ctype = detector.detect_column_type(df[col])
                    col_types[col] = ctype

                    if ctype == "Number":
                        df[col] = df[col].apply(parser.parse_amount)
                    elif ctype == "Date":
                        df[col] = df[col].apply(parser.parse_date)

                    print(f" {col}: {ctype}")
                except Exception as e:
                    print(f"Failed to process column {col}: {e}")

            dataset_name = f"{file.split('\\')[-1]}_{sheet}"
            storage.add_dataset(dataset_name, df, col_types)

            # Query examples
            for c in col_types:
                if col_types[c] == "Date":
                    try:
                        result = storage.query_by_range(dataset_name, c, datetime(2023, 1, 1), datetime(2023, 12, 31))
                        print(f"Query Date Range on '{c}': {len(result)} rows")
                    except Exception as e:
                        print(f"Date query failed: {e}")
                elif col_types[c] == "Number":
                    try:
                        result = storage.query_by_range(dataset_name, c, 1000, 10000)
                        print(f"Query Amount Range on '{c}': {len(result)} rows")
                    except Exception as e:
                        print(f"Amount query failed: {e}")


if __name__ == "__main__":
    run_parser()
