import pandas as pd
from openpyxl import load_workbook
import re
import pandas as pd
from datetime import datetime
from excel_processor import ExcelProcessor
from datetime import datetime
class ExcelProcessor:
    def __init__(self):
        self.workbooks = {}  # store sheet data
        self.files = []      # track loaded files

    def load_files(self, file_paths):
        for path in file_paths:
            try:
                xls = pd.ExcelFile(path)
                self.workbooks[path] = xls
                self.files.append(path)
                print(f"Loaded: {path}")
            except Exception as e:
                print(f"Failed to load {path}: {e}")

    def get_sheet_info(self):
        for file, xls in self.workbooks.items():
            print(f"\nFile: {file}")
            print("Sheets:", xls.sheet_names)
            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet)
                    print(f"  Sheet: {sheet} -> Rows: {df.shape[0]}, Columns: {df.shape[1]}")
                    print("   Columns:", list(df.columns))
                except Exception as e:
                    print(f"    Error loading sheet {sheet}: {e}")

    def preview_data(self, file, sheet, rows=5):
        try:
            df = self.workbooks[file].parse(sheet)
            print(df.head(rows))
        except Exception as e:
            print(f"Error previewing data from {file}, sheet {sheet}: {e}")


class DataTypeDetector:
    def __init__(self):
        pass

    def detect_column_type(self, column):
        non_null = column.dropna()

        if non_null.empty:
            return "Unknown"

        sample = non_null.astype(str).sample(min(10, len(non_null)))

        # Try detecting dates
        if self._is_date(sample):
            return "Date"

        # Try detecting numbers
        if self._is_number(sample):
            return "Number"

        return "String"

    def _is_date(self, sample):
        date_formats = [
            "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
            "%d-%b-%Y", "%b %Y", "%d-%m-%Y"
        ]

        for val in sample:
            try:
                if val.isdigit() and int(val) > 40000:
                    # Excel serial date (rough threshold)
                    continue
                for fmt in date_formats:
                    try:
                        datetime.strptime(val.strip(), fmt)
                        return True
                    except:
                        pass
            except:
                continue
        return False

    def _is_number(self, sample):
        number_regex = re.compile(r'^-?\(?[\$€₹]?\s?[\d,]+(\.\d+)?\)?[MBKmbk]?$')

        for val in sample:
            val = val.replace(",", "").replace(" ", "")
            if number_regex.match(val) or re.match(r"^-?\d+(\.\d+)?$", val):
                continue
            else:
                return False
        return True


if __name__ == "__main__":
    from excel_processor import ExcelProcessor
    processor = ExcelProcessor()
    detector = DataTypeDetector()
    
    file_paths = [
        r"C:\Users\PMLS\Desktop\sample\KH_Bank.XLSX",
        r"C:\Users\PMLS\Desktop\sample\Customer_Ledger_Entries_FULL.xlsx"
    ]
    
    processor.load_files(file_paths)
    processor.get_sheet_info()

    # Optional: preview a sheet
    processor.preview_data(file_paths[0], 'Sheet1')  # change 'Sheet1' as needed
    processor.load_files(file_paths)

    for file in file_paths:
        print(f"\nAnalyzing file: {file}")
        xls = processor.workbooks[file]
        for sheet in xls.sheet_names:
            print(f"  Sheet: {sheet}")
            df = xls.parse(sheet)
            for col in df.columns:
                col_type = detector.detect_column_type(df[col])
                print(f"    Column: {col} -> Detected Type: {col_type}")