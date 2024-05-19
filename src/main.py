import os
import polars as pl
from src.utils.classes import ADSData
from pyarrow.parquet import SortingColumn
from src.utils.data_directory import INPUT_DATA

WRITE_EXCEL_REPORT: bool = True
PERIOD: int = 7  # days in a week
LOOKBACK_PERIODS: int = 3  # 3 weeks back
PARQUET_DESTINATION_PATH: os.path = os.path.join("outputs/parquet_partitioned/")
EXCEL_DESTINATION_PATH: os.path = os.path.join("outputs/excel/")


if __name__ == '__main__':
    ads_data: ADSData = ADSData(
        input_data=INPUT_DATA,
        period=PERIOD, lookback_periods=LOOKBACK_PERIODS
    )

    data: pl.DataFrame = ads_data.get_report()

    os.system(f"rm -rf {PARQUET_DESTINATION_PATH}")
    data.write_parquet(
        PARQUET_DESTINATION_PATH,
        use_pyarrow=True, pyarrow_options={
            "partition_cols": ["date"],
            "sorting_columns": [
                SortingColumn(3, True),  # columns: value_pro
                SortingColumn(4, True)  # columns: impressions
            ]
        }
    )

    if WRITE_EXCEL_REPORT:
        data.write_excel(EXCEL_DESTINATION_PATH + "report.xlsx")

    print("Check the 'outputs' folder to see the result files.")
