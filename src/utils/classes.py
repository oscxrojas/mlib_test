import polars as pl
from typing import Union
from datetime import datetime, timedelta


class ADSData:
    """
    A class to handle advertising data operations.

    Attributes
    ----------
    impressions : pl.DataFrame
        DataFrame containing impression data.
    clicks : pl.DataFrame
        DataFrame containing click data.
    payments : pl.DataFrame
        DataFrame containing payment data.
    data : pl.DataFrame
        DataFrame containing joined data from impressions, clicks, and payments.

    Methods
    -------
    _read_file(**kwargs) -> pl.DataFrame
        Reads data from a file and returns it as a DataFrame.
    """

    def __init__(
            self,
            input_data: dict[str, dict[str, Union[str, dict[str, pl.datatypes], list[str]]]],
            period: int,
            lookback_periods: int
    ) -> None:
        """
        Initializes the ADSData object with input data and periods.

        Parameters
        ----------
        input_data : dict
            Dictionary containing the input data configuration.
        period : int
            The current period for the data analysis.
        lookback_periods : int
            The number of lookback periods for historical data analysis.
        """

        self._input_data = input_data
        self._period: int = period
        self._lookback_periods: int = lookback_periods

        all_datasets: dict[str, pl.LazyFrame] = self._get_data()
        self.impressions: pl.LazyFrame = all_datasets["impressions"]
        self.clicks: pl.LazyFrame = all_datasets["clicks"]
        self.payments: pl.LazyFrame = all_datasets["payments"]

        self.data: pl.LazyFrame = self._join_data()

    @staticmethod
    def _read_file(**kwargs) -> pl.LazyFrame:
        """
        Reads data from a file and returns it as a LazyFrame.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments for file reading:
                - file_extension : str
                    The file extension (e.g., 'jsonl', 'csv').
                - file_path : str
                    The path to the file.
                - data_schema : dict
                    Schema for the data.
                - unnest_columns : bool
                    Whether to unnest columns with structured data.

        Returns
        -------
        pl.LazyFrame
            LazyFrame containing the read data.

        Raises
        ------
        Exception
            If the file format is not supported.
        """

        if kwargs["file_extension"] == 'jsonl':
            data: pl.LazyFrame = pl.scan_ndjson(kwargs["file_path"], schema=kwargs["data_schema"])

            if kwargs["unnest_columns"]:
                columns: list[str] = [key for key, value in kwargs["data_schema"].items() if isinstance(value, pl.Struct)]

                for column in columns:
                    data = data.unnest(column)

            return data

        elif kwargs["file_extension"] == 'csv':
            data: pl.LazyFrame = pl.scan_csv(kwargs["file_path"]).cast(kwargs["data_schema"])

            return data

        else:
            raise Exception(f"The file format {kwargs["file_extension"]} is not supported.")

    def _get_data(self) -> dict[str, pl.LazyFrame]:
        """
        Reads and processes input data to create DataFrames for impressions, clicks, and payments.

        Returns
        -------
        dict
            A dictionary containing DataFrames for impressions, clicks, and payments.
        """

        impressions: pl.LazyFrame = self._read_file(**self._input_data["impressions"]).rename(
            self._input_data["impressions"]["columns_to_rename"]
        ).group_by(self._input_data["impressions"]["group_by_columns"]).agg(pl.len().alias("impressions"))

        clicks: pl.LazyFrame = self._read_file(**self._input_data["clicks"]).rename(
            self._input_data["clicks"]["columns_to_rename"]
        ).group_by(self._input_data["clicks"]["group_by_columns"]).agg(pl.len().alias("clicks"))

        payments: pl.LazyFrame = self._read_file(**self._input_data["payments"]).rename(
            self._input_data["payments"]["columns_to_rename"]
        ).group_by(self._input_data["payments"]["group_by_columns"]).agg(
            (pl.len().alias("payments_qty")),
            (pl.col("total").sum().alias("total"))
        )

        result: dict[str, pl.LazyFrame] = {
            "impressions": impressions,
            "clicks": clicks,
            "payments": payments
        }

        return result

    def _join_data(self) -> pl.LazyFrame:
        """
        Joins the impressions, clicks, and payments DataFrames on common columns.

        Returns
        -------
        pl.DataFrame
            A DataFrame with joined data from impressions, clicks, and payments, with null values filled with 0.
        """

        data: pl.LazyFrame = self.impressions.join(
            self.clicks,
            on=["date", "user_id", "value_prop"],
            how="left"
        ).join(
            self.payments,
            on=["date", "user_id", "value_prop"],
            how="left"
        ).fill_null(0)

        return data

    def get_stat(self, dataset: str, stat: str) -> datetime:
        """
        Retrieves a specified statistic from a dataset.

        Parameters
        ----------
        dataset : str
            The name of the dataset attribute (e.g., 'impressions', 'clicks', 'payments').
        stat : str
            The name of the statistic method to apply (e.g., 'min', 'max').

        Returns
        -------
        datetime
            The result of the statistic method applied to the 'date' column of the specified dataset.
        """

        df: pl.DataFrame = (eval(f"self.{dataset}")).collect()
        return getattr(df.get_column('date'), stat)()

    def get_report(self, filter_output: bool = True) -> pl.DataFrame:
        """
        Generates a report by aggregating data over a specified lookback period and optionally filtering the output.

        Parameters
        ----------
        filter_output : bool, optional
            If True, filters the output to include data only from the current period (default is True).

        Returns
        -------
        pl.DataFrame
            A DataFrame containing the report with aggregated data over the lookback period.
        """

        lookback_days: int = self._period * self._lookback_periods

        data: pl.DataFrame = self.data.join(
            self.data,
            on=["user_id", "value_prop"],
            how="left"
        ).filter(
            (pl.col("date_right") >= (pl.col("date") - timedelta(days=lookback_days))) &
            (pl.col("date_right") < pl.col("date"))
        ).group_by(
            ["date", "user_id", "value_prop"]
        ).agg(
            (pl.col("impressions_right").sum().alias(f"impressions_prev_{lookback_days}_days")),
            (pl.col("clicks_right").sum().alias(f"clicks_prev_{lookback_days}_days")),
            (pl.col("payments_qty_right").sum().alias(f"payments_prev_{lookback_days}_days")),
            (pl.col("total_right").sum().alias(f"total_prev_{lookback_days}_days")),
        ).collect()

        base: pl.DataFrame = self.data.collect()

        if filter_output:
            max_date: datetime = self.get_stat(dataset="impressions", stat="max")

            base = base.filter(
                pl.col("date") > (max_date - timedelta(days=self._period))
            )

        base = base.join(
            data,
            on=["date", "user_id", "value_prop"],
            how="left"
        ).select(
            [
                "date", "user_id", "value_prop", "impressions", "clicks", "payments_qty", "total",
                "impressions_prev_21_days", "clicks_prev_21_days", "payments_prev_21_days", "total_prev_21_days"
            ]
        ).fill_null(0)

        return base
