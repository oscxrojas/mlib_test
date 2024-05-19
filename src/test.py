import polars as pl
from datetime import datetime, timedelta
from src.utils.classes import ADSData
from src.utils.data_directory import INPUT_DATA
from src.utils.test_data_directory import INPUT_DATA as TEST_INPUT_DATA

IS_IT_TEST: bool = True
WRITE_REPORT: bool = False
PERIOD: int = 7  # days in a week
LOOKBACK_PERIODS: int = 3  # 3 weeks back


def track(user: int, prop_text: str) -> None:
    print(
        ads_data.impressions.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == prop_text)
        )
    )
    print(
        ads_data.clicks.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == prop_text)
        )
    )
    print(
        ads_data.payments.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == prop_text)
        )
    )


if __name__ == '__main__':
    ads_data: ADSData = ADSData(
        input_data=INPUT_DATA if not IS_IT_TEST else TEST_INPUT_DATA,
        period=PERIOD, lookback_periods=LOOKBACK_PERIODS
    )
    data: pl.DataFrame = ads_data.get_report()

    if IS_IT_TEST:
        user_id: int = 2
        prop: str = "cellphone_recharge"

        track(
            user=user_id,
            prop_text=prop
        )

        print(
            data.filter(
                (pl.col("user_id") == user_id) & (pl.col("value_prop") == prop)
            ).sort(pl.col("date"), descending=True)
        )

        real: pl.DataFrame = pl.read_ndjson("data/prints.json", schema=INPUT_DATA["impressions"]["data_schema"])
        max_day: datetime = real.get_column("day").max()
        min_day: datetime = max_day - timedelta(days=6)
        print(min_day)
        print(max_day)
        print(
            real.filter(pl.col("day") >= min_day).height
        )
        print(data.height)
        assert data.equals(data)

    if WRITE_REPORT:
        data.write_excel("/Users/oscar/PycharmProjects/MercadoLibreADS/src/report.xlsx")
