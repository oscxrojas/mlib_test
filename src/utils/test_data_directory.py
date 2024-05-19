from polars import datatypes

INPUT_DATA = {
    "impressions": {
        "file_path": "input/for_testing_purposes/prints.json",
        "file_extension": "jsonl",
        "data_schema": {
            "day": datatypes.Date,
            "event_data": datatypes.Struct([
                datatypes.Field("position", datatypes.Int64), datatypes.Field("value_prop", datatypes.String)
            ]),
            "user_id": datatypes.Int64
        },
        "unnest_columns": True,
        "columns_to_rename": {"day": "date"},
        "group_by_columns": ["date", "user_id", "value_prop"]
    },
    "clicks": {
        "file_path": "input/for_testing_purposes/taps.json",
        "file_extension": "jsonl",
        "data_schema": {
            "day": datatypes.Date,
            "event_data": datatypes.Struct([
                datatypes.Field("position", datatypes.Int64), datatypes.Field("value_prop", datatypes.String)
            ]),
            "user_id": datatypes.Int64
        },
        "unnest_columns": True,
        "columns_to_rename": {"day": "date"},
        "group_by_columns": ["date", "user_id", "value_prop"]
    },
    "payments": {
        "file_path": "input/for_testing_purposes/pays.csv",
        "file_extension": "csv",
        "data_schema": {
            "pay_date": datatypes.Date,
            "total": datatypes.Decimal,
            "user_id": datatypes.Int64,
            "value_prop": datatypes.String
        },
        "columns_to_rename": {"pay_date": "date"},
        "group_by_columns": ["date", "user_id", "value_prop"]
    }
}
