"""Schema enforcement for project."""
import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index

item_skeleton_schema = pa.DataFrameSchema(
    columns={
        # "non_zero_column": pa.Column(pa.Float, pa.Check.greater_than(0)),
        # "categorical_column": pa.Column(pa.String, pa.Check.isin(["value_1", "value_2"])),
        "min_holding": Column(pa.Int),
        "max_holding": Column(pa.Int),
        "max_sell": Column(pa.Int),
        "Buy": Column(pa.Int),
        "Sell": Column(pa.Int),
        "made_from": Column(pa.Bool),
        "make_pass": Column(pa.Int),
        "vendor_price": Column(pa.Int),
        "std_holding": Column(pa.Float),
        "mean_holding": Column(pa.Int),
    }
)
