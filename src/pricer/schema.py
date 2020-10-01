"""Schema enforcement for project."""
import pandera as pa
from pandera import Column, Index

item_skeleton_raw_schema = pa.DataFrameSchema(
    columns={
        "min_holding": Column(pa.Int, nullable=True),
        "max_holding": Column(pa.Int, nullable=True),
        "max_sell": Column(pa.Int, nullable=True),
        "Buy": Column(pa.Bool, nullable=True, coerce=True),
        "Sell": Column(pa.Bool, nullable=True, coerce=True),
        "made_from": Column(pa.Object, nullable=True),
        "make_pass": Column(pa.Int, nullable=True),
        "vendor_price": Column(pa.Int, nullable=True),
    },
    strict=True,
    index=Index(pa.String),
)

item_skeleton_schema = pa.DataFrameSchema(
    columns={
        "min_holding": Column(pa.Int),
        "max_holding": Column(pa.Int),
        "max_sell": Column(pa.Int, nullable=True),
        "Buy": Column(pa.Int),
        "Sell": Column(pa.Int),
        "made_from": Column(pa.Bool),
        "make_pass": Column(pa.Int),
        "vendor_price": Column(pa.Int),
        "std_holding": Column(pa.Float),
        "mean_holding": Column(pa.Int),
    },
    strict=True,
    index=Index(pa.String),
)
