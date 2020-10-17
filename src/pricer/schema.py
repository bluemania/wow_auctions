"""Schema enforcement for project."""
import pandera as pa
from pandera import Column, Index

item_skeleton_raw_schema = pa.DataFrameSchema(
    columns={
        "user_min_holding": Column(pa.Int, nullable=True),
        "user_max_holding": Column(pa.Int, nullable=True),
        "user_max_sell": Column(pa.Int, nullable=True),
        "user_Buy": Column(nullable=True),
        "user_Sell": Column(nullable=True),
        "user_Make": Column(nullable=True),
        "user_made_from": Column(pa.Object, nullable=True),
        "user_make_pass": Column(pa.Int, nullable=True),
        "user_vendor_price": Column(pa.Int, nullable=True),
    },
    strict=True,
    index=Index(pa.String),
)

item_skeleton_schema = pa.DataFrameSchema(
    columns={
        "user_min_holding": Column(pa.Int),
        "user_max_holding": Column(pa.Int),
        "user_max_sell": Column(pa.Int, nullable=True),
        "user_Buy": Column(pa.Int),
        "user_Sell": Column(pa.Int),
        "user_Make": Column(pa.Int),
        "user_make_pass": Column(pa.Int),
        "user_vendor_price": Column(pa.Int),
        "user_std_holding": Column(pa.Float),
        "user_mean_holding": Column(pa.Int),
    },
    strict=True,
    index=Index(pa.String),
)

auc_listings_raw_schema = pa.DataFrameSchema(
    columns={
        0: Column(pa.String, nullable=True),
        1: Column(pa.String, nullable=True),
        2: Column(pa.String, nullable=True),
        3: Column(pa.String, nullable=True),
        4: Column(pa.String, nullable=True),
        5: Column(pa.String, nullable=True),
        6: Column(pa.String, nullable=True),
        7: Column(pa.String, nullable=True),
        8: Column(pa.String, nullable=True),
        9: Column(pa.String, nullable=True),
        10: Column(pa.String, nullable=True),
        11: Column(pa.String, nullable=True),
        12: Column(pa.String, nullable=True),
        13: Column(pa.String, nullable=True),
        14: Column(pa.String, nullable=True),
        15: Column(pa.String, nullable=True),
        16: Column(pa.String, nullable=True),
        17: Column(pa.String, nullable=True),
        18: Column(pa.String, nullable=True),
        19: Column(pa.String, nullable=True),
        20: Column(pa.String, nullable=True),
        21: Column(pa.String, nullable=True),
        22: Column(pa.String, nullable=True),
        23: Column(pa.String, nullable=True),
        24: Column(pa.String, nullable=True),
        25: Column(pa.String, nullable=True),
        26: Column(pa.String, nullable=True),
        27: Column(pa.String, nullable=True),
    },
    strict=True,    
)

auc_listings_schema = pa.DataFrameSchema(
    columns={
        "item": Column(pa.String),
        "quantity": Column(pa.Int),
        "buy": Column(pa.Int),
        "sellername": Column(pa.String),
        "price_per": Column(pa.Int),
        "time_remaining": Column(pa.Int),
    },
    strict=True,    
)

beancounter_data_raw_schema = pa.DataFrameSchema(
    columns={
        0: Column(pa.String, nullable=True),
        1: Column(pa.String, nullable=True),
        2: Column(pa.String, nullable=True),
        3: Column(pa.String, nullable=True),
        4: Column(pa.String, nullable=True),
        5: Column(pa.String, nullable=True),
        6: Column(pa.String, nullable=True),
        7: Column(pa.String, nullable=True),
        8: Column(pa.String, nullable=True),
        9: Column(pa.String, nullable=True),
        10: Column(pa.String, nullable=True),
        11: Column(pa.String, nullable=True),
        12: Column(pa.String, nullable=True),
    },
    strict=True,    
)

beancounter_purchases_schema = pa.DataFrameSchema(
    columns={
        "auction_type": Column(pa.String),
        "item": Column(pa.String),
        "buyer": Column(pa.String),
        "qty": Column(pa.Int),
        "buyout": Column(pa.Int, nullable=True),
        "bid": Column(pa.Int),
        "seller": Column(pa.String),
        "timestamp": Column(pa.DateTime),
        "buyout_per": Column(pa.Float),
        "bid_per": Column(pa.Float),
    },
    strict=True,    
)

beancounter_posted_schema = pa.DataFrameSchema(
    columns={
        "auction_type": Column(pa.String),
        "item": Column(pa.String),
        "seller": Column(pa.String),
        "qty": Column(pa.Int),
        "deposit": Column(pa.Int),
        "buyout": Column(pa.Int, nullable=True),
        "bid": Column(pa.Int),
        "timestamp": Column(pa.DateTime),
        "buyout_per": Column(pa.Float),
        "bid_per": Column(pa.Float),
    },
    strict=True,    
)

beancounter_failed_schema = pa.DataFrameSchema(
    columns={
        "auction_type": Column(pa.String),
        "item": Column(pa.String),
        "seller": Column(pa.String),
        "qty": Column(pa.Int),
        "deposit": Column(pa.Int),
        "buyout": Column(pa.Int, nullable=True),
        "bid": Column(pa.Int),
        "timestamp": Column(pa.DateTime),
        "buyout_per": Column(pa.Float),
        "bid_per": Column(pa.Float),
    },
    strict=True,    
)

beancounter_success_schema = pa.DataFrameSchema(
    columns={
        "auction_type": Column(pa.String),
        "item": Column(pa.String),
        "seller": Column(pa.String),
        "qty": Column(pa.Int),
        "received": Column(pa.Int),
        "deposit": Column(pa.Int),
        "ah_cut": Column(pa.Int),
        "buyout": Column(pa.Int, nullable=True),
        "bid": Column(pa.Int),
        "buyer": Column(pa.String),
        "timestamp": Column(pa.DateTime),
        "received_per": Column(pa.Float),
        "buyout_per": Column(pa.Float),
        "bid_per": Column(pa.Float),
    },
    strict=True,    
)

beancounter_results_schema = schema.add_columns({
    "success": pa.Column(pa.Int)
    }
)

ark_inventory_schema = pa.DataFrameSchema(
    columns={
        "character": Column(pa.String),
        "location": Column(pa.String),
        "item": Column(pa.String),
        "count": Column(pa.Int),
        "timestamp": Column(pa.DateTime)
    },
    strict=True,    
)

ark_money_schema = pa.DataFrameSchema(
    columns={
        "monies": Column(pa.Int),
        "timestamp": Column(pa.DateTime)
    },
    strict=True,    
)
