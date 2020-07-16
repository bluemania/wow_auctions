# TIMINGS AND RESETS

# %%timeit
# generate_auction_scandata(verbose=True, test=True)
# #1.75 s ± 40.7 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# generate_auction_activity(verbose=True, test=True)
# 2.44 s ± 126 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# generate_inventory(verbose=True, test=True)
# #809 ms ± 29.2 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# analyse_item_prices(verbose=True, test=True)
# #1.62 s ± 30.4 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# analyse_sales_performance(test=True)
# #1.75 s ± 40.7 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# analyse_item_min_sell_price(test=True)
# #1.75 s ± 40.7 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# analyse_sell_data(test=True)
# #289 ms ± 7.02 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# apply_buy_policy(additional_percent=1.05, test=True)
# 2.79 s ± 82.1 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

# %%timeit
# apply_sell_policy(sale_number=2, stack_size=5, duration='medium', test=True)
# #2.31 s ± 15 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)


# Uncomment below for
# full scandata reset

# # # ensure cols are same as those used in clean
# cols = ["timestamp", "item", "count", "price", "agent", "price_per"]
# auction_scandata_reset = pd.DataFrame(columns=cols)
# auction_scandata_reset.to_parquet('full/auction_scandata.parquet', compression='gzip')

# Uncomment above for
# full scandata reset


# Uncomment below for
# full scandata reset

# cols = ['monies', 'timestamp']
# auction_scandata_reset = pd.DataFrame(columns=cols)
# auction_scandata_reset.to_parquet('full/monies.parquet', compression='gzip')

# cols = ['character', 'location', 'item', 'count', 'timestamp']
# auction_scandata_reset = pd.DataFrame(columns=cols)
# auction_scandata_reset.to_parquet('full/inventory.parquet', compression='gzip')

# Uncomment above for
# full scandata reset
