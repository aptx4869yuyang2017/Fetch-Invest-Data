
SELECT  monetaryfunds
       ,cash_deposit_pbc
       ,deposit_interbank
       ,precious_metal
       ,lend_fund
       ,trade_finasset
       ,derive_finasset
       ,buy_resale_finasset
       ,accounts_rece
       ,finance_rece
       ,interest_rece
       ,loan_advance
       ,note_rece
       ,dividend_rece
       ,export_refund_rece
       ,reinsure_rece
       ,rc_reserve_rece
       ,subsidy_rece
       ,prepayment
       ,inventory
       ,contract_asset
       ,div_holdsale_asset
       ,noncurrent_asset_1year
       ,other_rece
       ,other_current_asset
       ,current_asset_balance
       ,total_current_assets
       ,invest_rece
       ,settle_excess_reserve
       ,fin_fund
       ,premium_rece
       ,total_assets
       ,total_current_liab
FROM balance_sheets
WHERE report_date = '2024-12-31'
AND symbol = '000001'