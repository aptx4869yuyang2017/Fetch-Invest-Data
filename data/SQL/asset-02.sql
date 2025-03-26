
select 
creditor_invest
,other_creditor_invest
,other_equity_invest
,available_sale_finasset
,hold_maturity_invest
,long_equity_invest
,invest_subsidiary
,invest_joint
,invest_realestate
,fixed_asset
,cip
,project_material
,fixed_asset_disposal
,oil_gas_asset
,useright_asset
,intangible_asset
,develop_expense
,goodwill
,long_prepaid_expense
,defer_tax_asset
,other_noncurrent_finasset
,productive_biology_asset
,consumptive_biological_asset
,pend_mortgage_asset
,mortgage_asset_impairment
,net_pendmortgage_asset
,other_asset
,noncurrent_asset_balance
,total_noncurrent_assets
,fvtpl_finasset
,appoint_fvtpl_finasset
,trade_finasset_notfvtpl
,amortize_cost_finasset
,fvtoci_finasset
,long_rece
,internal_rece
,other_noncurrent_asset
,amortize_cost_ncfinasset
,fvtoci_ncfinasset
,agent_business_asset
,insurance_contract_reserve
,special_reserve
,unconfirm_invest_loss
,nvl(monetaryfunds,0)+ nvl(cash_deposit_pbc,0)+ nvl(deposit_interbank,0)+ nvl(precious_metal,0)+ nvl(lend_fund,0)+ nvl(trade_finasset,0)+ nvl(derive_finasset,0)+ nvl(buy_resale_finasset,0)+ nvl(accounts_rece,0)+ nvl(finance_rece,0)+ nvl(interest_rece,0)+ nvl(loan_advance,0)+ nvl(note_rece,0)+ nvl(dividend_rece,0)+ nvl(export_refund_rece,0)+ nvl(reinsure_rece,0)+ nvl(rc_reserve_rece,0)+ nvl(subsidy_rece,0)+ nvl(prepayment,0)+ nvl(inventory,0)+ nvl(contract_asset,0)+ nvl(div_holdsale_asset,0)+ nvl(noncurrent_asset_1year,0)+ nvl(other_rece,0)+ nvl(other_current_asset,0)+ nvl(current_asset_balance,0)+ nvl(total_current_assets,0)+ nvl(invest_rece,0)+ nvl(settle_excess_reserve,0)+ nvl(fin_fund,0)+ nvl(premium_rece,0)+ nvl(total_assets,0)+ nvl(total_current_liab,0) as  tt1
FROM balance_sheets
where report_date = '2024-12-31' and symbol = '000001'