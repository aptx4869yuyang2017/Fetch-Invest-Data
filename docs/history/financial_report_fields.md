# 资产负债表字段说明文档

## get_balance_sheet 方法返回的字段说明

资产负债表主要分为资产、负债和所有者权益三大部分。以下是常见字段的中英文对照：

### 报告期相关
- `report_date`: 报告期日期

### 资产部分
- `total_assets`: 资产总计
- `current_assets`: 流动资产合计
- `cash_and_equivalents`: 货币资金
- `trading_assets`: 交易性金融资产
- `notes_receivable`: 应收票据
- `accounts_receivable`: 应收账款
- `prepayments`: 预付款项
- `other_receivables`: 其他应收款
- `inventories`: 存货
- `non_current_assets`: 非流动资产合计
- `fixed_assets`: 固定资产
- `intangible_assets`: 无形资产
- `goodwill`: 商誉
- `long_term_equity_investment`: 长期股权投资
- `investment_property`: 投资性房地产
- `deferred_tax_assets`: 递延所得税资产

### 负债部分
- `total_liabilities`: 负债合计
- `current_liabilities`: 流动负债合计
- `short_term_borrowings`: 短期借款
- `notes_payable`: 应付票据
- `accounts_payable`: 应付账款
- `advance_from_customers`: 预收款项
- `employee_benefits_payable`: 应付职工薪酬
- `taxes_payable`: 应交税费
- `other_payables`: 其他应付款
- `non_current_liabilities`: 非流动负债合计
- `long_term_borrowings`: 长期借款
- `bonds_payable`: 应付债券
- `deferred_tax_liabilities`: 递延所得税负债

### 所有者权益部分
- `total_shareholders_equity`: 所有者权益合计
- `share_capital`: 股本
- `capital_reserve`: 资本公积
- `surplus_reserve`: 盈余公积
- `undistributed_profits`: 未分配利润

### 其他重要指标
- `minority_interests`: 少数股东权益
- `total_liabilities_and_equity`: 负债和所有者权益总计

注意：实际返回的字段可能会比上述列表更多或有所不同，这取决于东方财富网提供的原始数据。如果您需要查看完整的字段列表，可以查看保存的 `./data/balance_sheet_origin.csv` 文件。