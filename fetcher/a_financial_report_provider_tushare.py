#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tushare as ts
import pandas as pd
import logging
import os
import requests
import time
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from fetcher.base_financial_report_provider import FinancialReportProvider
from utils.stock_utils import get_full_symbol
from utils.http_utils import retry_on_http_error

from utils.df_utils import safe_concat


class AFinancialReportProviderTushare(FinancialReportProvider):
    """基于Tushare的财务报表数据提供者实现"""

    def __init__(self):
        """初始化Tushare财务报表数据提供者"""
        self.logger = logging.getLogger(__name__)
        # 加载环境变量
        load_dotenv()
        # 从环境变量获取Tushare API Token
        tushare_token = os.environ.get('TUSHARE_TOKEN')
        if not tushare_token:
            self.logger.error("未找到TUSHARE_TOKEN环境变量")
            raise ValueError("TUSHARE_TOKEN环境变量未设置")
        # 初始化Tushare API
        self.pro = ts.pro_api(tushare_token)

    @retry_on_http_error(max_retries=20, delay=2)
    def _fetch_balance_sheet_data(self, full_symbol, start_date, end_date):
        """内部方法，用于获取资产负债表数据并支持重试"""
        # 获取合并报表数据（report_type='1'）
        df_consolidated = self.pro.balancesheet(ts_code=full_symbol,
                                                start_date=start_date,
                                                end_date=end_date,
                                                report_type='1')

        # 获取母公司报表数据（report_type='6'）
        df_parent_company = self.pro.balancesheet(ts_code=full_symbol,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  report_type='6')

        return df_consolidated, df_parent_company

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取资产负债表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            资产负债表数据DataFrame，包含报告期和各项资产负债表指标
        """
        self.logger.debug(f"开始获取股票 {symbol} 的资产负债表数据")
        try:
            # 使用固定的开始日期和当前日期作为结束日期
            start_date = '20000101'
            end_date = pd.Timestamp.now().strftime('%Y%m%d')

            # 获取完整股票代码（后缀格式）
            full_symbol = get_full_symbol(symbol, type='suffix')

            # 调用带重试机制的内部方法获取数据

            df_consolidated,  df_parent_company = self._fetch_balance_sheet_data(

                full_symbol, start_date, end_date)

            # 添加报表类型标识列
            df_consolidated['report_type'] = '合并报表'
            df_parent_company['report_type'] = '母公司报表'

            # 合并所有DataFrame前检查是否为空，避免未来版本的pandas警告
            all_dfs = [df_consolidated, df_parent_company]

            df_merged = safe_concat(all_dfs)

            # 处理日期列
            if 'ann_date' in df_merged.columns:
                df_merged['ann_date'] = pd.to_datetime(
                    df_merged['ann_date'], errors='coerce').dt.date

            if 'f_ann_date' in df_merged.columns:
                df_merged['f_ann_date'] = pd.to_datetime(
                    df_merged['f_ann_date'], errors='coerce').dt.date

            if 'end_date' in df_merged.columns:
                df_merged['end_date'] = pd.to_datetime(
                    df_merged['end_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df_merged = df_merged.fillna("")

            # 重命名ts_code字段为symbol_full
            if 'ts_code' in df_merged.columns:
                df_merged = df_merged.rename(
                    columns={'ts_code': 'symbol_full'})

            # 添加原始symbol信息
            df_merged['symbol'] = symbol

            self.logger.debug(
                f"成功处理股票 {symbol} 的资产负债表数据，最终数据包含 {len(df_merged)} 行")
            return df_merged

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取股票 {symbol} 的资产负债表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()

    @retry_on_http_error(max_retries=20, delay=2)
    def _fetch_income_statement_data(self, full_symbol, start_date, end_date):
        """内部方法，用于获取利润表数据并支持重试"""
        # 获取合并报表数据（report_type='1'）
        df_consolidated = self.pro.income(ts_code=full_symbol,
                                          start_date=start_date,
                                          end_date=end_date,
                                          report_type='1',
                                          fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag')

        # 获取单季合并数据（report_type='2'）
        df_quarterly_consolidated = self.pro.income(ts_code=full_symbol,
                                                    start_date=start_date,
                                                    end_date=end_date,
                                                    report_type='2',
                                                    fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag')

        # 获取母公司报表数据（report_type='6'）
        df_parent_company = self.pro.income(ts_code=full_symbol,
                                            start_date=start_date,
                                            end_date=end_date,
                                            report_type='6',
                                            fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag')

        # 获取母公司单季表数据（report_type='7'）
        df_quarterly_parent_company = self.pro.income(ts_code=full_symbol,
                                                      start_date=start_date,
                                                      end_date=end_date,
                                                      report_type='7',
                                                      fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag')

        return df_consolidated, df_quarterly_consolidated, df_parent_company, df_quarterly_parent_company

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """获取利润表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            利润表数据DataFrame，包含报告期和各项利润表指标
        """
        self.logger.debug(f"开始获取股票 {symbol} 的利润表数据")
        try:
            # 使用固定的开始日期和当前日期作为结束日期
            start_date = '20000101'
            end_date = pd.Timestamp.now().strftime('%Y%m%d')

            # 获取完整股票代码（后缀格式）
            full_symbol = get_full_symbol(symbol, type='suffix')

            # 调用带重试机制的内部方法获取数据

            df_consolidated, df_quarterly_consolidated, df_parent_company, df_quarterly_parent_company = self._fetch_income_statement_data(
                full_symbol, start_date, end_date)

            # 添加报表类型标识列
            df_consolidated['report_type'] = '合并报表'
            df_quarterly_consolidated['report_type'] = '单季合并'
            df_parent_company['report_type'] = '母公司报表'
            df_quarterly_parent_company['report_type'] = '母公司单季表'

            # 合并所有DataFrame前检查是否为空，避免未来版本的pandas警告
            all_dfs = [df_consolidated, df_quarterly_consolidated,
                       df_parent_company, df_quarterly_parent_company]

            df_merged = safe_concat(all_dfs)

            # 处理日期列
            if 'ann_date' in df_merged.columns:
                df_merged['ann_date'] = pd.to_datetime(
                    df_merged['ann_date'], errors='coerce').dt.date

            if 'f_ann_date' in df_merged.columns:
                df_merged['f_ann_date'] = pd.to_datetime(
                    df_merged['f_ann_date'], errors='coerce').dt.date

            if 'end_date' in df_merged.columns:
                df_merged['end_date'] = pd.to_datetime(
                    df_merged['end_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df_merged = df_merged.fillna("")

            # 重命名ts_code字段为symbol_full
            if 'ts_code' in df_merged.columns:
                df_merged = df_merged.rename(
                    columns={'ts_code': 'symbol_full'})

            # 添加原始symbol信息
            df_merged['symbol'] = symbol

            self.logger.debug(
                f"成功处理股票 {symbol} 的利润表数据，最终数据包含 {len(df_merged)} 行")
            return df_merged

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取股票 {symbol} 的利润表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()

    @retry_on_http_error(max_retries=20, delay=2)
    def _fetch_cash_flow_statement_data(self, full_symbol, start_date, end_date):
        """内部方法，用于获取现金流量表数据并支持重试"""
        # 获取合并报表数据（report_type='1'）
        df_consolidated = self.pro.cashflow(ts_code=full_symbol,
                                            start_date=start_date,
                                            end_date=end_date,
                                            report_type='1')

        # 获取单季合并数据（report_type='2'）
        df_quarterly_consolidated = self.pro.cashflow(ts_code=full_symbol,
                                                      start_date=start_date,
                                                      end_date=end_date,
                                                      report_type='2')

        # 获取母公司报表数据（report_type='6'）
        df_parent_company = self.pro.cashflow(ts_code=full_symbol,
                                              start_date=start_date,
                                              end_date=end_date,
                                              report_type='6')

        # 获取母公司单季表数据（report_type='7'）
        df_quarterly_parent_company = self.pro.cashflow(ts_code=full_symbol,
                                                        start_date=start_date,
                                                        end_date=end_date,
                                                        report_type='7')

        return df_consolidated, df_quarterly_consolidated, df_parent_company, df_quarterly_parent_company

    def get_cash_flow_statement(self, symbol: str) -> pd.DataFrame:
        """获取现金流量表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            现金流量表数据DataFrame，包含报告期和各项现金流量表指标
        """
        self.logger.debug(f"开始获取股票 {symbol} 的现金流量表数据")
        try:
            # 使用固定的开始日期和当前日期作为结束日期
            start_date = '20000101'
            end_date = pd.Timestamp.now().strftime('%Y%m%d')

            # 获取完整股票代码（后缀格式）
            full_symbol = get_full_symbol(symbol, type='suffix')

            # 调用带重试机制的内部方法获取数据

            df_consolidated, df_quarterly_consolidated, df_parent_company, df_quarterly_parent_company = self._fetch_cash_flow_statement_data(
                full_symbol, start_date, end_date)

            # 添加报表类型标识列
            df_consolidated['report_type'] = '合并报表'

            df_quarterly_consolidated['report_type'] = '单季合并'
            df_parent_company['report_type'] = '母公司报表'
            df_quarterly_parent_company['report_type'] = '母公司单季表'

            # 合并所有DataFrame前检查是否为空，避免未来版本的pandas警告
            all_dfs = [df_consolidated, df_quarterly_consolidated,
                       df_parent_company, df_quarterly_parent_company]

            df_merged = safe_concat(all_dfs)

            # 处理日期列
            if 'ann_date' in df_merged.columns:
                df_merged['ann_date'] = pd.to_datetime(
                    df_merged['ann_date'], errors='coerce').dt.date

            if 'f_ann_date' in df_merged.columns:
                df_merged['f_ann_date'] = pd.to_datetime(
                    df_merged['f_ann_date'], errors='coerce').dt.date

            if 'end_date' in df_merged.columns:
                df_merged['end_date'] = pd.to_datetime(
                    df_merged['end_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df_merged = df_merged.fillna("")

            # 重命名ts_code字段为symbol_full
            if 'ts_code' in df_merged.columns:
                df_merged = df_merged.rename(
                    columns={'ts_code': 'symbol_full'})

            # 添加原始symbol信息
            df_merged['symbol'] = symbol

            self.logger.debug(
                f"成功处理股票 {symbol} 的现金流量表数据，最终数据包含 {len(df_merged)} 行")
            return df_merged

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取股票 {symbol} 的现金流量表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()
