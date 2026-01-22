from datetime import datetime

import baostock as bs
import pandas as pd
from openpyxl import load_workbook
import threading

## 股票信息的设计
class Stock:
    def __init__(self,code,name,start_date,end_date):
        self.code = code
        self.name = name
        self.start_date = start_date
        self.end_date =end_date

def query_to_file(
        code_map,
        sheet_name,
        workbook
        ):
    lock = threading.Lock()
    # 指定工作表的表名
    wb = workbook[sheet_name]
    for key, value in code_map.items():
        with lock:
            print("从%s开始,到%s为止",value.start_date, value.end_date)

            rs = bs.query_history_k_data_plus(key,
                                              "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                              value.start_date, value.end_date,
                                              frequency="d", adjustflag="2")
            print('query_history_k_data_plus respond error_code:' + rs.error_code)
            print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

            #### 打印结果集 ####
            data_list = []
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs.get_row_data())
            result = pd.DataFrame(data_list, columns=rs.fields)

            #### 结果集输出到csv文件 ####
            # result.to_csv(
            #     "/Users/hyperchain/Documents/out/history_A_stock_k_data" + value.name + "_" + value.start_date + "~" + value.end_date + ".csv",
            #     index=False)

            ### 修改结果集输出到指定文件的指定工作表中
            with pd.ExcelWriter('existing.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                result.to_excel(writer, sheet_name=sheet_name, index=False,
                                startrow=writer.sheets[sheet_name].max_row)
            print(result)



#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

## 获取当前最新日期
end_date = datetime.today().strftime( "%Y-%m-%d")
gu_list = [
    Stock('sh.603078','江化微','2026-01-21',end_date),
    Stock('sz.000555','精工科技','2026-01-21',end_date),
    Stock('sh.601127','赛力斯','2026-01-21',end_date),
    Stock('sz.000977','浪潮信息','2026-01-21',end_date),
    Stock('sz.002065','东华软件','2025-01-01',end_date)
]
#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
codeMap = {gu.code:gu for gu in gu_list}

# 调用函数
query_to_file(codeMap)
#### 登出系统 ####
bs.logout()
