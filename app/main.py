import baostock as bs
import pandas as pd
import threading

def query_to_file(
        codeMap
        ):
    lock = threading.Lock()
    for key, value in codeMap.items():
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
            result.to_csv(
                "/Users/hyperchain/Documents/out/history_A_stock_k_data" + value.name + "_" + value.start_date + "~" + value.end_date + ".csv",
                index=False)
            print(result)

## 股票信息的设计
class Stock:
    def __init__(self,code,name,start_date,end_date):
        self.code = code
        self.name = name
        self.start_date = start_date
        self.end_date =end_date

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
codeMap = {
    'sh.603078': Stock('sh.603078','江化微','2026-01-15','2026-01-15')
     ,'sz.000555': Stock('sz.000555','神州信息','2026-01-15','2026-01-15')
    , 'sh.601127': Stock('sh.601127','赛力斯','2026-01-15','2026-01-15')
    ,'sz.000977':Stock('sz.000977','浪潮信息','2025-01-01','2026-01-15')
}

# 调用函数
query_to_file(codeMap)
#### 登出系统 ####
bs.logout()
