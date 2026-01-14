import baostock as bs
import pandas as pd
from datetime import datetime
import threading

def query_to_file(
        start_date
        ,end_date
        ,codeMap
        ):
    lock = threading.Lock()
    for key, value in codeMap.items():
        with lock:
            rs = bs.query_history_k_data_plus(key,
                                              "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                              start_date, end_date,
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
                "/Users/hyperchain/Documents/out/history_A_stock_k_data" + value + "_" + start_date + "~" + end_date + ".csv",
                index=False)
            print(result)


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
    # 'sh.603078': '江化微',
     'sz.000555': '神州信息'
    # , 'sh.601127': '赛力斯'
}

start_date = '2025-01-01'
end_date = datetime.today().strftime( "%Y-%m-%d")
print("当前日期:",end_date)


query_to_file(start_date,end_date,codeMap)
#### 登出系统 ####
bs.logout()
