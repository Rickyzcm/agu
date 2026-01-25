
import configparser
import datetime
import os
# 当前系统时间
current_time_stamp=str(datetime.datetime.now())
local_config_filename = 'local_config.ini'
## 获取当前项目根路径
current_root_dir = os.path.dirname(os.path.realpath(__file__))

config =configparser.ConfigParser()
## 获取当前脚本的路径
local_config_path=os.path.join(current_root_dir, local_config_filename)
print(f"当前项目root路径是：'{current_root_dir}'")
## 读取本地配置
config.read(local_config_path)
# 读取源文件
original_file_path=config['excel']['original_file_path'].strip()
print(original_file_path)
if len(original_file_path)==0:
    raise ValueError("需要在 local_config.ini 文件中配置源excel文件完成路径（包含文件名） original_file_path ")
new_file_path=config['excel']['new_file_path'].format(current_time_stamp=current_time_stamp).strip()
if len(new_file_path)==0:
    raise ValueError("需要在 local_config.ini 文件中配置生成目标excel文件完整路径（包含文件名）  new_file_path ")
print(new_file_path)

primary_data_start_date=config['history_data']['primary_data_start_date']



def __get_original_file_path__():
    return original_file_path

def __get_new_file_path__():
    return new_file_path

def __get_primary_data_start_date__():
    return primary_data_start_date