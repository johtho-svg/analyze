import importlib, importlib.util
from datetime import datetime, timedelta
from ftplib import FTP
import pandas as pd
import numpy as np

# import LdMjrStcks

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HeatMapOutput():

    def heatmap(self, index_name, start_date, delta_time, ix):
        ldms = module_from_file("lsms", "../../../truewin/true/win/win/LdMjrStcks.py")
        ld = ldms.LdMjrStcks()
        ld.setPath("../finta/Scanners/")
        sqlh = module_from_file("sqlh", "../../../truewin/true/win/win/Tools/SqlHelper.py")
        sh = sqlh.SqlHelper()
        sql = "SELECT DISTINCT(portfolio_name)  FROM jforproducts.Freja_Portfolios_Transaction_Log where portfolio_name like '" + index_name + "%%' and cash>0;"
        p_sells = sh.execute(sql)
        sql = "SELECT * FROM jforproducts.Freja_Portfolios_Transaction_Log where portfolio_name like '" + index_name + "%%' and cash<0;"
        p_buys = sh.execute(sql)

        k = 0
        df = pd.DataFrame()
        while k < (len(p_sells)):
            portfolio_name=p_sells['portfolio_name'][k]
            sql ="SELECT * from jforproducts.Freja_Portfolio_Value where portfolio_name='"+portfolio_name+"'"
            value = sh.execute(sql)
            s_day_str = value['date'][0].strftime('%Y-%m-%d')
            end_date_str = value['date'][39].strftime('%Y-%m-%d')
            data = ld.getIndexDateBuffer(ix, s_day_str, end_date_str, sh)
            try:
                df2 = pd.DataFrame({'Portfolio name': [portfolio_name],
                                    'Life Length [days]': [len(value)],
                                    str(0): [(1 + ((value['total_value'][0] / value['total_value'][0]) - data['value'][0] /
                                                    data['value'][0]))],
                                    str(2): [(1 + ((value['total_value'][2]/value['total_value'][0]) - data['value'][2]/data['value'][0]))],
                                    str(4): [(1 + ((value['total_value'][4]/value['total_value'][0]) - data['value'][4]/data['value'][0]))],
                                    str(6): [(1 + ((value['total_value'][6]/value['total_value'][0]) - data['value'][6]/data['value'][0]))],
                                    str(8): [(1 + ((value['total_value'][8]/value['total_value'][0]) - data['value'][8]/data['value'][0]))],
                                    str(10): [(1 + ((value['total_value'][10]/value['total_value'][0]) - data['value'][10]/data['value'][0]))],
                                    str(12): [(1 + ((value['total_value'][12]/value['total_value'][0]) - data['value'][12]/data['value'][0]))],
                                    str(14): [(1 + ((value['total_value'][14]/value['total_value'][0]) - data['value'][14]/data['value'][0]))],
                                    str(16): [(1 + ((value['total_value'][16]/value['total_value'][0]) - data['value'][16]/data['value'][0]))],
                                    str(18): [(1 + ((value['total_value'][18]/value['total_value'][0]) - data['value'][18]/data['value'][0]))],
                                    str(20): [(1 + (
                                                (value['total_value'][20] / value['total_value'][0]) - data['value'][20] /
                                                data['value'][0]))],
                                    str(22): [(1 + (
                                                (value['total_value'][22] / value['total_value'][0]) - data['value'][22] /
                                                data['value'][0]))],
                                    str(24): [(1 + (
                                                (value['total_value'][24] / value['total_value'][0]) - data['value'][24] /
                                                data['value'][0]))],
                                    str(26): [(1 + (
                                                (value['total_value'][26] / value['total_value'][0]) - data['value'][26] /
                                                data['value'][0]))],
                                    str(28): [(1 + (
                                                (value['total_value'][28] / value['total_value'][0]) - data['value'][
                                            28] / data['value'][0]))],
                                    str(30): [(1 + (
                                                (value['total_value'][30] / value['total_value'][0]) - data['value'][
                                            30] / data['value'][0]))],
                                    str(32): [(1 + (
                                                (value['total_value'][32] / value['total_value'][0]) - data['value'][
                                            32] / data['value'][0]))],
                                    str(34): [(1 + (
                                                (value['total_value'][34] / value['total_value'][0]) - data['value'][
                                            34] / data['value'][0]))],
                                    str(36): [(1 + (
                                                (value['total_value'][36] / value['total_value'][0]) - data['value'][
                                            36] / data['value'][0]))],
                                    str(38): [(1 + (
                                                (value['total_value'][38] / value['total_value'][0]) - data['value'][
                                            38] / data['value'][0]))]})
                              
            except: 
                print("Exception")
                k=k
            finally: 
                print(k)
                k=k+1
            
            df = pd.concat([df, df2], ignore_index=True)
        #df = df.set_index(['portfolio_name'])
        print(df)
        df.to_excel(index_name+"_Analyze_L2.xlsx")

if __name__ == '__main__':
    nw = HeatMapOutput()
    nw.heatmap("RA1W3M","2024-01-03",90,"^SPXEW")
