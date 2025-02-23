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

    def output(self, index_name, start_date, delta_time, ix):
        ldms = module_from_file("lsms", "../../true/win/win/LdMjrStcks.py")
        ld = ldms.LdMjrStcks()
        ld.setPath("../finta/Scanners/")
        sqlh = module_from_file("sqlh", "../../true/win/win/Tools/SqlHelper.py")
        sh = sqlh.SqlHelper()
        sql = "SELECT * FROM jforproducts.Freja_Portfolios where portfolio_name like '" + index_name + "%%' and date>='"+start_date+"';"
        portfolios_transaction = sh.execute(sql)

        k = 0
        df = pd.DataFrame()
        while k < (len(portfolios_transaction)):
            ticker = portfolios_transaction['ticker'][k]
            portfolio_name = portfolios_transaction['portfolio_name'][k]
            print(ticker)
            date_1 = portfolio_name.replace(index_name,"")
            wh = module_from_file("wh", "../../../mawin/Helpers/WHelper.py")
            ws = wh.WHelper()
            w3_start = ws.getW3(date_1)
            s_day_str = w3_start
            end_date_str = ws.getE6(date_1)
            data = ld.getIndexDateBuffer(ix, s_day_str, end_date_str, sh)
            data2 = ld.getStockWithDate(ticker, s_day_str, end_date_str)
            data3 = ld.getStockWithDate(ticker, date_1, end_date_str)
            #data2 = ld.getStockWithDatePGBuffer(ticker, s_day_str+" 05:01:00", end_date_str+" 05:01:00", sh, False)
            #data3 = ld.getStockWithDatePGBuffer(ticker, date_1+" 05:01:00", end_date_str+" 05:01:00", sh, False)
            data4 = ld.getIndexDateBuffer(ix, date_1, end_date_str, sh)
            s_day1 = datetime.strptime(date_1, '%Y-%m-%d') - timedelta(days=1)
            rm_date=s_day1.strftime('%Y-%m-%d')
            print(rm_date)
            try:
                sql = "SELECT * from jforproducts.SeasonMomentum where ticker='"+ticker+"' and date<='"+rm_date+"' order by date desc LIMIT 1"
                resu = sh.execute(sql)
                print(str(resu['seasonmomentum'][0]))
                df2 = pd.DataFrame({'ticker': [ticker],
                                    'Portfolio name': [portfolio_name],
                                    'Raven Moment': str(resu['seasonmomentum'][0]),
                                    str(-40): [(1 + ((data2['Close'][0] / data2['Close'][0]) - data['value'][0] /
                                                    data['value'][0]))],
                                    str(-38): [(1 + ((data2['Close'][2]/data2['Close'][0]) - data['value'][2]/data['value'][0]))],
                                    str(-36): [(1 + ((data2['Close'][4]/data2['Close'][0]) - data['value'][4]/data['value'][0]))],
                                    str(-34): [(1 + ((data2['Close'][6] / data2['Close'][0]) - data['value'][6] /
                                                     data['value'][0]))],
                                    str(-32): [(1 + ((data2['Close'][8] / data2['Close'][0]) - data['value'][8] /
                                                     data['value'][0]))],
                                    str(-30): [(1 + ((data2['Close'][10] / data2['Close'][0]) - data['value'][10] /
                                                     data['value'][0]))],
                                    str(-28): [(1 + ((data2['Close'][12] / data2['Close'][0]) - data['value'][12] /
                                                     data['value'][0]))],
                                    str(-26): [(1 + ((data2['Close'][14] / data2['Close'][0]) - data['value'][14] /
                                                     data['value'][0]))],
                                    str(-24): [(1 + ((data2['Close'][16] / data2['Close'][0]) - data['value'][16] /
                                                     data['value'][0]))],
                                    str(-22): [(1 + ((data2['Close'][18] / data2['Close'][0]) - data['value'][18] /
                                                     data['value'][0]))],
                                    str(-20): [(1 + ((data2['Close'][20] / data2['Close'][0]) - data['value'][20] /
                                                     data['value'][0]))],
                                    str(-18): [(1 + ((data2['Close'][22] / data2['Close'][0]) - data['value'][22] /
                                                     data['value'][0]))],
                                    str(-16): [(1 + ((data2['Close'][24] / data2['Close'][0]) - data['value'][24] /
                                                     data['value'][0]))],
                                    str(-14): [(1 + ((data2['Close'][26] / data2['Close'][0]) - data['value'][26] /
                                                     data['value'][0]))],
                                    str(-12): [(1 + ((data2['Close'][28] / data2['Close'][0]) - data['value'][28] /
                                                     data['value'][0]))],
                                    str(-10): [(1 + ((data2['Close'][30] / data2['Close'][0]) - data['value'][30] /
                                                     data['value'][0]))],
                                    str(-8): [(1 + ((data2['Close'][32] / data2['Close'][0]) - data['value'][32] /
                                                     data['value'][0]))],
                                    str(-6): [(1 + ((data2['Close'][34] / data2['Close'][0]) - data['value'][34] /
                                                     data['value'][0]))],
                                    str(-4): [(1 + ((data2['Close'][36] / data2['Close'][0]) - data['value'][36] /
                                                     data['value'][0]))],
                                    str(-2): [(1 + ((data2['Close'][38] / data2['Close'][0]) - data['value'][38] /
                                                     data['value'][0]))],
                                    str("Inköp"): [data3['Close'][0]],
                                    str(2): [(1 + ((data3['Close'][2]/data3['Close'][0]) - data4['value'][2]/data4['value'][0]))],
                                    str(4): [(1 + ((data3['Close'][4]/data3['Close'][0]) - data4['value'][4]/data4['value'][0]))],
                                    str(6): [(1 + ((data3['Close'][6]/data3['Close'][0]) - data4['value'][6]/data4['value'][0]))],
                                    str(8): [(1 + ((data3['Close'][8]/data3['Close'][0]) - data4['value'][8]/data4['value'][0]))],
                                    str(10): [(1 + ((data3['Close'][10]/data3['Close'][0]) - data4['value'][10]/data4['value'][0]))],
                                    str(12): [(1 + ((data3['Close'][12]/data3['Close'][0]) - data4['value'][12]/data4['value'][0]))],
                                    str(14): [(1 + ((data3['Close'][14] / data3['Close'][0]) - data4['value'][14] / data4['value'][0]))],
                                    str(16): [(1 + ((data3['Close'][16] / data3['Close'][0]) - data4['value'][16] / data4['value'][0]))],
                                    str(18): [(1 + ((data3['Close'][18] / data3['Close'][0]) - data4['value'][18] / data4['value'][0]))],
                                    str(20): [(1 + ((data3['Close'][20] / data3['Close'][0]) - data4['value'][20] / data4['value'][0]))],
                                    str(22): [(1 + ((data3['Close'][22] / data3['Close'][0]) - data4['value'][22] / data4['value'][0]))],
                                    str(24): [(1 + ((data3['Close'][24] / data3['Close'][0]) - data4['value'][24] / data4['value'][0]))],
                                    str(26): [(1 + ((data3['Close'][26] / data3['Close'][0]) - data4['value'][26] / data4['value'][0]))],
                                    str(28): [(1 + ((data3['Close'][28] / data3['Close'][0]) - data4['value'][28] / data4['value'][0]))],
                                    str(30): [(1 + ((data3['Close'][30] / data3['Close'][0]) - data4['value'][30] / data4['value'][0]))],
                                    str(32): [(1 + ((data3['Close'][32] / data3['Close'][0]) - data4['value'][32] / data4['value'][0]))],
                                    str(34): [(1 + ((data3['Close'][34] / data3['Close'][0]) - data4['value'][34] / data4['value'][0]))],
                                    str(36): [(1 + ((data3['Close'][36] / data3['Close'][0]) - data4['value'][36] / data4['value'][0]))],
                                    str(38): [(1 + ((data3['Close'][38] / data3['Close'][0]) - data4['value'][38] / data4['value'][0]))],
                                    str(40): [(1 + ((data3['Close'][40] / data3['Close'][0]) - data4['value'][40] / data4['value'][0]))]})

            except:
                k=k
            finally: 
                k=k+1
            df = pd.concat([df, df2], ignore_index=True)
        #df = df.set_index(['ticker'])
        print(df)
        df.to_excel(index_name+"_L1.xlsx")

if __name__ == '__main__':
    nw = HeatMapOutput()
    nw.output("RA1W3M","2024-01-03",90,"^SPXEW")
