
from Dhan_Tradehull import Tradehull
import time
import datetime
import pdb
import os

client_id    = "1000690797"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2MTc2MzMxLCJpYXQiOjE3NzYwODk5MzEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNjkwNzk3In0.V2bsKdfP-t6duBE6_FU2NY9y67bNmi0unNzLsKa_ft68CkzsfjOYUxr70_C_uZKYES2gFFTh-d5dSxJpWcY5Sw"
tsl          = Tradehull(client_id, access_token)
folder       = "Monthly Options data 60 mins"
timeframe=60

watchlist    = ["NIFTY"]
expiries     = ["2025-01-30", "2025-02-27", "2025-03-27", "2025-04-24", "2025-05-29", "2025-06-26", "2025-07-31", "2025-08-28", "2025-09-25", "2025-09-30", "2025-10-28", "2025-11-25", "2025-12-30"]
#["2021-01-28" ,"2021-02-25" ,"2021-03-25" ,"2021-04-29" ,"2021-05-27" ,"2021-06-24" ,"2021-07-29" ,"2021-08-26" ,"2021-09-30" ,"2021-10-28" ,"2021-11-25" ,"2021-12-30" ,"2022-01-27" ,"2022-02-24" ,"2022-03-31" ,"2022-04-28" ,"2022-05-26" ,"2022-06-30" ,"2022-07-28" ,"2022-08-25" ,"2022-09-29" ,"2022-10-27" ,"2022-11-24" ,"2022-12-29" ,"2023-01-25" ,"2023-02-23" ,"2023-03-29" ,"2023-04-27" ,"2023-05-25" ,"2023-06-29" ,"2023-07-27" ,"2023-08-31" ,"2023-09-28" ,"2023-10-26" ,"2023-11-30" ,"2023-12-28" ,"2024-01-25" ,"2024-02-29" ,"2024-03-28" ,"2024-04-25" ,"2024-05-30" ,"2024-06-27" ,"2024-07-25" ,"2024-08-29" ,"2024-09-26" ,"2024-10-31" ,"2024-11-28" ,"2024-12-26" ,"2025-01-30" ,"2025-02-27" ,"2025-03-27" ,"2025-04-24" ,"2025-05-29" ,"2025-06-26" ,"2025-07-31" ,"2025-08-28" ,"2025-09-30" ,"2025-10-28" ,"2025-11-25"]
atm_range    = ['ATM-10',  'ATM-9',  'ATM-8',  'ATM-7',  'ATM-6',  'ATM-5',  'ATM-4',  'ATM-3',  'ATM-2',  'ATM-1',  'ATM',  'ATM+1',  'ATM+2',  'ATM+3',  'ATM+4',  'ATM+5',  'ATM+6',  'ATM+7',  'ATM+8',  'ATM+9',  'ATM+10']
security_ids = {"NIFTY": 13}




for name in watchlist:
	for expiry in expiries:
		for rangex in atm_range: 
			for right in ["CALL", "PUT"]:
				try:
					from_date = datetime.datetime.strptime(expiry, "%Y-%m-%d") - datetime.timedelta(days=30)
					from_date = from_date.strftime("%Y-%m-%d")
					security_id = security_ids.get(name)
					if security_id is None:
						raise ValueError(f"Missing security ID for {name}")
					data      = tsl.get_expired_option_data(exchangeSegment="NSE_FNO",instrument="OPTIDX",fromDate=from_date,toDate=expiry,exchange="NSE",interval=timeframe,securityId=security_id,expiry_flag="MONTH",expiry_code=1,strike=rangex,option_type=right)
					file_name = f"{name}_{expiry}_{right}.csv"
					path      = f"{folder}/ATM Wise data/{name}/{expiry}/{rangex}"
					os.makedirs(path, exist_ok=True)
					data.to_csv(f"{path}/{file_name}", index=False)
					print(f"{name} {rangex} {expiry} {file_name}: Download completed")
					time.sleep(0.1)

				except Exception as e:
					print(f"{name} {expiry} : Error {e}")
					continue


