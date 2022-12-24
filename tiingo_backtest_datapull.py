from data_scripts.get_data import AHistroy
from source.Commons import upload_to_rds_table
from datetime import datetime
try:
    Today = str(datetime.today().date())
    data = AHistroy(['all'], 'tiingo', End_Day=Today, Days=21900)
except:
    data.to_csv('tiingodata.csv')
    raise e
data.to_csv('tiingodata.csv')
if "dat" in data.columns.values:
    data = data.rename(columns={'dat', 'date'})
data.to_csv('tiingodata.csv')
upload_to_rds_table(data,'tiingo')
