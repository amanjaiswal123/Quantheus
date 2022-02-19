from data_scripts.get_data import AHistroy
from source.Commons import upload_to_rds_table
try:
    data = AHistroy(['all'], 'tiingo', End_Day='2020-07-26', Days=21900)
except:
    data.to_csv('tiingodata.csv')
    raise e
data.to_csv('tiingodata.csv')
if "dat" in data.columns.values:
    data = data.rename(columns={'dat', 'date'})
data.to_csv('tiingodata.csv')
upload_to_rds_table(data,'tiingo')
