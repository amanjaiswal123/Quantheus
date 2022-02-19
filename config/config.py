import os

NYSETtickerlist = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
NASDAQtickerlist = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
AMEXtickerlist = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'
alpha_apikey = 'HFU6ZG51DTV3WVDR'
backtestdatapath = '/home/ec2-user/Quantheus/Data/MarketData/'
livedatapath = '/home/ec2-user/Quantheus/Data/MarketData/livedata/'
PortfolioPath = '/home/ec2-user/Quantheus/Data/portfolio/'
Signalspath = '/home/ec2-user/Quantheus/Data/signals/'
subtradespath = '/home/ec2-user/Quantheus/Data/trades/subtrades/'
subsellpath = '/home/ec2-user/Quantheus/Data/trades/subsell/'
openedpositionspath = '/home/ec2-user/Quantheus/Data/positions/'
closedpositionspath = '/home/ec2-user/Quantheus/Data/positions/'
data_logs_path = '/home/ec2-user/Quantheus/Data/data_logs/'
logpath = '/home/ec2-user/Quantheus/Data/logs/'
ranpath = '/home/ec2-user/Quantheus/Data/'
InitialMargReq = .5
MinMargReq = .3
tiingo_apikey = 'fe68de195678d05adeb74be52448a79d68ca68fd'
qtheus_rds = {'dbname':'postgres','user':'qtheus','host':'qtheus-dev.cxd1dlbrmydp.us-east-2.rds.amazonaws.com','password':'ycoWwypi2'}