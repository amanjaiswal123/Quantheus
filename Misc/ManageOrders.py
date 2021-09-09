from source.IB import *
from time import sleep

app = TestApp("127.0.0.1", 7497, 10) #Connection to TWS
OpenOrdersBeforeOpen = app.get_openordersall()
OpenOrdersBeforeOpen['Action'] = None
for x in OpenOrdersBeforeOpen.index:
    OpenOrdersBeforeOpen['Action'][x] = getattr(OpenOrdersBeforeOpen['order'][x], 'action')
now = datetime.now() #Get exzact time and date
runAt = (datetime.now() + timedelta(days=0)).replace(hour=9, minute=29, second=0, microsecond=0) #Set time to re-run as tommorow at 6 am
delta = (runAt - now).total_seconds() #Get diffrence in seconds between that re-run time and the time now
sleep(delta) #Sleep for the diffrence between the re-run time and current time
if len(OpenOrdersBeforeOpen.index) > 0:
    OpenOrdersAfterOpen = app.get_openordersall()
    start = datetime.now()
    time = (datetime.now()-start).seconds
    while len(OpenOrdersAfterOpen.index) > 0 and time < 660:
        sleep(30)
        OpenOrdersAfterOpen = app.get_openordersall()
        time = (datetime.now()-start).seconds
    networth = app.get_account_details('NetLiquidation')
    Positions = app.get_positions(networth)
    NotInAfterOpen = ~OpenOrdersBeforeOpen.index.isin(OpenOrdersAfterOpen.index)
    NotinPortfolio = ~OpenOrdersBeforeOpen.index.isin(Positions.index)
    CancledOrders = OpenOrdersBeforeOpen[(NotinPortfolio & NotInAfterOpen & (OpenOrdersBeforeOpen['Action'] == 'BUY')) | (~NotinPortfolio & NotInAfterOpen & (OpenOrdersBeforeOpen['Action'] == 'SELL'))]
else:
    CancledOrders = pandas.DataFrame()
for x in CancledOrders.index:
    contract = CancledOrders['contract'][x]
    order = CancledOrders['order'][x]
    order.tif = ''
    try:
        app.placeOrder(app.get_order_ID(), contract, order)
    except:
        pass
file = open('/home/ubuntu/Quantheus/Data/Ran', 'w')
file.close()
