import pandas
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from threading import Thread
from queue import Queue
from ibapi.common import BarData
import time
import warnings
from ibapi.order_state import OrderState
from ibapi.order import Order
from source.config import NYSETtickerlist, NASDAQtickerlist

def nyse_ticker_list():
    return pandas.read_csv(NYSETtickerlist)['Symbol'].values
def nasdaq_ticker_list():
    return pandas.read_csv(NASDAQtickerlist)['Symbol'].values

#Has all the receiving functions for IBAPI
class TestWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        store = Queue()
        self.ID = store
    #Before requesting data you must create a queuee to store the data in
    def rcv_ID(self):
        store = Queue()
        self.ID = store
        return store
    #Everytime a request is sent to tws it will return the data in a function like this one you must overwrite it and put the data where you need it such as in a queue you already created
    def nextValidId(self, orderId:int):
        self.ID.put(orderId)

    def rcv_time(self):
        store = Queue()
        self.time = store
        return store

    def currentTime(self, time:int):
        self.time.put(time)

    def rcv_positions(self):
        store = Queue()
        self.postions_store = store
        return store

    def rcv_openordersall(self):
        store = Queue()
        self.openorders_store = store
        return store

    def rcv_reqMktData(self):
        store = Queue()
        self.reqMktData_store = store
        return store

    def position(self, account:str, contract:Contract, position:float,avgCost:float):
        if position > 0:
            allocation = (position*avgCost)/self.tcv
            temp = pandas.DataFrame(columns=['ticker','exchange','Buy Price','Quantity','Allocation','Contract'],index=[0])
            temp['ticker'] = getattr(contract,'localSymbol')
            if set(list('NASDAQ')).issubset(list(getattr(contract,'exchange'))):
                temp['exchange'] = 'NASDAQ'
            else:
                temp['exchange'] = getattr(contract,'exchange')
            temp['Quantity'] = position
            temp['Buy Price'] = avgCost
            temp['Allocation'] = allocation
            temp['Contract'] = contract
            self.positionsL = self.positionsL.append(temp)

    def positionEnd(self):
        if len(self.positionsL) == 0:
            self.positionsL = pandas.DataFrame(columns=['ticker','exchange','amount holding','avg cost','Allocation','contract'])
            self.positionsL.set_index(['ticker','exchange'])
        self.postions_store.put(self.positionsL)

    def rcv_contract_by_ticker(self):
        store = Queue()
        self.contract_store = store
        return store

    def symbolSamples(self, reqId:int,contractDescriptions: list):
        self.contract_store.put(contractDescriptions)

    def rcv_contract_details(self):
        store = Queue()
        self.contract_details_store = store
        return store


    def contractDetails(self, reqId:int, contractDetails):
        self.contract_details_store.put(contractDetails)

    def rcv_account_details(self):
        store = Queue()
        self.account_details = store
        return store

    def accountSummary(self, reqId:int, account:str, tag:str, value:str,currency:str):
        try:
            value = float(value)
        except:
            value = list(value)
            while ',' in value:
                value.remove(',')
            value = ''.join(str(x) for x in value)
            value = float(value)
        self.account_details.put(value)

    def orderStatus(self, orderId:int , status:str, filled:float,remaining:float, avgFillPrice:float, permId:int,parentId:int, lastFillPrice:float, clientId:int, whyHeld:str, mktCapPrice: float):
        try:
            self.temp1 = pandas.DataFrame(columns=['status','filled','remaining','contract'],index=[0])
            self.temp1['status'] = status
            self.temp1['filled'] = filled
            self.temp1['remaining'] = remaining
            self.temp1['ticker'] = self.temp2['ticker'][0]
            self.temp1['contract'] = self.temp2['contract'][0]
            self.temp1['order'] = self.temp2['order'][0]
            if self.temp2['ticker'].values[0] in NYSETICKERLIST:
                self.temp1['exchange'] = 'NYSE'
            elif self.temp2['ticker'].values[0] in nasdaq:
                self.temp1['exchange'] = 'NASDAQ'
            else:
                self.temp1['exchange'] = 'Unknown'
            self.temp1.set_index(['ticker','exchange'],inplace=True)
            self.open_orders = self.open_orders.append(self.temp1)
        except Exception as e:
            pass

#    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float):
#        self.temp2 = pandas.DataFrame(columns=['ticker', 'contract', 'genericTickList',"snapshot","regulatorySnapShat"], index=[0])
#        try:
#            self.temp2['ticker'] = getattr(contract, 'symbol')
#            self.temp2['contract'] = contract
#            self.temp2['genericTickList'] = genericTickList
#            self.temp2['snapshot'] = snapshot
#            self.temp2['regulatorySnapShat'] = regulatorySnapShat
#        except:
#            pass

#    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
#        self.temp2 = pandas.DataFrame(columns=['ticker', 'contract', 'genericTickList',"snapshot","regulatorySnapShat"], index=[0])
#        try:
#            x = 1
#        except:
#            pass

    def openOrder(self, orderId:int, contract:Contract, order:Order,orderState:OrderState):
        self.temp2 = pandas.DataFrame(columns=['ticker', 'contract', 'order'], index=[0])
        try:
            self.temp2['ticker'] = getattr(contract, 'symbol')
            self.temp2['contract'] = contract
            self.temp2['order'] = order
        except:
            pass
    def openOrderEnd(self):
        try:
            self.openorders_store.put(self.open_orders)
        except:
            self.open_orders = pandas.DataFrame()
            self.openorders_store.put(self.open_orders)

    def historicalData(self, reqId: int, Bar: BarData):
        self.temp2 = pandas.DataFrame.from_dict(data={'date':[Bar.date], 'open':[Bar.open], 'high': [Bar.high],'low':[Bar.low],'close':[Bar.close],'volume':[Bar.volume],'barCount':[Bar.barCount],'average':[Bar.average]})



    def historicalDataEnd(self, reqId: int, start: str, end: str):
        #super().historicalDataEnd(reqId, start, end)
        #print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        self.historical_data.put(self.temp2)
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        self.temp3 = pandas.DataFrame.from_dict(data={'date':[bar.date], 'open':[bar.open], 'high':[bar.high],'low':[bar.low],'close':[bar.close],'volume':[bar.volume],'barCount':[bar.barCount],'average':[bar.average]})
        self.temp2 = self.temp2.append(self.temp3)
    def rcv_historical_data(self):
        store = Queue()
        self.historical_data = store
        return store
#Test Client houses all the functions to request data
class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
    #Everytime you request you must follow a procedure before and after
    def get_order_ID(self):
        #First you must create a queue
        store = self.wrapper.rcv_ID()
        #Then request
        self.reqIds(-1)
        #Then try and retrieve data from the queue
        try:
            ID = store.get(timeout=10)
        except:
            #If the data did not reach in the desired time
            print('Exceeded Max Wait Time')
            ID = None
        #Return the data
        return ID

    def get_time(self,testing=False):
        store = self.wrapper.rcv_time()
        self.reqCurrentTime()
        try:
            time = store.get(timeout=10)
        except:
            if testing == False:
                print('Exceeded Max Wait Time')
            time = None
        return time

    def get_contract_by_ticker(self,ticker: str):
        store = self.wrapper.rcv_contract_by_ticker()
        self.reqMatchingSymbols(1,ticker)
        try:
            contract = store.get(timeout=10)
        except:
            print('Exceeded Max Wait Time')
            contract = None
        return contract

    def get_contract_details(self,__contract):
        store = self.wrapper.rcv_contract_details()
        self.reqContractDetails(1,__contract)
        try:
            contract = store.get(timeout=10)
        except:
            print('Exceeded Max Wait Time')
            contract = None
        return contract

    def get_historical_data(self, contract: Contract, endDateTime:str,durationStr:str, barSizeSetting:str, whatToShow:str,useRTH:int, formatDate:int, keepUpToDate:bool):
        store = self.wrapper.rcv_historical_data()
        self.reqHistoricalData(1, contract , endDateTime,durationStr, barSizeSetting, whatToShow,useRTH, formatDate, keepUpToDate, [])
        try:
            contract = store.get(timeout=300)
        except:
            print('Exceeded Max Wait Time')
            contract = None
        return contract

    def get_account_details(self,tag):
        store = self.wrapper.rcv_account_details()
        OrderId1 = self.get_order_ID()
        OrderId2 = self.get_order_ID()
        if OrderId1 is not None and OrderId2 is not None:
            self.reqAccountSummary(OrderId1,'All',tag)
            try:
                contract = store.get(timeout=10)
            except:
                print('Exceeded Max Wait Time')
                contract = None
            self.cancelAccountSummary(OrderId2)
        else:
            contract = None
        return contract

    def get_positions(self,networth):
        store = self.wrapper.rcv_positions()
        self.wrapper.positionsL = pandas.DataFrame()
        self.wrapper.tcv = networth
        self.reqPositions()
        try:
            contract = store.get(timeout=10)
        except:
            print('Exceeded Max Wait Time')
            contract = None
        self.cancelPositions()
        if contract is not None and len(contract) != 0:
            contract = contract.set_index(['ticker', 'exchange'])
        else:
            pass
        return contract

    def get_openordersall(self):
        store = self.wrapper.rcv_openordersall()
        self.wrapper.open_orders = pandas.DataFrame()
        self.reqAllOpenOrders()
        try:
            contract = store.get(timeout=10)
        except:
            print('Exceeded Max Wait Time')
            contract = None
        return contract
#Used so that both requesting and reciving are avalaible through 1 class
class TestApp(TestWrapper,TestClient):
    #When called it will automaticly connect to tws
    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        while True:
            try:
                self.connect(ipaddress, portid, clientid)
            except:
                pass
            thread = Thread(target=self.run)
            thread.start()
            test = self.get_time(testing=True)
            if test is not None:
                print('Successfully Connected to TWS on ',self.twsConnectionTime(),'\n')
                break
            else:
                while True:
                    time.sleep(.5)
                    warnings.warn('Could not connect to TWS')
                    time.sleep(.5)
                    retry = str(input('\nShould I try to connect again? (Y or N): ')).lower()
                    if retry != 'y' and retry != 'n':
                        print('Please enter Y or N')
                    else:
                        break
                if retry == 'n':
                    break
                else:
                    self.disconnect()
        time.sleep(.05)

