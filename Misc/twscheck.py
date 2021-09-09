#Used to check if you are able to connect to TWS
from source.IB import TestClient,TestWrapper
from time import sleep
from threading import Thread
import warnings
class TestApp(TestWrapper,TestClient):
    #When called it will automaticly connect to tws
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
connected = False #The connection check is False unless the checks are passed
Try = True #Will try unless changed by user
while connected is False and Try is True: #Loop until you are connected or dont want to try to connect any more
    connected = False  # The connection check is False unless the checks are passed
    trys = 0 #Set trys to 0
    Try = True #Set try to True every loop
    while trys <= 10: #Try to connect 10 times until asking user to fix
        sleep(30) #Wait 30 seconds between connection attempts
        trys += 1 #Add 1 to try
        app = TestApp()  # Creating an instance of TestApp
        try:
            # Trying to connect
            app.connect("127.0.0.1", 7497, 10) #Trying to connect using TestApp instance
        except Exception as e:
            print(e) #Print exception if unable to connect
        thread = Thread(target=app.run)  # IDK what this does just saw it online
        thread.start()  # Start thread
        test = app.get_time(testing=True) #Test connection by requesting time from TWS
        if test is not None: #If test is not None then there was successful communtion of the time signifying a sucessful connection
            print('Successfully Connected to TWS on ', app.twsConnectionTime(), '\n') #Tell user connection was succesful
            file = open('/home/ubuntu/Quantheus/Data/Connected', 'w') #Create file signifying that you are able to connect
            file.close() #Closing File
            connected = True #Setting connection to True
            break #Breaking try loop
        else:
            warnings.warn('Could not connect to TWS') #Warning user that we were unable to connect to tws
            app.disconnect() #Close any possible connection to TWS
    app.disconnect() #Close connection to TWS
    file = open('/home/ubuntu/Quantheus/Data/Finished', 'w')  # Create file signifying that the script has finished
    file.close()  # Closing File
