from time import sleep
from source.AWS import *
import boto3
from datetime import datetime
from datetime import timedelta
import paramiko
import os
from source.Commons import TradingDays, _datadate, notify
from source.AWS import *
def ManageOrders():
    os.system('sudo timedatectl set-timezone America/New_York')
    first = False
    firstrun = True
    while True:
        Today = str(datetime.today().date())  # Getting Todays Date will run everyday at 6am Timezone is location of the machine running this script
        if Today in TradingDays and firstrun is False:  # Check if today is a trading day
            print("Starting Instance")
            exception = False
            while True:
                try:
                    # Create Client's connected ssh instance to EC2 machine
                    client = Start_Instance('i-0f2118d238b27d1d1')
                    if exception == True:
                        notify('AWS has successfully started')
                    break
                except Exception as e:
                    exception = True
                    notify('Error starting AWS for Order Manager. Error: ' + str(e))
                    print('Error starting AWS.\n Retrying in 30 seconds.\nError:: ' + str(e))
                    sleep(30)
            sleep(120)
            print('Creating SSH Connection to AWS')
            exception = False
            while True:
                try:
                    # Create Client's connected ssh instance to EC2 machine
                    sshclient = ssh('i-0f2118d238b27d1d1', '/home/ec2-user/Quantheus/keys/EC2ubuntu.pem','ubuntu')  # Creating a SSH connection
                    if exception == True:
                        notify('SSH connection error has been resolved')
                    break
                except Exception as e:
                    exception = True
                    notify('Error creating ssh connection to AWS for Order Manager. Error:: ' + str(e))
                    print('Error creating ssh connection.\n Retrying in 30 seconds.\nError: ' + str(e))
                    sleep(30)
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('sudo timedatectl set-timezone America/New_York') #Setting timezone to EST
            #Start IBGateway
            print('Starting IBGateway')
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('Xvfb :20 &') #Creating a virtual display using Xvfb and putting it into the background using &
            #check if IBGateway is running
            print('Checking IB Connection')
            connected = False #connection test is set to false unless the test is passed
            while connected is False: #Will loop indefinitely until we are able to connect to tws
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('export DISPLAY=:20; /opt/IBController/IBControllerGatewayStart.sh')  # Setting the DISPLAY environment variable to 20 since that is what we created the virtual display on. and then starting IBController. These two commands must be on the same line because each call of the exec_command function creates a new terminal and that would delete the enviorment variable we are setting so both commands are called at once using a ;
                sleep(60*2) #Sleep two minutes while IBGateway Starts
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('python3 /home/ubuntu/Quantheus/twscheck.py') #Run script to see if we are able to connect to tws
                sleep(60*3) #Sleep for 5 minutes to wait for script to run. If it ran it will produce a file called Connected that signifies the script ran without errors
                while True: #Loop indefinitely until the script is finished. We will know it is finished because the script will create a file called Finished when it is finished.
                    ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('ls /home/ubuntu/Quantheus/Data')  # Get list of files in the Data directory
                    response = ssh_stdout.readlines()  # Read the output
                    if 'Connected\n' in response and connected is False: #Check if the Connected file was create
                        connected = True #If it exists we are able to connect to tws
                        ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Connected') #Delete file signifying we are able to connect
                    elif 'Finished\n' in response and connected is False: #If the script is finished and we are not able to connect there is a problem and a human needs to fix it
                        notify('TWS did not start for Order Manager')
                        input('we are not able to connect to TWS press enter to retry') #If the file is not found wait for the user to fix problem and retry on user command
                        ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Finished')  # Delete file signifying the script was finished
                        break
                    elif 'Finished\n' in response:
                        ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Finished')  # Delete file signifying the script has finished
                        break
                    else:
                        sleep(60)
            print('Running Order Manager')
            #Run script and check if it ran properly
            Ran = False #Checking if the Order Manager script has been run is false until proven otherwise
            while Ran is False: #Loop until the Order Manager script has not run
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -m -d OrderManager') #Creating a screen for Order Manager to run on if you want to attach to this screen do screen -r realtime
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -X stuff "python3 /home/ubuntu/Quantheus/ManageOrders.py\n"')  #run Order Manager script it will create a file called Ran that will signify it ran without any errors.
                sleep(60*25) #Wait 25 min for it to finish
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('ls /home/ubuntu/Quantheus/Data') #Geting list of files in Data directory which is where the file signifying that the script ran will be
                response = ssh_stdout.readlines() #Reading output
                if 'Ran\n' in response: #Checking if the Ran file is in the directory
                    Ran = True #If it is then it passes the check
                    ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Ran') #Deleting Signifying File
                else:
                    notify('Order Manager did not run')
                    input('Press enter to shutdown the instance')
                    Ran = True
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -X -S OrderManager quit') #kill the Order Manager screen
            #Stop EC2
            print('Stopping Instance')
            client.stop_instances(InstanceIds=['i-0f2118d238b27d1d1']) #Stop the EC2 using the Client's connection instance
        now = datetime.now() #Get exzact time and date
        if first:
            runAt = (datetime.now() + timedelta(days=0)).replace(hour=9, minute=20, second=0, microsecond=0) #Set time to re-run as tommorow at 6 am
            delta = (runAt - now).total_seconds() #Get diffrence in seconds between that re-run time and the time now
        else:
            runAt = (datetime.now() + timedelta(days=1)).replace(hour=9, minute=20, second=0,microsecond=0)  # Set time to re-run as tommorow at 6 am
            delta = (runAt - now).total_seconds()  # Get diffrence in seconds between that re-run time and the time now
        print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n') #Tell user tasks for today have been finished and how long it will wait to re-run the codes
        sleep(delta) #Sleep for the diffrence between the re-run time and current time
        first = False
        firstrun = False
ManageOrders()


