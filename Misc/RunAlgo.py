from datetime import datetime
from datetime import timedelta
from source.Commons import _datadate,notify
from source.AWS import *
first = True
os.system('sudo timedatectl set-timezone America/New_York')
while True: #Run indefinately as this will be the shot caller on a ec2-user
    Today = str(datetime.today().date()) #Getting Todays Date will run everyday at 6am Timezone is location of the machine running this script
    if Today in TradingDays and first is False: #Check if today is a trading day
        #Check if Data for today was downloaded locally
        print('Starting Tasks for',Today)
        datadate = _datadate(Today)
        print('Checking for data file')
        tfirst = True
        while os.path.isfile('/home/ec2-user/Quantheus/Data/SCPedToAWS') is False:  # Checking if the data file was created
            print('Could not find live data for ' + datadate + '.retrying in 20 minutes')  # If it is not created tell the user it will retry in 20 minutes
            if tfirst:
                notify('Could not find live data for ' + datadate + '.retrying in 20 minutes')
            sleep(60 * 20)  # Sleep for 20 mins and check if the file exists again
            tfirst = False
        #If the file exists it will proceed
        #Connect to EC2
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
                notify('Error starting AWS. Error for Run Algo: '+str(e))
                print('Error starting AWS.\n Retrying in 30 seconds.\nError:: '+str(e))
                sleep(30)
        print('Creating SSH Connection to AWS')
        exception = False
        sleep(60*2)
        while True:
            try:
                # Create Client's connected ssh instance to EC2 machine
                sshclient = ssh('i-0f2118d238b27d1d1', '/home/ec2-user/Quantheus/keys/EC2ubuntu.pem','ubuntu')  # Creating a SSH connection
                if exception == True:
                    notify('SSH connection error has been resolved')
                break
            except Exception as e:
                exception = True
                notify('Error creating ssh connection to AWS for Run Algo Error: '+ str(e))
                print('Error creating ssh connection.\n Retrying in 30 seconds.\nError:: '+str(e))
                sleep(30)
        ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('sudo timedatectl set-timezone America/New_York')  # Setting timezone to EST
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
                if 'Connected\n' in response and connected is False: #Check if the Connected file was created
                    connected = True #If it exists we are able to connect to tws
                    ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Connected') #Delete file signifying we are able to connect
                elif 'Finished\n' in response and connected is False: #If the script is finished and we are not able to connect there is a problem and a human needs to fix it
                    notify('IB Gateway did not start for Realtime Script')
                    input('we are not able to connect to TWS press enter to retry') #If the file is not found wait for the user to fix problem and retry on user command
                    ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Finished')  # Delete file signifying the script was finished
                    break
                elif 'Finished\n' in response:
                    ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Finished')  # Delete file signifying the script has finished
                    break
                else:
                    sleep(60)
        #Run script and check if it ran properly
        print('Running Realtime Script')
        Ran = False #Checking if the realtime script has been run is false until proven otherwise
        while Ran is False: #Loop until the realtime script has not run
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -m -d realtime') #Creating a screen for reatime to run on if you want to attach to this screen do screen -r realtime
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -X stuff "python3 /home/ubuntu/Quantheus/PeakTrough.py\n"')  #run realtime script it will create a file called Ran that will signify it ran without any errors.
            sleep(60*10) #Wait 10 min for it to finish
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('ls /home/ubuntu/Quantheus/Data') #Geting list of files in Data directory which is where the file signifying that the script ran will be
            response = ssh_stdout.readlines() #Reading output
            if 'Ran\n' in response: #Checking if the Ran fiqle is in the directory
                Ran = True #If it is then it passes the check
                ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/Ran') #Deleting Signifying File
            else:
                notify('Realtime Script ran into an error')
                input('The algorithm did not run press enter to retry') #If the realtime script didnt work it will wait for the user to fix it and then it will retry on user input
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('screen -X -S realtime quit') #kill the realtime screen
        if Ran:
            print('Downloading realtime script data')
            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('ls /home/ubuntu/Quantheus/Data')
            response = ssh_stdout.readlines()
            Download = []
            for x in response:
                if x[0:3] == 'sub' or x[0:9] == 'portfolio' or x[0:4] == 'comp':
                    try:
                        filename = list(x)
                        filename = ''.join(filename[0:len(filename)-1])
                        scpdownload(sshclient,'/home/ubuntu/Quantheus/Data/'+filename,'/home/ec2-user/Quantheus/Data/'+filename)
                        Delete = True
                        if x[0:3] == 'sub':
                            if x[0:6] == 'subbuy':
                                Date = x[6:16]
                            else:
                                Date = x[7:17]
                            if Date == Today:
                                Delete = False
                        elif x[0:9] == 'portfolio':
                            Date = x[9:19]
                            if Date == Today:
                                Delete = False
                        elif x[0:4] == 'comp':
                            if x[0:7] == 'compbuy':
                                Date = x[7:17]
                            else:
                                Date = x[8:18]
                            if Date == datadate:
                                Delete = False
                        if Delete:
                            ssh_stdin, ssh_stdout, ssh_stderr = sshclient.exec_command('rm /home/ubuntu/Quantheus/Data/'+filename)
                    except Exception as e:
                        notify('Could not download '+filename+' because of '+str(e))
        #Stop EC2
        print('Stopping Instance')
        client.stop_instances(InstanceIds=['i-0f2118d238b27d1d1']) #Stop the EC2 using the Client's connection instance
        os.remove('/home/ec2-user/Quantheus/Data/SCPedToAWS')
    now = datetime.now() #Get exzact time and date
    if (datetime.now() < datetime.now().replace(hour=9, minute=10, second=0, microsecond=0)) and (datetime.now() > datetime.now().replace(hour=4, minute=0, second=0, microsecond=0)):
        runAt = (datetime.now() + timedelta(days=0)).replace(hour=0, minute=0, second=30,microsecond=0)
    else:
        runAt = (datetime.now() + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0) #Set time to re-run as tommorow at 6 am
    delta = (runAt - now).total_seconds() #Get diffrence in seconds between that re-run time and the time now
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n') #Tell user tasks for today have been finished and how long it will wait to re-run the codes
    #sleep(delta) #Sleep for the diffrence between the re-run time and current time
    first = False