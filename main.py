import os
import pyodbc
import configparser
import requests
import xml.etree.ElementTree as ET
import urllib.request
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import time
from azure.storage.blob import BlockBlobService, PublicAccess
import glob
import shutil
import logging.handlers

# Create a log handler
handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "errors.log"))
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
root.addHandler(handler)

BASE_DIR = os.path.dirname(__file__)
# read config file
def read_config():
    config = configparser.ConfigParser  ()
    config.read(os.path.join(BASE_DIR+ '/config.ini'))
    return config

# function for creating a database connection
def create_connection():
        config = read_config()
        driver = config['SQL_Credentials']['driver']
        server = config['SQL_Credentials']['server']
        database  = config['SQL_Credentials']['database']
        uid = config['SQL_Credentials']['uid']
        password = config['SQL_Credentials']['password']
        connection = pyodbc.connect("driver={};server={};database={};uid={};PWD={}".format(driver, server, database, uid, password),autocommit=True)
        return connection

class FetchData:
    def __init__(self):
        self.usernames = []
        self.count = 0

    # fetch all usernames from database
    def fetch_usernames(self):
        dbconnection = create_connection()
        cursor = dbconnection.cursor()
        usernames = cursor.execute("select employeeID from degreedAllUsers")
        for username in usernames.fetchall():
            self.usernames.append(username[0])
        
    # fetch ecard for a single user
    def fetch_ecard(self, username):
        
        config = read_config()
        # create folder
        try:
            target_path = username + '/2019/'
            target_path_flex = username + '/'
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
        except Exception as e:
            print()
            print('error in creating directory '+ str(e))   
         
        # FOR SF API
        try:
            # Prepare request
            try:
                payload = "<DataRequest><groupcode>TCL</groupcode><employeeno>{username}</employeeno></DataRequest>".format(username = username)
                url = config['SF_Credentials']['end_point']
                headers = {"Authorization": config['SF_Credentials']['authorization'], "Content-Type":"application/xml"}
            except Exception as e:
                print('error in preparing payload '+ str(e))
            try:
                # check cron_flag for ECARD
                if config['cron_flag']['ecard_flag'].lower() == "true":
                    # for ecard

                    try:
                        response = requests.post(url, data = payload, headers= headers)
                        xml_data = ET.fromstring((response.content))
                        ecard_url = xml_data.getchildren()[0].text
                        # Download ECARD file
                        print('ecard file', ecard_url, username)
                        try:
                            ecard_file = requests.get(ecard_url)
                            
                            # for ecard
                            with open(target_path+'ecard.pdf', 'wb') as f:
                                f.write(ecard_file.content)
                                 
                        except Exception as e:
                            # print('error in download ecard'+ str(e))
                            pass
                    except Exception as e:
                        print('erro in ecard process '+ str(e))
                # check Cron_flag for FLEX
                if config['cron_flag']['enrollment_plan'].lower() == "true":
                    try:
                        # for flex
                        response_flex = requests.post(config['SF_Credentials']['flex_end_point'], data = payload, headers= headers)
                        xml_flex = ET.fromstring((response_flex.content))    
                        flex_url = xml_flex.getchildren()[0].text         
                    
                        # download flex file
                        try:
                            flex_file = requests.get(flex_url)
                            
                            # for ecard
                            with open(target_path_flex+'/flex.pdf', 'wb') as f:
                                f.write(flex_file.content)
                        except Exception as e:
                            # print('error in downloading flex '+ str(e))
                            pass

                    except Exception:
                        print('errot in flec process'+ str(e))
            except Exception as e:
                print('error in fetch sf api '+ str(e))
            if response.status_code >300:
                print('error in getting ecard '+ response.text)
            
            # push to azure
            self.push_to_blob(username)
            shutil.rmtree( BASE_DIR +'/{}'.format(username))
        except Exception as e:
            # push to azure
            try:
                self.push_to_blob(username)
            except Exception as e:
                print('error in azure blob creation'+ str(e))
            try:
                shutil.rmtree( BASE_DIR +'/{}'.format(username))
            except Exception as e:
                print('error in removing directry'+ str(e))
                
            print('exception in getting ecard '+ str(e))
            
    # function for blob upload
    def push_to_blob(self, username):
            
        try:
            if len(username) < 7:
                username = 't' + username
            container_name = 'evabenefits'
            config = read_config()
            block_blob_service = BlockBlobService(connection_string=config['azure']['connection_string'])
            
            # try:
            #     create_container = block_blob_service.create_container(container_name)
            # except Exception as e:
            #     print('error in creting container', e)
            # Set the permission so the blobs are public.
            # block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
            if config['cron_flag']['ecard_flag'].lower() == "true":
                ecard_response = block_blob_service.create_blob_from_path(container_name, '{}/2019/ecard.pdf'.format(username), BASE_DIR + '/{}/2019/ecard.pdf'.format(username))        
            if config['cron_flag']['enrollment_plan'].lower() == "true":
                flex_response = block_blob_service.create_blob_from_path(container_name, '{}/flex.pdf'.format(username),  BASE_DIR +'/{}/flex.pdf'.format(username))   
        except Exception as e:
            # try:
            #     block_blob_service.create_blob_from_path(container_name, '{}/2019/ecard.pdf'.format(username), BASE_DIR + '/{}/2019'.format(username))
            # except Exception as e:
            #     print('error in creating empty blob folder' + str(e))
            #print('error in uploading blob '+ str(e)) 
            pass 
        # manager = multiprocessing.Manager()
        # lock = manager.Lock()
        # with lock:
        self.count -=1
        print('count: ', self.count, end = '\r')

    def execute_all(self):
        # fetch usernames
        # TODO undo comment
        # self.fetch_usernames()
        
        self.count = len(self.usernames)
        # create a threadPool
        executor = ThreadPoolExecutor(max_workers=80)
        # TODO delet
        self.usernames = self.usernames[14]
        for username in self.usernames:
            # TODO: DELETEso    
            if username[0] !='T':
                continue
            if username[0] == 'T':
                username = username[1:]
            executor.submit(self.fetch_ecard, username)
            # self.fetch_ecard(username)
        

if __name__ == "__main__":
    obj = FetchData()
    # TODO delete this part after whitelisting IP in DB
    with open('users.txt', 'r') as f:
        for line in f:
            line = line[:-1]
            obj.usernames.append(line)

    obj.execute_all()

