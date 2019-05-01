###
# tested in Python 3.7 in AWS Lambda 
###

import json
import boto3
from urllib.parse import urlencode
from botocore.vendored import requests
import time
import datetime

firehose_client = boto3.client("firehose", region_name="eu-west-1")

def lambda_handler(event, context):
    
    # Define Okta API base URL
    base_okta_url = "https://dev-123456.oktapreview.com"
    okta_api_name = "api/v1/logs"
    
    # Define the API token required by Okta
    # !!MAKE SURE IT HAS READONLY PERMISSION ONLY"!!
    # TODO externalize and encrypt this apikey
    okta_apikey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX_"
    
    # Define the syslog API parameters 
    # # E.g. logs?since=2018-11-16T00:00:00.000Z&until=2018-12-31T23:59:59.999Z
    # # using parameters since and until to limit to yesterday (UTC)
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    since = yesterday.strftime('%Y-%m-%dT00:00:00.000Z')
    until = yesterday.strftime('%Y-%m-%dT23:59:59.999Z')
    #since = yesterday.strftime('2018-11-01T00:00:00.000Z')
    #until = yesterday.strftime('2018-11-30T23:59:59.999Z')
    
    url_params = {"since": since, "until": until}
    
    url = base_okta_url + "/" + okta_api_name + "?" + urlencode(url_params)
    
    # Construct the headers
    headers = {
        'content-type': 'application/json',
        'Authorization': 'SSWS ' + okta_apikey
    }
    
    # Set conditions for while loop. Used with pagination.
    next = False
    first = True
    next_url = ""
    response_json = []
    counter = 0
    
    # While loop use to control first and subsequent requests
    while (next or first):
    
        # wait for 1 second, do you know why?
        print("#%s call: sleep 1 second ...\n" % counter)
        counter = counter + 1
        time.sleep(1)
    
        # If next_url is defined, this is not the first time through the loop,
    
        # and we should have the next_url generated from Okta
    
        # # Make the request to Okta and record the response
        # #
    
        # # response = requests.request("GET", url, headers=headers)
        if (next_url != ""):
    
            # subsequent calls
            response = requests.request("GET", next_url, headers=headers, verify=True)
    
        else:
            # first call
            response = requests.request("GET", url, headers=headers, verify=True)
    
        first = False
    
        # Parse the LINK headers from the response.
        if (response.status_code == 200):
    
            response_json.extend(response.json())
    
            response_links = requests.utils.parse_header_links(response.headers['Link'].rstrip('>').replace('>,<', ',<'))
    
            next_url = ""
            next = False
    
            for linkobj in response_links:
                print(linkobj)
    
                if linkobj['rel'] == 'next':
                    next_url = linkobj['url']
                    next = True
    
        else:
    
            print(response)
    
        ## max size for a firehose batch is 500
        if (len(response_json) == 500):
            print("FailedPutCount = " + str(transport2Hose(response_json).get("FailedPutCount", 0)) + "\n")
            response_json = []
    
    ## transport remaining items as the last batch        
    if (len(response_json) > 0):
        print("FailedPutCount = " + str(transport2Hose(response_json).get("FailedPutCount", 0)) + "\n")
        response_json = []
            


    
def transport2Hose (logs):    
    
    encodedLogs = []
    
    for log in logs:
        encodedLogs.append ({'Data' : json.dumps(log)})
        
    response = firehose_client.put_record_batch(
        DeliveryStreamName='oktalogstream',
        Records=encodedLogs
    )

    return response
    

    
    
