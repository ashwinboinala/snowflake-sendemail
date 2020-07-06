#Create new Python Lambda with below Code.
#Enter following values.
#SENDER, ADMINEMAIL, region_name, user, password, account, warehouse, database, and schema

import json
import snowflake.connector
import logging
import datetime as dt
from datetime import datetime
from botocore.exceptions import ClientError
import boto3
import base64

logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
for name in logging.Logger.manager.loggerDict.keys():
         if 'snowflake' in name:
             logging.getLogger(name).setLevel(logging.WARNING)
             logging.getLogger(name).propagate = False

CHARSET = "UTF-8"
SENDER = '@email.com'#Sender Email
ADMINEMAIL = 'email.com' #Enter admin email address ex: ['admin@email.com'] or a list with (,) separated


html_body = """<html>
<head></head>
<body>
  <h1>{header}</h1>
  <p>
       <br>{errordetails}<br>
       
  </p>
</body>
</html>
"""
             
def sendmail(errormessage,subject,RECIPIENTS):
    try:
            client = boto3.client('ses',region_name="Enter Region")
            htmlHeader = "Snowflake Notification"
            response = client.send_email(
            Destination={
                'ToAddresses': RECIPIENTS,
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': html_body.format(header=htmlHeader,errordetails=errormessage),
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=SENDER,
            
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email Sent! Message ID:")
        print(response['MessageId'])


def lambda_handler(event, context):
    # TODO implement
    ctx = snowflake.connector.connect(user='Enter Username', password='Enter Password',role="Role_Name",account='Account_name',warehouse="Warehouse_name",database="DB_Name",schema="Schema_Name")
    cs = ctx.cursor()
    try:
        cs.execute("SELECT ID,TO_ADDRESS,SUBJECT,MESSAGE FROM NOTIFICATIONLIST WHERE ISPROCESSED = FALSE AND ERR_MESSAGE IS NULL ORDER BY ID asc LIMIT 100")
        for (ID,TO_ADDRESS,SUBJECT,MESSAGE) in cs:
            MM = {}
            MM['to'] = TO_ADDRESS
            MM['subject'] = SUBJECT
            MM['message'] = MESSAGE
            print(MM)
            if ',' in TO_ADDRESS:
                to_list = (TO_ADDRESS).split(",")
            else:
                to_list = [TO_ADDRESS]
            sendmail(MESSAGE,SUBJECT,to_list)
            cs.execute("UPDATE NOTIFICATIONLIST set ISPROCESSED = TRUE WHERE ID = %s",(ID))
        cs.close()
        ctx.close()
    
    except Exception as e:
        sendmail(e+'\n'+MM,'Snowflake email notifcation error',ADMINEMAIL)
        cs.close()
        ctx.close()
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

#Create a Role with SES access and attach it to Lambda.
#Under layer select the newly created layer.
#Change the max timeout value to 15 mins and memory to 512 MB.