## snowflake-sendmail
Send Email notifications from Snowflake using AWS Lambda

1) Create a notification table.

``` sql
create or replace table "ADMIN"."PUBLIC"."NOTIFICATIONLIST"
(
    id number default IDT.nextval,
    to_address varchar not null,
    subject varchar not null,
    message varchar not null,
    isprocessed BOOLEAN DEFAULT FALSE,
    err_message VARCHAR(500) DEFAULT NULL, 
    createddate  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

```

2) Create Stored Procedure

``` sql
 CREATE OR REPLACE PROCEDURE sendmail(TOADDRESS VARCHAR,SUBJECT VARCHAR,MESSAGE VARCHAR)
  RETURNS varchar
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS
  $$
  
  try {
      
      var row_as_json = {};     
      row_as_json['to'] = TOADDRESS
      row_as_json['subject'] = SUBJECT
      row_as_json['message'] = MESSAGE
      
      sql_command = `INSERT INTO ADMIN.PUBLIC.NOTIFICATIONLIST(TO_ADDRESS,SUBJECT,MESSAGE) VALUES(:1, :2, :3);` 
           
      
      result = snowflake.execute(
        { 
        sqlText: sql_command,
         binds: [TOADDRESS,SUBJECT,MESSAGE] 
        }
        ); 
      
      return 'Success'
      
      }
  catch (err)  {
        
      return 'Failed: '+err
      }
  $$
  ;

```

3) Need to create a Lambda layer for Python Snowflake connector module and attach it to the Lambda. Below are the steps to create a Layer.

``` bash

#Create a folder

mkdir -p snowconnector/python

#Change Dir
cd snowconnector/python

#Install Module, I am using Python3.7.

pip3.7 install snowflake-connector-python -t .

#Change Dir
cd ..

#Zip it

zip -r9 ../pysnowflake.zip .



```

4) Create a Lambda layer.

    - Log into AWS account.
    - Click on AWS Lambda service.
    - Under layers tab, Click on Create Layer.
    - Enter Name, Description, Select "Upload a .zip file" and click on Upload and select the zip file pysnowflake.zip
    - Select Python version I am using Python3.7.
    - Click on Create, this will create a new layer.

5) Create new Lambda function.


- Create new Python Lambda function.

``` python

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
SENDER = #Sender Email
ADMINEMAIL = #Enter admin email address ex: ['admin@email.com']


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


```

- Create a Role with SES access and attach it to Lambda.
- Select the newly created layer.
- Change the max timeout value to 15 mins and memory to 512 MB.

6) You can schedule this Lambda using AWS Cloudwatch Events rules(Every min)

7) Test notifications by running the SP. This will Insert a record into Notifcations table and it will processed by Aws Lambda Function.

``` sql

CALL SENDMAIL('Enter your email address(you can add multiple email address with comma(,) separated.)','Hello','Hello from Snowflake');

```

