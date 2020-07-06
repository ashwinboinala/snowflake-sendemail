
#1) Create Table.

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



#2) Create stored procedure

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