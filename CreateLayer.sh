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

