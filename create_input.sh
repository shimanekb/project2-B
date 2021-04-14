#!/bin/bash
INPUT_FILE_NAME=$1
INPUT_NUMBER=$2


echo "type,key1,key2,value" >> $INPUT_FILE_NAME
for ((n=0;n<$INPUT_NUMBER;n++))
do
  uuid=$(uuidgen)
  key="key${uuid:0:13}"
  value="val${uuid:0:13}"
  echo "put,${key},,${value}" >> $INPUT_FILE_NAME 
done
