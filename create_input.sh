#!/bin/bash
INPUT_FILE_NAME=$1
INPUT_NUMBER=$2


echo "type,key1,key2,value" >> $INPUT_FILE_NAME
for (( n=0; n<$INPUT_NUMBER; n++ ))
do
  key=$(printf "key%013d" $n)
  value=$(printf "val%013d" $n)
  echo "put,${key},,${value}" >> $INPUT_FILE_NAME 
done
