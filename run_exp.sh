#!/bin/bash
rm -rf storage 2> /dev/null
rm -rf output_*.txt 2> /dev/null
rm -rf metrics_*.txt 2> /dev/null
rm -rf logs.txt 2> /dev/null
go build .

echo "Creating SS Tables"
if [ -d "storage_backup" ]
then
  echo "Table Backups detected using these."
  cp -r storage_backup storage
else
  echo "Creating SS Table A"
  ./project2-B -store_file store_A ./docs/input_a.txt output.txt
  echo "Created SS Table A"

  echo "Creating SS Table B"
  ./project2-B -store_file store_B ./docs/input_b.txt output.txt
  echo "Created SS Table B"

  echo "Creating SS Table C"
  ./project2-B -store_file store_C ./docs/input_a.txt output.txt
  echo "Created SS Table C"

  echo "Creating SS Table D"
  ./project2-B -store_file store_D ./docs/input_a.txt output.txt
  echo "Created SS Table D"

  cp -r storage storage_backup
fi

echo "Created SS Tables"

echo "Running experiment"
{ time ./project2-B -logs -store_file store_A ./docs/input_mill.txt output_a.txt ; } 2> metrics_a.txt
./calculate_wa.sh >> metrics_a.txt 
rm logs.txt

{ time ./project2-B -logs -store_file store_B ./docs/input_mill.txt output_b.txt ; } 2> metrics_b.txt
./calculate_wa.sh >> metrics_b.txt 
rm logs.txt


{ time ./project2-B -logs -store_file store_C ./docs/input_mill.txt output_c.txt ; } 2> metrics_c.txt
./calculate_wa.sh >> metrics_c.txt 
rm logs.txt


{ time ./project2-B -logs -store_file store_D ./docs/input_mill.txt output_d.txt ; } 2> metrics_d.txt
./calculate_wa.sh >> metrics_c.txt 
rm logs.txt


echo "Finished running experiment"
