#!/bin/bash

LOGS=logs.txt
ENTRIES=$(grep 'Number of writes' logs.txt | xargs -I {} sh -c 'echo {} | cut -d " " -f9' | paste -sd+ | bc)
WRITES=$(grep 'Number of new' logs.txt | xargs -I {} sh -c 'echo {} | cut -d " " -f9' | paste -sd+ | bc)

echo "scale=2 ; $WRITES / $ENTRIES" | bc
