votes-count:
    cqlsh 172.28.0.10 \
        -e "SELECT votes FROM elections.votes;" \
    | head -n -2 \
    | tail -n +5 \
    | awk '{s+=$1} END {print s}'


validate-lists file:
  jsonschema validate lists-shcema.json {{file}}
