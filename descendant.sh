i=3
bin/hadoop dfs -rmr $2
START=$(date +%s)
bin/hadoop jar build/hadoop-0.20.2-dev-examples.jar descendent $1 $2 92636 $i
END=$(date +%s)
echo "$i  $DIFF ms"
