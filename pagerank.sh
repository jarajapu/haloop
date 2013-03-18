bin/hadoop dfs -rmr $2
START=$(date +%s)
# parameters: inputdir outputdir num_of_iteration num_of_graphnode num_of_reducer
bin/hadoop jar build/hadoop-0.20.2-dev-examples.jar pagerank $1 $2 $3 $4 $5
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "$i  $DIFF ms"

