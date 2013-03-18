for i in 3
do
bin/hadoop dfs -rmr $2
START=$(date +%s)
#./retest.sh
#sleep 30
bin/hadoop jar build/hadoop-0.20.2-dev-examples.jar naivepagerank $1 $2 $i
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "$i  $DIFF ms"
done

