for i in 5
do
bin/hadoop dfs -rmr $2
#./retest.sh
#sleep 30
START=$(date +%s)
bin/hadoop jar build/hadoop-0.20.2-dev-examples.jar naivedescendent $1 $2 92636 $i
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "$i $DIFF">>"result/descedent_sn.txt"
done
