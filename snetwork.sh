for i in 1 2 3 4 5
do
bin/hadoop dfs -rmr pcresult
./retest.sh
sleep 30
START=$(date +%s)
bin/hadoop jar build/hadoop-0.20.2-dev-examples.jar descendent pc pcresult 92636 $i
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "$i $DIFF">>"result/descedent_sn.txt"
done
