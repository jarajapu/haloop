COUNT=9
# bash while loop
ant
ant examples
while [ $COUNT -gt 0 ]; do
#	echo Value of count is: $COUNT
	scp -rp ubuntu@asterix-master:/home/ubuntu/yingyib/HaLoop/conf/* ubuntu@asterix-00$COUNT:/home/ubuntu/yingyib/HaLoop/conf/ 
	scp -rp ubuntu@asterix-master:/home/ubuntu/yingyib/HaLoop/build/* ubuntu@asterix-00$COUNT:/home/ubuntu/yingyib/HaLoop/build/
	let COUNT=COUNT-1
done
