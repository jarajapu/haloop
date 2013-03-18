bin/stop-all.sh
rm -r logs
ant
ant examples
bin/start-all.sh

