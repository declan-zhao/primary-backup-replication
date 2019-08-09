#!/bin/bash
# title   : auto_kill_backup_server.sh
# author  : Youdongchen Zhao (declan.zhao@edu.uwaterloo.ca)
# usage   : bash auto_kill_backup_server.sh

# put this script into the Assignment 3 directory.
# ensure there is no server running on ecetesla[0-3], eceubuntu[1-2] before running this script.

# 1. run `bash runclient.sh` to see if you can get "No primary found".
# 2. If there is primary server, run `netstat -lNp | grep java | awk '{ print $7 }' | cut -d/ -f1` to see the pid of your running servers on ecetesla[0-3], eceubuntu[1-2].
# 3. run `kill -9 pid` to kill all the servers.
# 4. run this script.
# 5. run `bash runclient.sh` if not running.

# disclaimer: use at your own risk, the author does not take any responsibility.
#==============================================================================

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

# change your port number, e.g. 11xxx-19xxx
KV_PORT=0
KV_PORT_2=0
echo Port number: $KV_PORT
echo Port number 2: $KV_PORT_2

# kill all processes listening to these two ports if owned by you
kill -9 $(lsof -i:$KV_PORT -t) $(lsof -i:$KV_PORT_2 -t)

# set 130s to ensure that runclient.sh is able to run for 120s
end=$((SECONDS + 130))

# start primary server
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
P1=$!
sleep 1s
# start backup server
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT_2 $ZKSTRING /$USER &
P2=$!
# wait for 7s to give enough time for user to call runclient.sh
sleep 7s

# kill backup server every 3s
while [ $SECONDS -lt $end ]; do
    kill -9 $P2
    sleep 1.5s # comment out for paranoid mode
    $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT_2 $ZKSTRING /$USER &
    P2=$!
    sleep 1.5s
done

# kill both servers before exit
kill -9 $P1 $P2
