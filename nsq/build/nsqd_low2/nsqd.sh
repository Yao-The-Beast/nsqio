rm *.dat & rm write*
./nsqd --daemon-priority=LOW --broadcast-address=DrTailors-MacBook-Pro.local --tcp-address=0.0.0.0:4140 --http-address=0.0.0.0:4141 --lookupd-tcp-address=127.0.0.1:4160 -tls-required=false -tls-min-version='ssl3.0'
