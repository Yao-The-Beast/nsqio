rm *.dat & rm write*
./nsqd --daemon-priority=HIGH --broadcast-address=drtailors-mbp.dhcp.wustl.edu --tcp-address=0.0.0.0:5000 --http-address=0.0.0.0:5001 --lookupd-tcp-address=127.0.0.1:4160 -tls-required=false -tls-min-version='ssl3.0'
