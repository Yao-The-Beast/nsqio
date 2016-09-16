package main

import (
//  "log"
//  "github.com/nsqio/go-nsq"
//  "os"
  "net"
)

func main() {
//	name, _ := os.Hostname()
//	println(name)
   addrs, err := net.InterfaceAddrs()
    if err != nil {
        println ("shit")
	return
    }
    for _, address := range addrs {
        // check the address type and if it is not a loopback the display it
        if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                println(ipnet.IP.String())
            }
        }
    }
    return 

}
