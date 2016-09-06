package main

import (
//  "log"
//  "github.com/nsqio/go-nsq"
  "os"
)

func main() {
	name, _ := os.Hostname()
	println(name)
}
