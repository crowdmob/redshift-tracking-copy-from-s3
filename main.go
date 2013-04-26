/*
Author: Matthew Moore, CrowdMob Inc.
*/

package main

import (
  "flag"
  "fmt"
  "os"
  // configfile "github.com/crowdmob/goconfig"
  // "github.com/crowdmob/goamz/aws"
  // "github.com/crowdmob/goamz/s3"
)

const (
  VERSION = "0.1"
)

var configFilename string
var shouldOutputVersion bool


func init() {
  flag.StringVar(&configFilename, "c", "config/redshift-tracking-copy-from-s3.properties", "path to config file")
	flag.BoolVar(&shouldOutputVersion, "v", false, "output the current version and quit")
}


func main() {
  flag.Parse()  // Read argv
  
  if shouldOutputVersion {
    fmt.Printf("redshift-tracking-copy-from-s3 %s\n", VERSION)
    os.Exit(0)
  }
}