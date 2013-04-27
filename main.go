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
  // "github.com/crowdmob/goamz/exp/sns"
)

const (
  VERSION = "0.1"
)

var configFilename string
var shouldOutputVersion bool
var configFile := struct {
  Default struct {
    Debug                 bool
  }
  Aws struct {
    Region                string
    Accesskey             string
    Secretkey             string
  }
  Sns struct {
    FailureNotifications  bool
    Topic                 string
  }
  S3 struct {
    Buckets               []string
    Prefixes              []string
  }
  Redshift struct {
    Tables                []string
    Migrate               bool
    SchemaJsonUrl         string
    Host                  string
    Port                  int64
    Database              string
    User                  string
    Password              string
    Emptyasnull           bool
    Blanksasnull          bool
    Fillrecord            bool
    Maxerror              int64
    Delimiter             string
  }
  
}

func init() {
  flag.StringVar(&configFilename, "c", "config/redshift-tracking-copy-from-s3.properties", "path to config file")
	flag.BoolVar(&shouldOutputVersion, "v", false, "output the current version and quit")
}

func reportError(errorMessage *string) {
  // print to stderr and sns notify if required
}

func parseConfigfile() {
  // use configFilename with config.*
  // write into configFile and report errors
}

func main() {
  flag.Parse()  // Read argv
  
  if shouldOutputVersion {
    fmt.Printf("redshift-tracking-copy-from-s3 %s\n", VERSION)
    os.Exit(0)
  }
  
  // Read config file
  parseConfigfile()
  
  // Load schema json and check in redshift and migrate if needed
  // Startup goroutine for each Bucket/Prefix/Table
  
  // Take a look at STL_FILE_SCAN on this Table to see if any files have already been imported.
  
  // If not: run generic COPY for this Bucket/Prefix/Table
  
  // If yes: diff STL_FILE_SCAN with S3 bucket files list, COPY and missing files into this Table
  
}