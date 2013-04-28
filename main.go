/*
Author: Matthew Moore, CrowdMob Inc.
*/

package main

import (
  "flag"
  "fmt"
  "os"
  "strings"
  configfile "github.com/crowdmob/goconfig"
  // "github.com/crowdmob/goamz/aws"
  // "github.com/crowdmob/goamz/s3"
  // "github.com/crowdmob/goamz/exp/sns"
)

const (
  VERSION = "0.1"
)

var cfgFile string
var shouldOutputVersion bool
var cfg struct {
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
  flag.StringVar(&cfgFile, "c", "config/redshift-tracking-copy-from-s3.properties", "path to config file")
	flag.BoolVar(&shouldOutputVersion, "v", false, "output the current version and quit")
}

func reportError(desc string, err error) {
  // print to stderr and sns notify if required
  if cfg.Sns.FailureNotifications && len(cfg.Sns.Topic) > 0 && len(cfg.Aws.Accesskey) > 0 && len(cfg.Aws.Secretkey) > 0 {
    // TODO FIXME call SNS
  }
  fmt.Printf("%s: %s\n", desc, err)
  panic(err)
}

func parseConfigfile() {
  config, err := configfile.ReadConfigFile(cfgFile)
  if err != nil {
    fmt.Printf("Couldn't read config file %s because: %#v\n", cfgFile, err)
    panic(err)
  }
  
  // Prioritize config values required to send SNS notifications
  cfg.Aws.Accesskey, err = config.GetString("aws", "accesskey")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Aws.Secretkey, err = config.GetString("aws", "secretkey")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Sns.FailureNotifications, err = config.GetBool("sns", "failure_notifications")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Sns.Topic, err = config.GetString("sns", "topic")
  if err != nil { reportError("Couldn't parse config: ", err) }

  // Everything else
  cfg.Default.Debug, err = config.GetBool("default", "debug")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Aws.Region, err = config.GetString("aws", "region")
  if err != nil { reportError("Couldn't parse config: ", err) }

  var arrayString string
  var stringsArray []string
  
  arrayString, err = config.GetString("s3", "buckets")
  if err != nil { reportError("Couldn't parse config: ", err) }
  stringsArray = strings.Split(arrayString, ",")
  cfg.S3.Buckets = make([]string, len(stringsArray))
  for i, _ := range stringsArray { cfg.S3.Buckets[i] = strings.TrimSpace(stringsArray[i]) }
  
  arrayString, err = config.GetString("s3", "prefixes")
  if err != nil { reportError("Couldn't parse config: ", err) }
  stringsArray = strings.Split(arrayString, ",")
  cfg.S3.Prefixes = make([]string, len(stringsArray))
  for i, _ := range stringsArray { cfg.S3.Prefixes[i] = strings.TrimSpace(stringsArray[i]) }
  
  arrayString, err = config.GetString("redshift", "tables")
  if err != nil { reportError("Couldn't parse config: ", err) }
  stringsArray = strings.Split(arrayString, ",")
  cfg.Redshift.Tables = make([]string, len(stringsArray))
  for i, _ := range stringsArray { cfg.Redshift.Tables[i] = strings.TrimSpace(stringsArray[i]) }
  
  cfg.Redshift.Migrate, err = config.GetBool("redshift", "migrate")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.SchemaJsonUrl, err = config.GetString("redshift", "schema_json_url")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Host, err = config.GetString("redshift", "host")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Port, err = config.GetInt64("redshift", "port")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Database, err = config.GetString("redshift", "database")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.User, err = config.GetString("redshift", "user")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Password, err = config.GetString("redshift", "password")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Emptyasnull, err = config.GetBool("redshift", "emptyasnull")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Blanksasnull, err = config.GetBool("redshift", "blanksasnull")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Fillrecord, err = config.GetBool("redshift", "fillrecord")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Maxerror, err = config.GetInt64("redshift", "maxerror")
  if err != nil { reportError("Couldn't parse config: ", err) }
  cfg.Redshift.Delimiter, err = config.GetString("redshift", "delimiter")
  if err != nil { reportError("Couldn't parse config: ", err) }
}

func main() {
  flag.Parse()  // Read argv
  
  if shouldOutputVersion {
    fmt.Printf("redshift-tracking-copy-from-s3 %s\n", VERSION)
    os.Exit(0)
  }
  
  // Read config file
  parseConfigfile()
  
  fmt.Printf("Got redshift tables: %#v\n", cfg.Redshift.Tables)
  
  // Load schema json and check in redshift and migrate if needed
  // Startup goroutine for each Bucket/Prefix/Table
  
  // Take a look at STL_FILE_SCAN on this Table to see if any files have already been imported.
  
  // If not: run generic COPY for this Bucket/Prefix/Table
  
  // If yes: diff STL_FILE_SCAN with S3 bucket files list, COPY and missing files into this Table
  
}