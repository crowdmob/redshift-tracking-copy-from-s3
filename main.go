/*
Author: Matthew Moore, CrowdMob Inc.
*/

package main

import (
  "flag"
  "fmt"
  "os"
  "strings"
  "net/http"
  "io/ioutil"
  "bytes"
  simplejson "github.com/bitly/go-simplejson"
  configfile "github.com/crowdmob/goconfig"
  _ "github.com/bmizerany/pq"
  "database/sql"
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

func createTableStatement(tableName *string, columnsJson *simplejson.Json) string {
  var buffer bytes.Buffer
  buffer.WriteString("CREATE TABLE ")
  buffer.WriteString(*tableName)
  buffer.WriteString(" (\n")

  columnsArry, err := columnsJson.Array()
  if err != nil { reportError("Couldn't parse columns json: ", err) }
  for i, _ := range columnsArry {
    if i != 0 { buffer.WriteString(",\n") }
    columnSchema := columnsJson.GetIndex(i)
    columnName, err := columnSchema.Get("name").String()
    if err != nil { reportError("Couldn't parse column name for table json: ", err) }
    columnType, err := columnSchema.Get("type").String()
    if err != nil { reportError("Couldn't parse column type for table json: ", err) }
    buffer.WriteString(columnName)
    buffer.WriteString(" ")
    buffer.WriteString(columnType)
    
    if columnUnique, err := columnSchema.Get("unique").Bool(); err == nil && columnUnique {
      buffer.WriteString(" UNIQUE")
    }
    
    if columnNull, err := columnSchema.Get("null").Bool(); err == nil && !columnNull {
      buffer.WriteString(" NOT NULL")
    }
  } 
  buffer.WriteString("\n);")

  return buffer.String()
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
  response, err := http.Get(cfg.Redshift.SchemaJsonUrl)
  if err != nil { reportError("Couldn't load schema url: ", err) }
  defer response.Body.Close()
  schemaContents, err := ioutil.ReadAll(response.Body)
  if err != nil { reportError("Couldn't read response body from schema url: ", err) }
  schemaJson, err := simplejson.NewJson(schemaContents)
  if err != nil { reportError("Couldn't parse json from schema url: ", err) }
  fmt.Printf("SCHEMA JSON:::::::: %#v\n", schemaJson)
  
  
  // Repeat per table
  currentTable := cfg.Redshift.Tables[0]
  
  db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.User, cfg.Redshift.Password, cfg.Redshift.Database))
  if err != nil { reportError("Couldn't connect to redshift database: ", err) }
  rows, err := db.Query(fmt.Sprintf("select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where table_name = '%s' limit 1000", currentTable))
  if err != nil { reportError("Couldn't execute statement for table: ", err) }
  anyRows := false
  for rows.Next() {
    var column_name string
    var data_type string
    err = rows.Scan(& column_name, & data_type)
    if err != nil { reportError("Couldn't scan row for table: ", err) }
    fmt.Printf("RESULT:::::::: %s | %s\n",  column_name, data_type)
    anyRows = true
  }
  
  if !anyRows {
    tablesArry, err := schemaJson.Get("tables").Array()
    if err != nil { reportError("Schema json error; expected tables element to be an array: ", err) }
    tableIndex := -1
    for i, _ := range tablesArry { 
      if schemaJson.Get("tables").GetIndex(i).Get("name").MustString() == currentTable {
        tableIndex = i
        break
      }
    }
    fmt.Println(createTableStatement(&currentTable, schemaJson.Get("tables").GetIndex(tableIndex).Get("columns")))
  }
  
  // Startup goroutine for each Bucket/Prefix/Table
  
  // Take a look at STL_FILE_SCAN on this Table to see if any files have already been imported.
  
  // If not: run generic COPY for this Bucket/Prefix/Table
  
  // If yes: diff STL_FILE_SCAN with S3 bucket files list, COPY and missing files into this Table
  
}