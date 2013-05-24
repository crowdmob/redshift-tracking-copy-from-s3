/*
Author: Matthew Moore, CrowdMob Inc.
*/

package main

import (
  "flag"
  "fmt"
  "log"
  "os"
  "time"
  "strings"
  "net/http"
  "io/ioutil"
  "bytes"
  "os/signal"
  simplejson "github.com/bitly/go-simplejson"
  configfile "github.com/crowdmob/goconfig"
  _ "github.com/bmizerany/pq"
  "database/sql"
  "github.com/crowdmob/goamz/aws"
  "github.com/crowdmob/goamz/s3"
  "github.com/crowdmob/goamz/exp/sns"
)

const (
  VERSION = "0.1"
)

var cfgFile string
var shouldOutputVersion bool
var cfg struct {
  Default struct {
    Debug                 bool
    Pollsleepinseconds    int64
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

func reportError(desc string, err error) { // print to stderr and sns notify if required
  if cfg.Sns.FailureNotifications && len(cfg.Sns.Topic) > 0 && len(cfg.Aws.Accesskey) > 0 && len(cfg.Aws.Secretkey) > 0 {
  	_, snsErr := sns.New(aws.Auth{cfg.Aws.Accesskey, cfg.Aws.Secretkey}, aws.Regions[cfg.Aws.Region]).Publish(&sns.PublishOpt{fmt.Sprintf("%s: %#v", desc, err), "", "[redshift-tracking-copy-from-s3] ERROR Notification", cfg.Sns.Topic})
    if snsErr != nil { log.Println(fmt.Sprintf("SNS error: %#v during report of error writing to kafka: %#v", snsErr, err)) }
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
  cfg.Default.Pollsleepinseconds, err = config.GetInt64("default", "pollsleepinseconds")
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

func defaultCopyStmt(currentTable *string, currentBucket *string, currentPrefix *string) string {
  var buffer bytes.Buffer
  buffer.WriteString("COPY ")
  buffer.WriteString(*currentTable)
  buffer.WriteString(" FROM 's3://")
  buffer.WriteString(*currentBucket)
  buffer.WriteString("/")
  buffer.WriteString(*currentPrefix)
  buffer.WriteString("' credentials 'aws_access_key_id=")
  buffer.WriteString(cfg.Aws.Accesskey)
  buffer.WriteString(";aws_secret_access_key=")
  buffer.WriteString(cfg.Aws.Secretkey)
  buffer.WriteString("'")
  
  if cfg.Redshift.Emptyasnull { buffer.WriteString(" emptyasnull") }
  if cfg.Redshift.Blanksasnull { buffer.WriteString(" blanksasnull") }
  if cfg.Redshift.Fillrecord { buffer.WriteString(" fillrecord") }
  if cfg.Redshift.Maxerror > 0 { buffer.WriteString(" maxerror "); buffer.WriteString(fmt.Sprintf("%d", cfg.Redshift.Maxerror)) }
  if len(cfg.Redshift.Delimiter) > 0 { buffer.WriteString(" delimiter "); buffer.WriteString(fmt.Sprintf("'%s'", cfg.Redshift.Delimiter)) }

  buffer.WriteString(";")
  return buffer.String()
}

func createTableStatement(tableName *string, columnsJson *simplejson.Json, uniqueColumns *simplejson.Json, primaryKeyColumns *simplejson.Json) string {
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

  uniqueColumnsArry, err := uniqueColumns.Array()
  if err == nil && len(uniqueColumnsArry) > 0 { // error would mean there weren't any uniqueColmns defined 
    buffer.WriteString(",\nUNIQUE (")
    for i, _ := range uniqueColumnsArry {
      if i != 0 { buffer.WriteString(", ") }
      uniqueColumnName, err := uniqueColumns.GetIndex(i).String()
      if err != nil { reportError("Couldn't parse unique column name for table json: ", err) }
      buffer.WriteString(uniqueColumnName)
    }
    buffer.WriteString(")")
  }
  
  primaryKeyColumnsArry, err := primaryKeyColumns.Array()
  if err == nil && len(primaryKeyColumnsArry) > 0 { // error would mean there weren't any uniqueColmns defined 
    buffer.WriteString(",\nUNIQUE (")
    for i, _ := range primaryKeyColumnsArry {
      if i != 0 { buffer.WriteString(", ") }
      primaryKeyColumnName, err := primaryKeyColumns.GetIndex(i).String()
      if err != nil { reportError("Couldn't parse primary key column name for table json: ", err) }
      buffer.WriteString(primaryKeyColumnName)
    }
    buffer.WriteString(")")
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
  
  // ----------------------------- Load schema json and check in redshift and migrate if needed ----------------------------- 
  
  response, err := http.Get(cfg.Redshift.SchemaJsonUrl)
  if err != nil { reportError("Couldn't load schema url: ", err) }
  defer response.Body.Close()
  schemaContents, err := ioutil.ReadAll(response.Body)
  if err != nil { reportError("Couldn't read response body from schema url: ", err) }
  schemaJson, err := simplejson.NewJson(schemaContents)
  if err != nil { reportError("Couldn't parse json from schema url: ", err) }
  if cfg.Default.Debug { fmt.Printf("Read schema json:\n %#v\n", schemaJson) }
  
  // ----------------------------- Startup goroutine for each Bucket/Prefix/Table & Repeat migration check per table ----------------------------- 


  done := make(chan bool, len(cfg.Redshift.Tables))
  for i, _ := range cfg.Redshift.Tables {
    quitSignal := make(chan os.Signal, 1) 
    signal.Notify(quitSignal, os.Interrupt)
    
    go func(currentTable string, currentBucket string, currentPrefix string) {
      quitReceived := false
      
      go func() {
        <-quitSignal
        if cfg.Default.Debug { fmt.Printf("Quit signal received on %s watcher. Going down...\n",  currentTable) }
        quitReceived = true
      }()
  
      db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.User, cfg.Redshift.Password, cfg.Redshift.Database))
      if err != nil { reportError("Couldn't connect to redshift database: ", err) }
      rows, err := db.Query(fmt.Sprintf("select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where table_name = '%s' limit 1000", currentTable))
      if err != nil { reportError("Couldn't execute statement for INFORMATION_SCHEMA.COLUMNS: ", err) }
      if cfg.Default.Debug { fmt.Println("Looking for table, columns will display below.") }
      anyRows := false
      for rows.Next() {
        var column_name string
        var data_type string
        err = rows.Scan(& column_name, & data_type)
        if err != nil { reportError("Couldn't scan row for table: ", err) }
        if cfg.Default.Debug { fmt.Printf("   %s, %s\n",  column_name, data_type) }
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
        createTableStmt := createTableStatement(&currentTable, schemaJson.Get("tables").GetIndex(tableIndex).Get("columns"), schemaJson.Get("tables").GetIndex(tableIndex).Get("unique"), schemaJson.Get("tables").GetIndex(tableIndex).Get("primary_key"))
        if cfg.Default.Debug {
          fmt.Println("Creating table with:")
          fmt.Println(createTableStmt)
        }
        _, err = db.Exec(createTableStmt)
        if err != nil { reportError("Unable to create table: ", err) }
      } else {
        if cfg.Default.Debug { fmt.Println("Table found, will not migrate") }
      }
      
      // ----------------------------- Take a look at STL_FILE_SCAN on this Table to see if any files have already been imported -----------------------------
  
      for !quitReceived {
        if cfg.Default.Debug { fmt.Printf("Re-polling with %s watcher.\n",  currentTable) }
        loadedFiles := map[string]bool{}
        
        rows, err = db.Query(fmt.Sprintf("select * from STL_FILE_SCAN"))
        if err != nil { reportError("Couldn't execute STL_FILE_SCAN: ", err) }
        anyRows = false
        for rows.Next() {
          var (
            userid    int
            query     int
            slice     int
            name      string
            lines     int64
            bytes     int64
            loadtime  int64
            curtime   time.Time
          )
          err = rows.Scan(&userid, &query, &slice, &name, &lines, &bytes, &loadtime, &curtime)
          if err != nil { reportError("Couldn't scan row for STL_FILE_SCAN: ", err) }
          if cfg.Default.Debug { fmt.Printf("  Already loaded: %d|%d|%d|%s|%d|%d|%d|%s\n", userid, query, slice, name, lines, bytes, loadtime, curtime) }
          loadedFiles[strings.TrimSpace(name)] = true
          anyRows = true
        }
        
        // ----------------------------- If not: run generic COPY for this Bucket/Prefix/Table -----------------------------
        if !anyRows {
          copyStmt := defaultCopyStmt(&currentTable, &currentBucket, &currentPrefix)
          if cfg.Default.Debug { fmt.Printf("No records found in STL_FILE_SCAN, running `%s`\n", copyStmt) }
          _, err = db.Exec(copyStmt)
          if err != nil { reportError("Couldn't execute default copy statement: ", err) }
        } else {
        
        // ----------------------------- If yes: diff STL_FILE_SCAN with S3 bucket files list, COPY and missing files into this Table -----------------------------
          if cfg.Default.Debug { fmt.Printf("Records found, have to do manual copies from now on.\n") }
          s3bucket := s3.New(aws.Auth{cfg.Aws.Accesskey, cfg.Aws.Secretkey}, aws.Regions[cfg.Aws.Region]).Bucket(currentBucket)
        
          // list all missing files and copy in the ones that are missing
          nonLoadedFiles := []string{}
          keyMarker := ""
          moreResults := true
          for moreResults {
            if cfg.Default.Debug { fmt.Printf("Checking s3 bucket %s.\n", currentBucket) }
            results, err := s3bucket.List(currentPrefix, "", keyMarker, 0)
            if err != nil { reportError("Couldn't list default s3 bucket: ", err) }
            if cfg.Default.Debug { fmt.Printf("s3bucket.List returned %#v.\n", results) }
            if len(results.Contents) == 0 { break } // empty request, assume we found every file
            for _, s3obj := range results.Contents {
              if cfg.Default.Debug { fmt.Printf("Checking whether or not %s was preloaded.\n", strings.TrimSpace(s3obj.Key)) }
              if !loadedFiles[strings.TrimSpace(s3obj.Key)] {
                nonLoadedFiles = append(nonLoadedFiles, s3obj.Key)
              }
            }
            keyMarker = results.Contents[len(results.Contents)-1].Key
            moreResults = results.IsTruncated
          }
        
          if cfg.Default.Debug { fmt.Printf("Haven't ever loaded %#v.\n", nonLoadedFiles) }
          for _, s3key := range nonLoadedFiles {
            copyStmt := defaultCopyStmt(&currentTable, &currentBucket, &s3key)
            if cfg.Default.Debug { fmt.Printf("  Copying `%s`\n", copyStmt) }
            _, err = db.Exec(copyStmt)
            if err != nil { reportError("Couldn't execute default copy statement: ", err) }
          }
        
        
        }
        
        time.Sleep(time.Duration(cfg.Default.Pollsleepinseconds * 1000) * time.Millisecond)
      }
      

      done <- true
    }(cfg.Redshift.Tables[i], cfg.S3.Buckets[i], cfg.S3.Prefixes[i])
    

  }

  <-done // wait until the last iteration finishes before returning
}