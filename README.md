Tracking Copy for S3 -> Redshift
==============================

An executable (intended to be run as a daemon) that polls S3 for added files, and runs a COPY command in Amazon Redshift for the new file.

Polls the Redshift `STL_FILE_SCAN` table to keep informed 

Stops when `SIGINT` (control-c) is recieved.

Install
--------------------

First, get all dependencies with `go get .`

Don't forget to make your own config file, by copying and updating the example one, found at `redshift-tracking-copy-from-s3.example.properties`.


Run
--------------------
```bash
go run main.go -c <config_file_path>
```

* `-c` Defaults to conf.properties in the current working directory

Deployment
--------------------

There's a Chef recipe to fetch and build this on any server, at: https://github.com/crowdmob/chef-redshift-tracking-copy-from-s3

Configuration Options
--------------------

While most of the configuration options are self-expanatory, here are the ones that can use some helpful explanation:


License and Author
===============================
Author:: Matthew Moore

Copyright:: 2013, CrowdMob Inc.


Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

