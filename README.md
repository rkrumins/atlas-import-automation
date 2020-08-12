# Atlas Hive Import Automation

This is a repository for importing specific hive databases and tables over SSH. This is meant to be used in cases where Atlas Hive Hook is disabled and one is only interested in importing a subset of Hive databases and tables.

There are following design options:
- Maintain a file of sources.txt that one is interested in and SCP this file to /tmp folder on node and then execute
- Construct a list of commands that will be triggered, however issue with this is the fact that it is obsolete

import-hive.sh -f $PATH/sources.txt

SCP file accross
Enable sudo su - into separate account
Renew keytab before running
