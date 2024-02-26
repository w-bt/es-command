# ES Command

Script for migrating elasticsearch doc

### Requirements

- Golang 1.21
- ElasticSearch 6.8 or later

# How it works

## Command
- Change command/const.go
```
sourceHostName = "http://elastic:password@localhost:9200"
destHostName   = "http://elastic:password@localhost:9201"
```
- Go build -a

| command                               | description                                                           | example                                                        |
|---------------------------------------|-----------------------------------------------------------------------|----------------------------------------------------------------|
| aliases                               | get list of index alias                                               | ./es-command aliases                                           |
| add_alias                             | add alias, need file aliases.json generated from ./es-command aliases | ./es-command add_alias                                         |
| threshold                             | check doc threshold, based on created_at and updated_at               | ./es-command threshold                                         |
| duplicate_all                         | duplicate all indices including its config                            | ./es-command duplicate_all                                     |
| duplicate <index name>                | duplicate index name including its config                             | ./es-command duplicate products                                |
| remove_all                            | remove all indices                                                    | ./es-command remove_all                                        |
| remove <index name>                   | remove specific index name                                            | ./es-command remove products                                   |
| reindex_all                           | reindex all indices                                                   | ./es-command reindex_all                                       |
| reindex <index name>                  | reindex specific index name                                           | ./es-command reindex products                                  |
| manual_reindex_chunk <date> <refresh> | manual reindex based on threshold                                     | ./es-command manual_reindex_chunk 2023-09-05T00:00:00.000 true |