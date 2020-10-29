# SSB-DB2

Status: Design

## TODO

Refactor indexes to be:
 - core (including EBT ones)
 - social (mentions, roots, vote links, profiles)
 
- Add versions to indexes like flume
- Add ability to reindex a subset of messages - needed for private groups

Consider fixing validate, the state is akward for partial replication,
maybe use seperate validate for ooo

Consider making streams wait for index sync by default

Better pattern for waiting for message + index update after posting
message (like getGraphForFeed does).

Fix createHistoryStream to use new seq posibility

lite friends
 - maybe no index for contacts, just use jitdb

Add methods for migrating data from old flume
 - Code for this in jitdb repo

Maybe ability to sync between old ssb-db and new one, so an app can
migrate slowly.

private messages
 - tribes
 - encrypted indexes?
 
## Notes
 - keys is handled by ssb-config

