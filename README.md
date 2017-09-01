# archivecd-es
software to upload archivecd logs to elastic search

This needs to be run from somewhere that has write access to the ES cluster.  When run outside of the archive this can be done by means of an ssh tunnel.


The uploaded uploads log files from the archivecd software.  Each log line is parsed into 6 fields using the regular expression `^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d,\d+)\s+([^\s]+\(.*\))\s+([^\s]+\|
)\s+([^\s]+)\s+([^\s]+\.py)\s+(.*)$`.  The fields are:
  * Timestamp
  * Python Thread
  * Log Level
  * Module
  * Python File
  * Log Message

We also use regular expressions to find if certain things exist in the actual log messages.  The things (which regular expressions) that we search for are:
  * Project Finished: `^project_finished: <class 'iaclient.Finished'>\((.*)\)$`
  * Album Identified: `^identify_album_finished: <class 'iaclient.Finished'>\((.*)\)$`
  * Rename Scan Lines (to count images): `^.*rename_scan.*\sto\s*(.*)$`
  * Music Brainz id: `^MusicBrainz disc id (.*)$`
  * CDDB id: `^CDDB disc id: (.*)$`