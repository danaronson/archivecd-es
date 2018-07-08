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

-----------
the primary script is upload.py which does forks and runs two processes.
the first one parses log files and creates an item in a log schema for each log line AND a single item in a project schema for each project (analagous to an ia item)
the second fork looks for curate states of 'dark', 'freeze', 'undark' and 'NULL' for each item that has been uploaded to the archive and changes the curate state of the corresponding es project.

that script also updates an ES schema called track_info which contains information (rip speed, time, etc) for each track of each disk in a project
there is another script called: 'update_hours_worked_data.py' which reads a google spreadsheet that tracks operator working hours (by day, email, and hours/day) and updates an ES schema called 'hours_worked'.

