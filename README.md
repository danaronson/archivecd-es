# archivecd-es
software to upload archivecd logs to elastic search

This needs to be run from somewhere that has write access to the ES cluster.  When run outside of the archive this can be done by means of an ssh tunnel.


the uploaded uploads log files from the archivecd software.  Relevant documented regular expressions:
  * Log line patterns: `"^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d,\d+)\s+([^\s]+\(.*\))\s+([^\s]+\|
)\s+([^\s]+)\s+([^\s]+\.py)\s+(.*)$"`
