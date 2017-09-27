# some notes:

For archive-cd index there are the following scripted fields:

Name | Language | Script
---- | -------- | ------
discs_per_hour | painless | ```return doc['total_discs'].value/doc['hours'].value;```
