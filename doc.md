# some notes:

For archive-cd index there are the following scripted fields:

Name | Language | Script
---- | -------- | ------
discs_per_hour | painless | ```return doc['total_discs'].value/doc['hours'].value;```
images_per_hour | painless | ```return doc['total_images'].value/doc['hours'].value;```
projects_per_hour | painless | ```return doc['total_projects'].value/doc['hours'].value;```
projects_link | painless | ```def month_value = doc['@timestamp'].date.monthOfYear; def month_string = ""; if (month_value < 10) { month_string = "0" + month_value; } else { month_string = month_value; } def day_value = doc['@timestamp'].date.dayOfMonth; def day_string = ""; if (day_value < 10) { day_string = "0" + day_value; } else { day_string = day_value; } def date = doc['@timestamp'].date.year + "-" + month_string + "-" + day_string; def operator = params._source.operator; return "http://crawl-monitor.us.archive.org:82/app/kibana?#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'" + date + "T00:00:00.000Z',mode:absolute,to:'" + date + "T23:59:59.999Z'))&_a=(columns:!(operator,discs,image_count),index:'archivecd-*',interval:auto,query:(query_string:(analyze_wildcard:!t,query:'_type:project++AND+operator:%22" + operator + "%22')),sort:!('@timestamp',desc),uiState:(spy:(%5Cmode:(fill:!f,name:request),mode:(fill:!f,name:!n))))";```

