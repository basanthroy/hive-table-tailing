#!/usr/bin/env bash

hive -e "use radiumone;alter table connect_overlap_categories set location '/data/radiumone/connect/overlap/top_content_categories/' ; msck repair table connect_overlap_categories"
hive -e "use radiumone;alter table connect_overlap_cities set location '/data/radiumone/connect/overlap/top_cities/' ; msck repair table connect_overlap_cities"
hive -e "use radiumone;alter table connect_overlap_domains set location '/data/radiumone/connect/overlap/top_domains/' ; msck repair table connect_overlap_domains"
hive -e "use radiumone;alter table connect_overlap_media_types set location '/data/radiumone/connect/overlap/top_media_types/' ; msck repair table connect_overlap_media_types"

hive -e "use radiumone; msck repair table connect_overlap_cities"


#hdfs dfs -rm -r /data/radiumone/connect/overlap/top*
#hdfs dfs -rm -r /data/radiumone/connect/overlap/media_types