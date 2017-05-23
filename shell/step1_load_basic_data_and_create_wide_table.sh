#!/bin/bash
hive -f ../script/dim_feature_v3/1_creata_basic_table.sql
hive -f ../script/dim_feature_v3/2_basic_wide_table.sql
