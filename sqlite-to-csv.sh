#! /bin/sh
sqlite3 result.db <<EOF
.headers on
.mode csv
.output testselection.csv
select * from testselection;
.output testselection_ri.csv
select * from testselection_ri;
.output testselection_denormalized.csv
select testselection.key,qm,granularity,project,version,base,select_from,algorithm,run_id,fault_detection,testselection.determined_by,selected_triggers,selected_count,selected_tgs,selected_time,relevant_tgs,base_triggers,base_count,base_tgs,base_time,base_relevant_triggers,base_relevant_count,base_relevant_tgs,base_relevant_time,aug_triggers,aug_count,aug_tgs,aug_time,aug_relevant_triggers,aug_relevant_count,aug_relevant_tgs,aug_relevant_time,aug_additional_triggers,aug_additional_count,aug_additional_tgs,aug_additional_time 
    from testselection left join testselection_ri using (qm, granularity, project, version, base, select_from);
.exit
EOF
