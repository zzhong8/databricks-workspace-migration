-- Databricks notebook source
desc premium360_dv.sat_answer_custom_dataentry 

-- COMMAND ----------

desc premium360_dv.sat_store_alerts

-- COMMAND ----------

select * from premium360_dv.sat_store_alerts
limit 10

-- COMMAND ----------

desc rex_dv.sat_service_request

-- COMMAND ----------

select * from rex_dv.sat_service_request
limit 10

-- COMMAND ----------

desc rex_dv.sat_service_request_attribute

-- COMMAND ----------

select * from rex_dv.sat_service_request_attribute
limit 10

-- COMMAND ----------

desc rex_dv.sat_linked_product_source

-- COMMAND ----------

select * from rex_dv.sat_linked_product_source
limit 10

-- COMMAND ----------

desc premium360_analytics_im.vw_fact_service_order

-- COMMAND ----------

select * from premium360_analytics_im.vw_fact_service_order
limit 10

-- COMMAND ----------

desc acosta_retail_analytics_im.vw_dimension_store

-- COMMAND ----------

select * from acosta_retail_analytics_im.vw_dimension_store
limit 10

-- COMMAND ----------

desc premium360_analytics_im.vw_dimension_question

-- COMMAND ----------

select * from premium360_analytics_im.vw_dimension_question
limit 10

-- COMMAND ----------

desc premium360_dv.sat_question_detail

-- COMMAND ----------

select * from premium360_dv.sat_question_detail
limit 10

-- COMMAND ----------

desc premium360_analytics_im.vw_dimension_survey_detail

-- COMMAND ----------

select * from premium360_analytics_im.vw_dimension_survey_detail
limit 10

-- COMMAND ----------

select 
c.qid as question_id, 
c.id as response_id, 
to_date(c.ts_datetime) as response_last_modified_date, 
q.response_type as response_type_code, 
q.response_type as response_type, 
so.store_id, 
so.store_tdlinx_id, 
so.holding_id, 
so.channel_id,
sd.premium_client_id,
nvl2(sr.id, coalesce(lc.mdm_client_id, sd.client_id), nvl2( pc.parent_client_id, COALESCE(lc.mdm_client_id, COALESCE(mdmitem.client_id, mdmitem2.client_id)), sd.client_id ) ) as client_id,
nvl2(sr.id, coalesce(lc.master_client_id, sd.master_client_id), nvl2( pc.parent_client_id, COALESCE(lc.master_client_id, COALESCE(mdmitem.master_client_id, mdmitem2.master_client_id)), sd.master_client_id) ) as master_client_id,
COALESCE(concat('PRSK', lp.productsourcekey1), COALESCE(mdmitem.item_level_id, mdmitem2.item_level_id)) as item_level_id,
COALESCE(lp.productsourcekey1, COALESCE(mdmitem.item_dimension_id, mdmitem2.item_dimension_id)) as item_dimension_id, 
'PRSK' as item_entity_id, 
so.country_id as product_country_id, 
so.country_id , 
c.soid as call_id, 
so.calendar_key as calendar_key, 
cast(date_format(so.date_call_planned, 'yMMdd') as int) as call_planned_date_key,
so.tm_executed_id as tm_executed_emp_id, 
so.executed_retail_team_id as um_executed_emp_id, 
so.mgmtgroupid as profile_id, 
so.mgmtgroupid as profile_name, 
so.mgmtgroupid as profile_family, 
c.answer as response, 
coalesce(nullif(qd.choicetextabbrev, ''), c.answer) as response_customer_report,
case when q.response_type in ('Numeric', 'Currency', 'Duration') then cast(trim(answer) as decimal(19,2)) when answer = '' or answer = '<<n/a>>' then 0 else 1 End as response_value,
c.responseid as mchoice_id, 
sra.attributevalue as shelfcode, 
sr.priority as priority, 
Case when q.response_type = 'Free Text' then c.answer else NULL END as comment, 
so.executed_retail_team_id as retail_team_id, so.store_id as store_with_answer, 
Case when isnull(u.url) then 0 else 1 end as num_attachments, 
case when u.url is null then null else Concat('https://www.qtraxweb.com/internal/reports/photoview.asp?CycleID=',ca.cycle_id,'&ActionID=',q.assessment_id,'&SOID=',so.bus_call_id,'&REJ=0&cc=0&nr=1') end as url, 
u.url as image_URL, 
u.photoreviewstatusid as photoreviewstatusid, 
case u.photoreviewstatusid
when 1 then 'Approve'
when 3 then 'Approved - Excellent'
when 5 then 'Unapprove'
when 6 then 'REJECT!  Level 1 Execution Error'
when 8 then 'REJECT! Level 2 Execution Error'
when 9 then 'REJECT! Level 3 Execution Error'
when 10 then 'REJECT! - Insufficient'
when 12 then 'REJECT! - Duplicate'
when 14 then 'Approved w/ Issues'
else 'Unkown' end as photoreviewstatusid_description, 
-1 as store_call_frequency_key, 
1000 as call_complete_status_id, 
so.call_completed_date, 
Case when answer = '' or answer = '<<n/a>>' then 19000101000000 else date_format(so.call_completed_date, "yyyyMMddHHmmss") + so.visit_rank end as valid_response_rank,
qd.responserank as response_rank, 
Case when qd.iscompliant = 'False' then 0 else 1 END as compliant, 
Case when qd.isresolved = 'False' then 0 when qd.isresolved = 'True' then 1 else null END as valid_flag,
cast(NULL as string) as library_id, 
1 as response_rank_by_date, 
1 as response_rank_by_earliest_date, 
to_date(c.ts_datetime) as last_edit_date, 
Date_format(c.ts_datetime, 'H:m:s') as last_edit_time, 
cast(NULL as string) as action_required, 
so.call_type_id as call_type_code, 
so.call_type_id, 
so.team_executed_code, 
so.team_planned_code, 
cast(NULL as string) as void_origin, 
cast(NULL as string) as system_response, 
cast(NULL as string) as facing, 
cast(NULL as string) as pricing, 
cast(NULL as string) as tagged, 
cast(NULL as string) as not_authorized, 
cast(NULL as string) as in_stock, 
cast(NULL as string) as out_of_stock,
cast(NULL as string) as call_back, 
cast(NULL as string) as void_response_type, 
cast(NULL as string) as response_category, 
so.service_id, 
c.audit_batch_id as batch_id, 
cast(NULL as string) as image_abstract_predicted_tag, 
cast(NULL as string) as image_abstract_audit_tag, 
cast(NULL as string) as image_abstract_accepted_tag, 
cast(NULL as string) as image_display_type_predicted_tag, 
cast(NULL as string) as image_display_type_audit_tag, 
cast(NULL as string) as image_display_type_accepted_tag, 
cast(NULL as string) as image_outlier_predicted_tag, 
cast(NULL as string) as image_outlier_audit_tag, 
cast(NULL as string) as image_outlier_accepted_tag, 
cast(NULL as string) as image_userdefined_audit_tag, 
cast(NULL as string) as image_userdefined_predicted_tag, 
cast(NULL as string) as image_userdefined_accepted_tag, 
cast(NULL as string) as tag_last_modified_date, 
cast(NULL as string) as missed_date, 
a.soid as bus_call_id, 
ivs.total_intervention_effect, 
ivs.total_impact, 
so.job_id, 
so.mgmtgroupid as management_group_id, 
Case when c.answer = '' or c.answer = '<<n/a>>' then 0 else 1 end as no_response_flag, 
nvl2(COALESCE(concat('PRSK', lp.productsourcekey1), COALESCE(mdmitem.item_level_id, mdmitem2.item_level_id)), concat(s.subbanner_id,COALESCE(concat('PRSK', lp.productsourcekey1), COALESCE(mdmitem.item_level_id, mdmitem2.item_level_id))), NULL) as apl_id,
Case when q.internal_access_flag = 1 then 1 when so.mgmtgroupid = 94 then 1 else 0 end as exclude_data_flag,
lsv.attributevalue as lost_sales_value,
so.market_id,
sd.business_unit_id,
sr.description as alert_description,
sr.id as alert_id,
sr.begindate,
sr.enddate,
sr.priority,
sr.datatimestamp,
Case when sr.id is not null then 'P360_DP' else 'P360' END as Source 
From premium360_dv.sat_answer_custom_dataentry c 
Left join premium360_dv.sat_store_alerts a on c.externaldataid = a.id and c.soid = a.soid and a.audit_current_ind = 1 
Left Join rex_dv.sat_service_request sr on  lower(a.alertid) = sr.id and sr.audit_current_ind = 1 and sr.isdeleted = 'False' and sr.`date` is not null
Left join (Select servicerequestid, attributevalue 
            from rex_dv.sat_service_request_attribute 
            where attributename = 'ItemNumber' and audit_current_ind = 1) as sra on sra.servicerequestid = sr.id 
left join (Select servicerequestid, attributevalue 
            from rex_dv.sat_service_request_attribute 
            where attributename = 'LostSalesValue' and audit_current_ind = 1) as lsv on lsv.servicerequestid = sr.id  
Left join rex_dv.sat_linked_product_source lp on lp.servicerequestid = sr.id and lp.audit_current_ind = 1 
Inner join premium360_analytics_im.vw_fact_service_order so on so.call_id = c.soid
left join acosta_retail_analytics_im.vw_dimension_store s on so.store_id = s.store_id
Inner join premium360_analytics_im.vw_dimension_question q on c.qid = q.question_id 
Left join premium360_dv.sat_question_detail qd on c.responseid = qd.qdid and qd.audit_current_ind = 1 
Inner join premium360_analytics_im.vw_dimension_survey_detail sd on q.assessment_id = sd.assessment_id
Left JOIN premium360_dv.sat_custom_dataentry_photolink cdp on cdp.externalid = c.externaldataid and cdp.audit_current_ind = 1 and c.qid = q.question_id and q.response_type = 'Image'
Left join premium360_dv.sat_uploaded_file u on u.fileid = cdp.fileid and u.audit_current_ind = 1
Left JOin premium360_analytics_im.vw_lookup_client lc 
on sr.clientid = lc.client_id
left join premium360_analytics_im.vw_lookup_parent_client pc 
on pc.parent_client_id = sd.premium_client_id
Left join acosta_retail_analytics_im.vw_ds_intervention_summary ivs on (c.id = ivs.response_id)
left join mdm_raw.hp_holding p on so.holding_id = p.holdingid
Left join premium360_analytics_im.vw_lookup_item_level mdmitem on  lpad(a.upc, 13,'000000000000') = mdmitem.upc_normal 
            and so.country_id = mdmitem.country_id 
            and COALESCE(p.holdingid,0) = mdmitem.holding_id
            and mdmitem.row_id = 1
Left join premium360_analytics_im.vw_lookup_item_level mdmitem2 on  lpad(a.upc, 13,'000000000000') = mdmitem2.upc_full 
            and so.country_id = mdmitem2.country_id 
            and COALESCE(p.holdingid,0) = mdmitem2.holding_id
            and mdmitem2.row_id = 1
Left join premium360_analytics_im.vw_dimension_assessment_cycle ca
on ca.assessment_id = q.assessment_id
and ca.job_id = so.job_id            
Where c.audit_current_ind = 1
And so.service_order_status = 'Reported'
and lc.mdm_client_id = 17686 -- Beiersdorf
and q.question_type = 'Data Led Alerts'
and s.banner_id = 7743
and so.calendar_key between 20240101 and 20240801
