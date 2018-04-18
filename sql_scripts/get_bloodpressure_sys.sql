select subject_id, icustay_id, charttime, (combo).id as itemid, (combo).value as value from(
select subject_id, icustay_id, charttime, systolic_aggregate(itemid, avg_value) as combo from(
  select subject_id, icustay_id, charttime, itemid, avg(valuenum) as avg_value from (
    select ce.subject_id as subject_id, icu.icustay_id as icustay_id,
           date_trunc('hour', ce.charttime) as charttime, 
           di.itemid as itemid, ce.valuenum as valuenum
    from chartevents ce, d_items di, icustays as icu, patients as p
    where di.itemid = ce.itemid
    and ce.subject_id = icu.subject_id
    and p.subject_id = ce.subject_id
    and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    and ce.charttime <= icu.outtime
    and ce.charttime >= icu.intime
    and ce.itemid in (220179, 220050, 228152, 227243, 224167, 220059, 225309)
   ) t1 
   group by subject_id, charttime, icustay_id, itemid
) t2
 where t2.avg_value != 0
 group by subject_id, icustay_id, charttime
 order by subject_id, charttime
) t3