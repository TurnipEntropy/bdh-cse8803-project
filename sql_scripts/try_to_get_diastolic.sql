select subject_id, icustay_id, charttime, combo as itemid, combo as diastolic_pressure
from (
  select subject_id, icustay_id, charttime, diastolic_aggregate(itemid, avg_value) as combo
  from (
    select subject_id, icustay_id, charttime, itemid, avg(valuenum) as avg_value
    from (
      select ce.subject_id as subject_id, icu.icustay_id as icustay_id, 
             ce.charttime as charttime, di.itemid as itemid, ce.valuenum as valuenum
      from chartevents ce, icustays icu, d_items di, patients p
      where ce.subject_id = icu.subject_id
      and ce.subject_id = p.subject_id
      and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
      and ce.charttime <= icu.outtime
      and ce.charttime >= icu.intime
      and ce.itemid in (220180, 220051, 228151, 227242, 224643, 220060, 225310)
    ) t1
    group by subject_id, icustay_id, charttime, itemid
    limit 10
  ) t2
  group by subject_id, icustay_id, charttime
) t3
  