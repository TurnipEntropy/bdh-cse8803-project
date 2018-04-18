select *
from (
  select subject_id, icustay_id, charttime, itemid, avg(valuenum) as temp_C
  from (
    select ce.subject_id as subject_id, icu.icustay_id as icustay_id,
           date_trunc('hour', ce.charttime) as charttime,
           di.itemid as itemid, ce.valuenum as valuenum
    from chartevents ce, d_items di, icustays icu, patients p
    where di.itemid = ce.itemid
    and ce.subject_id = icu.subject_id
    and p.subject_id = ce.subject_id
    and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    and ce.charttime <= icu.outtime
    and ce.charttime >= icu.intime
    and ce.itemid = 223762
  ) pre_tc
  group by subject_id, charttime, icustay_id, itemid
) tc
full outer JOIN
(
  select subject_id, icustay_id, charttime, itemid, avg(valuenum) as temp_F
  from (
    select ce.subject_id as subject_id, icu.icustay_id as icustay_id,
           date_trunc('hour', ce.charttime) as charttime,
           di.itemid as itemid, ce.valuenum as valuenum
    from chartevents ce, d_items di, icustays icu, patients p
    where di.itemid = ce.itemid
    and ce.subject_id = icu.subject_id
    and p.subject_id = ce.subject_id
    and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    and ce.charttime <= icu.outtime
    and ce.charttime >= icu.intime
    and ce.itemid = 223761
  ) pre_tf
  group by subject_id, charttime, icustay_id, itemid
) tf
on tc.icustay_id = tf.icustay_id
and tc.charttime = tf.charttime
full outer JOIN
(
  select subject_id, icustay_id, charttime, itemid, avg(valuenum) as temp_C
  from (
    select ce.subject_id as subject_id, icu.icustay_id as icustay_id,
           date_trunc('hour', ce.charttime) as charttime,
           di.itemid as itemid, ce.valuenum as valuenum
    from chartevents ce, d_items di, icustays icu, patients p
    where di.itemid = ce.itemid
    and ce.subject_id = icu.subject_id
    and p.subject_id = ce.subject_id
    and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    and ce.charttime <= icu.outtime
    and ce.charttime >= icu.intime
    and ce.itemid = 223762
  ) pre_tc
  group by subject_id, charttime, icustay_id, itemid
)
