select subject_id, icustay_id, min(charttime) from (
select t1.subject_id, t1.icustay_id, charttime, t1.starttime, max(sofa_24hours), min(sofa_24hours), sofa_dif_agg(sofa_24hours) as dif
from (
  select i.subject_id, i.icustay_id, m.spec_type_desc, date_trunc('hour', m.charttime) as charttime, m.org_name, i.starttime, i.category
  from (
    select inputevents_mv.*, d_items.category 
    from inputevents_mv, d_items, icustays
    where inputevents_mv.itemid = d_items.itemid
    and icustays.subject_id = inputevents_mv.subject_id
  ) i, microbiologyevents m
  where i.subject_id = m.subject_id
  and m.charttime is not null
  and m.org_name is not null
  and i.category = 'Antibiotics'
  and (m.spec_itemid in (70011, 70012, 70060, 70017, 70076, 70070, 70062, 70079))
  and (extract(epoch from (i.starttime - m.charttime))/60.0/60.0 between 0.0 and 72.0 or
       extract(epoch from (m.charttime - i.starttime))/60.0/60.0 between 0.0 and 24.0)
  --and i.subject_id = 188
  order by charttime, starttime
) t1
inner join
(
  select t3.subject_id, t3.icustay_id, t3.sofa_24hours, t3.starttime, t3.endtime
  from (
    select t5.subject_id, t5.icustay_id, t5.sofa_24hours, t5.starttime, t5.endtime
    from (
      select icu.subject_id, icu.icustay_id
      from icustays icu, patients p
      where icu.subject_id = p.subject_id
      and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    ) t4
    inner join
    (
      select icu.subject_id, icu.icustay_id, ps.sofa_24hours, ps.starttime, ps.endtime
      from icustays icu, pivoted_sofa ps
      where icu.icustay_id = ps.icustay_id
    )t5
    on t4.icustay_id = t5.icustay_id
    and t4.subject_id = t5.subject_id
  )t3
) t2
on t1.icustay_id = t2.icustay_id
and t1.subject_id = t2.subject_id
and t2.starttime >= charttime 
and t2.starttime <= t1.starttime
group by t1.subject_id, t1.icustay_id, charttime, t1.starttime
) grouped 
where dif[2] >= 2
group by subject_id, icustay_id