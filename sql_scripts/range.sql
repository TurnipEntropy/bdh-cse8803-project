select subject_id, min(charttime) from (
select t1.subject_id, charttime, t1.starttime, max(sofa_24hours), min(sofa_24hours), sofa_dif_agg(sofa_24hours) as dif
from (
  select i.subject_id, m.spec_type_desc, date_trunc('hour', m.charttime) as charttime, m.org_name, i.starttime, i.category
  from (
    select inputevents_mv.*, d_items.category 
    from inputevents_mv, d_items
    where inputevents_mv.itemid = d_items.itemid
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
  select icu.subject_id, ps.sofa_24hours, ps.starttime, ps.endtime
  from icustays icu, pivoted_sofa ps
  where icu.icustay_id = ps.icustay_id
) t2
on t1.subject_id = t2.subject_id and t2.starttime >= charttime and t2.starttime <= t1.starttime
group by t1.subject_id, charttime, t1.starttime
) grouped 
where dif[2] >= 2
group by subject_id