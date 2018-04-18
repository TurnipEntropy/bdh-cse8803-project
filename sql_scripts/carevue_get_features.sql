select * from (
  select ce.subject_id, date_trunc('hour', ce.charttime) as charttime, di.itemid as itemid, avg(ce.valuenum) as avg_value
  from chartevents ce, d_items di
  where di.itemid = ce.itemid
  and ce.itemid in (211, 618, 646, 678, 442, 51, 8368)
  and ce.subject_id not in (499, 8733, 10273, 14712, 19967, 20861)
  group by ce.subject_id, charttime, di.itemid
) t1
where t1.avg_value is not null
limit 1000000