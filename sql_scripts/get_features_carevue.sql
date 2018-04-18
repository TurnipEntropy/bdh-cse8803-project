select ce.subject_id, date_trunc('hour', ce.charttime) as charttime, di.label as item_desc, avg(ce.valuenum) as avg_value
  from chartevents ce, d_items di
  where di.itemid = ce.itemid
  and ce.itemid in (211, 618, 646, 678, 442, 51, 8368)
  group by ce.subject_id, charttime, item_desc