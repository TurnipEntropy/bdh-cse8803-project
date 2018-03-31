select ce.subject_id, date_trunc('hour', ce.charttime) as charttime, di.label as item_desc, avg(ce.valuenum) as avg_value
  from chartevents ce, d_items di
  where di.itemid = ce.itemid
  and ce.itemid in (220179, 220050, 228152, 227243, 225167, 220059, 225309,
                    220180, 220051, 228151, 227242, 224643, 220060, 225310,
                    220045, 220210, 223761, 220277, 220739, 223900, 223901,
                    226756, 226758, 228112, 226757, 227011, 227012, 227014)
  group by ce.subject_id, charttime, item_desc