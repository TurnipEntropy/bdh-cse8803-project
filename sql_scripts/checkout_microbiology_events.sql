--select mbe.subject_id, d_items.itemid, d_items.label, mbe.*--, count(*)
--from microbiologyevents mbe, d_items
--where (mbe.spec_itemid = d_items.itemid)
--and mbe.subject_id = 38
--group by mbe.subject_id, d_items.itemid, d_items.label, 
--order by mbe.charttime
select * from microbiologyevents where subject_id = 64
order by charttime