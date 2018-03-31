select chartevents.itemid, d_items.label, count(*)--, chartevents.valuenum, chartevents.value, chartevents.valueuom
from chartevents, d_items 
where subject_id = 99991
and chartevents.itemid = d_items.itemid
group by chartevents.itemid, d_items.label