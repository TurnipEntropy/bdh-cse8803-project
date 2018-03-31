select labevents.subject_id, labevents.itemid, labevents.flag, d_labitems.label, count(*) 
from labevents, d_labitems
where labevents.subject_id = 38
and d_labitems.itemid = labevents.itemid
group by labevents.subject_id, labevents.itemid, labevents.flag, d_labitems.label