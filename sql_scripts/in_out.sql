select subject_id, date_trunc('hour', intime) as intime, date_trunc('hour', outtime + interval '30 minute') as outtime 
from icustays
where dbsource = 'metavision'
order by subject_id