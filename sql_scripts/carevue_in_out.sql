select subject_id, date_trunc('hour', intime) as intime, date_trunc('hour', outtime + interval '30 minute') as outtime 
from icustays
where dbsource = 'carevue'
and subject_id not in (499, 8733, 10273, 14712, 19967, 20861)
order by subject_id