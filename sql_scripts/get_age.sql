select subject_id, icustay_id, age from (
  select p.subject_id, icu.icustay_id, extract(epoch from (icu.intime - p.dob))/60./60./24./365.242 as age
  from icustays icu, patients p
  where p.subject_id = icu.subject_id
  and icu.dbsource = 'metavision'
  and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
) t1