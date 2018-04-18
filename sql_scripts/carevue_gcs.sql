select icu.subject_id, gcs.charttime, gcs.gcs
from icustays icu, pivoted_gcs gcs
where icu.icustay_id = gcs.icustay_id
and dbsource = 'carevue'
and icu.subject_id not in (499, 8733, 10273, 14712, 19967, 20861)