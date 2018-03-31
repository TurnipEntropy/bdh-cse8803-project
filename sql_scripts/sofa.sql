select icu.subject_id, ps.*--, count(*)
from icustays icu, pivoted_sofa ps
where icu.icustay_id = ps.icustay_id
and icu.subject_id = 188
order by starttime
--group by icu.subject_id, ps.sofa_24hours