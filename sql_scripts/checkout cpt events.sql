select cptevents.subject_id, cptevents.cpt_cd, d_cpt.sectionheader, count(*)
from cptevents, d_cpt
where cptevents.subject_id = 21
and cptevents.cpt_cd < d_cpt.maxcodeinsubsection
and cptevents.cpt_cd > d_cpt.mincodeinsubsection
group by cptevents.subject_id, cptevents.cpt_cd, d_cpt.sectionheader