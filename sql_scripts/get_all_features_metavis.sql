select coalesce(
  bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id, tf.subject_id,
  spo.subject_id, e.subject_id, v.subject_id, m.subject_id, age.subject_id
) as subject_id,
coalesce(
  bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id, tf.icustay_id,
  spo.icustay_id, e.icustay_id, v.icustay_id, m.icustay_id, age.icustay_id
) as icustay_id,
coalesce(
  bpd.charttime, bps.charttime, hr.charttime, rr.charttime, tf.charttime,
  spo.charttime, e.charttime, v.charttime, m.charttime
) as charttime,
bp_dia, bp_sys, heart_rate, resp_rate, temp_F, spo2, eye_opening, verbal, motor, cast(age as integer) as age
from (
  (select (get_bp_dia_table()).*) bpd
  full outer join
  (select(get_bp_sys_table()).*) bps
  on bps.charttime = bpd.charttime
  and bps.subject_id = bpd.subject_id
  and bps.icustay_id = bpd.icustay_id
  full outer join
  (
    select subject_id, icustay_id, charttime, avg_value as heart_rate
    from (select (get_single_feature_table(220045)).*) t1
  ) hr
  on coalesce(bpd.charttime, bps.charttime) = hr.charttime
  and coalesce(bpd.subject_id, bps.subject_id) = hr.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id) = hr.icustay_id
  full outer join
  (
    select subject_id, icustay_id, charttime, avg_value as resp_rate
    from (select (get_single_feature_table(220210)).*) t1
  ) rr
  on coalesce(bpd.charttime, bps.charttime, hr.charttime) = rr.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id) = rr.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id) = rr.icustay_id
  full outer join
  (select(get_temperature_table()).*) tf
  on coalesce(bpd.charttime, bps.charttime, hr.charttime, rr.charttime) = tf.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id) = tf.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id) = tf.icustay_id
  full outer join
  (
    select subject_id, icustay_id, charttime, avg_value as spo2
    from (select(get_single_feature_table(220277)).*) t1
  ) spo
  on coalesce(bpd.charttime, bps.charttime,
              hr.charttime, rr.charttime, tf.charttime) = spo.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id,
               tf.subject_id) = spo.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id,
               tf.icustay_id) = spo.icustay_id
  full outer join
  (select(get_eye_opening_table()).*) e
  on coalesce(bpd.charttime, bps.charttime,
              hr.charttime, rr.charttime, tf.charttime,
              spo.charttime) = e.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id,
               tf.subject_id, spo.subject_id) = e.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id,
               tf.icustay_id, spo.icustay_id) = e.icustay_id
  full outer join
  (select(get_verbal_table()).*) v
  on coalesce(bpd.charttime, bps.charttime,
              hr.charttime, rr.charttime, tf.charttime,
              spo.charttime, e.charttime) = v.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id,
               tf.subject_id, spo.subject_id, e.subject_id) = v.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id,
               tf.icustay_id, spo.icustay_id, e.icustay_id) = v.icustay_id
  full outer join
  (select(get_motor_table()).*) m
  on coalesce(bpd.charttime, bps.charttime,
              hr.charttime, rr.charttime, tf.charttime,
              spo.charttime, e.charttime, v.charttime) = m.charttime
  and coalesce(bpd.subject_id, bps.subject_id, hr.subject_id, rr.subject_id,
               tf.subject_id, spo.subject_id, e.subject_id,
               v.subject_id) = m.subject_id
  and coalesce(bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id,
               tf.icustay_id, spo.icustay_id, e.icustay_id,
               v.icustay_id) = m.icustay_id
  full outer join
  (
    select subject_id, icustay_id, age
    from (
      select p.subject_id, icu.icustay_id,
             extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 as age
      from icustays icu, patients p
      where p.subject_id = icu.subject_id
      and icu.dbsource = 'metavision'
      and extract(epoch from (icu.intime - p.dob))/60.0/60.0/24.0/365.242 > 15
    ) t1
  ) age
  on coalesce(
    bpd.icustay_id, bps.icustay_id, hr.icustay_id, rr.icustay_id,
    tf.icustay_id, spo.icustay_id, e.icustay_id, v.icustay_id, m.icustay_id
  ) = age.icustay_id
)
where age < 150
order by subject_id, charttime
