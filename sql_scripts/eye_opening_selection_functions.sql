create function select_eye_opening(res int_dub, id integer, val double precision) returns int_dub
  as 'select case
        when res.value = 0 then cast(row(id, val) as int_dub)
        when val = 0 then res
        when id = res.id then cast(row(id, (res.value + val) / 2.0) as int_dub)
        when res.id = 220739 then res
        when res.id = 226756 then cast(row(id, val) as int_dub)
        else cast(row(0, 0.0) as int_dub)
      end as final_result'
    language sql
    immutable
    returns null on null input;

create aggregate eye_opening_aggregate(integer, double precision)
(
  sfunc = select_eye_opening,
  stype = int_dub,
  initcond = '(0, 0.0)'
)