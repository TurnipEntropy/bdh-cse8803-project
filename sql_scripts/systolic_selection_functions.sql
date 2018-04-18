create function select_systolic(res int_dub, id integer, val double precision) returns int_dub
  as 'select case
        when res.value = 0 then cast(row(id, val) as int_dub)
        when val = 0 then res
        when id = res.id then cast(row(id, (res.value + val) / 2.0) as int_dub)
        when res.id = 220179 then res
        when res.id = 220050 then
          case
            when id = 220179 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 220059 then
          case
            when id = 220179 then cast(row(id,val) as int_dub)
            when id = 220050 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 225309 then
          case
            when id = 220179 then cast(row(id,val) as int_dub)
            when id = 220050 then cast(row(id,val) as int_dub)
            when id = 220059 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 224643 then
          case
            when id = 220179 then cast(row(id,val) as int_dub)
            when id = 220050 then cast(row(id,val) as int_dub)
            when id = 220059 then cast(row(id,val) as int_dub)
            when id = 225309 then cast(row(id,val) as int_dub)
            else res
          end
        when id = 227243 then
          case
            when res.id = 228152 then cast(row(id,val) as int_dub)
            else res
          end
        else res
      end as final_result'
    language sql
    immutable
    returns null on null input;

create aggregate systolic_aggregate(integer, double precision)
(
  sfunc = select_systolic,
  stype = int_dub,
  initcond = '(0, 0.0)'
)