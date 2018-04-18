create type int_dub as (id int, value double precision);
create function select_diastolic(res int_dub, id integer, val double precision) returns int_dub
  as 'select case
        when res.value = 0 then cast(row(id, val) as int_dub)
        when val = 0 then res
        when id = res.id then cast(row(id, (res.value + val) / 2.0) as int_dub)
        when res.id = 220180 then res
        when res.id = 220051 then
          case
            when id = 220180 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 220060 then
          case
            when id = 220180 then cast(row(id,val) as int_dub)
            when id = 220051 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 225310 then
          case
            when id = 220180 then cast(row(id,val) as int_dub)
            when id = 220051 then cast(row(id,val) as int_dub)
            when id = 220060 then cast(row(id,val) as int_dub)
            else res
          end
        when res.id = 224643 then
          case
            when id = 220180 then cast(row(id,val) as int_dub)
            when id = 220051 then cast(row(id,val) as int_dub)
            when id = 220060 then cast(row(id,val) as int_dub)
            when id = 225310 then cast(row(id,val) as int_dub)
            else res
          end
        when id = 227242 then
          case
            when res.id = 228151 then cast(row(id,val) as int_dub)
            else res
          end
        else res
      end as final_result'
    language sql
    immutable
    returns null on null input;

select select_diastolic(cast(row(225310, 1.0) as int_dub), 220180, 1.0);

create aggregate diastolic_aggregate(integer, double precision)
(
  sfunc = select_diastolic,
  stype = int_dub,
  initcond = '(0, 0.0)'
)