create function select_temp(res int_dub, id integer, val double precision) returns int_dub
  as 'select case
        when id = 223761 or id = 223762 then
          case
            when val < 50 then cast(row(223761, val * 9.0/5.0 + 32) as int_dub)
            else cast(row(223761, val) as int_dub)
          end
        else cast(row(0, 0.0) as int_dub)
      end as final_result'
    language sql
    immutable
    returns null on null input;

create aggregate temp_aggregate(integer, double precision)
(
  sfunc = select_temp,
  stype = int_dub,
  initcond = '(0, 0.0)'
)