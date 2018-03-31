DROP FUNCTION sofa_dif CASCADE
CREATE FUNCTION sofa_dif(integer[], integer) RETURNS integer[]
  AS 'select array(
        select case
          when $2 <= $1[1] then $2
          else $1[1]
        end as smaller
        union all
        select case 
          when t1.update >= t1.history then t1.update
          else t1.history
        end as winner
        from (select ($2 - $1[1]) as update, $1[2] as history) t1)'
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT;
select sofa_dif('{3, 0}', 6);
CREATE AGGREGATE sofa_dif_agg(integer)
(
  SFUNC = sofa_dif,
  STYPE = integer[],
  INITCOND = '{99, 0}'
)