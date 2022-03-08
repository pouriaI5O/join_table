{{ config(
    materialized='table'
)}}
with cte160 as(select timestamp as timestamps,
          personline160 as count_person,
          line160wip as count_wip,
          line160wt as count_wt,
          idline160wip as id_wip,
          idline160wt as id_wt,
          'Line160' as station 

FROM {{ source('public','pridemobility_tracking_160_new') }}
where timestamps>'2022-02-25T12:56:00'),
cte170 as (select timestamp as timestamps,
          personline170 as count_person,
          line170wip as count_wip,
          line170wt as count_wt,
          idline170wip as id_wip,
          idline170wt as id_wt,
          'Line170' as station 

FROM {{ source('public','pridemobility_tracking_170_new') }}
where timestamps>'2022-02-25T12:56:00')
SELECT * FROM cte160
UNION 
SELECT * FROM cte170
order by timestamps desc