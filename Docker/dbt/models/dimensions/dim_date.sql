{{ config(materialized='table') }}

with raw_dates as (
  select distinct date_trunc('day', invoice_date)::date as dt
  from {{ ref('stg_ventes') }}
  where invoice_date is not null
)

select
  row_number() over (order by dt) as cle_date,
  dt as date,
  extract(year from dt)::int as annee,
  ceil(extract(month from dt)::numeric/3)::int as trimestre,
  extract(month from dt)::int as mois,
  extract(day from dt)::int as jour,
  extract(dow from dt)::int as jour_semaine,
  case when extract(dow from dt) in (0,6) then true else false end as est_weekend
from raw_dates
order by dt
