{{ config(materialized='table') }}

with stores as (
  select
    store_id,
    store_name,
    store_city,
    store_region,
    country
  from {{ ref('stg_ventes') }}
  where store_id is not null
)

select
  row_number() over (order by store_id) as cle_magasin,
  store_id as id_magasin,
  store_name as nom_magasin,
  null::text as type_magasin,
  store_city as ville,
  store_region as region,
  country as pays
from stores
group by store_id, store_name, store_city, store_region, country
order by id_magasin
