{{ config(materialized='table') }}

with clients as (
  select
    customer_id,
    customer_prenom,
    customer_nom,
    customer_email
  from {{ ref('stg_ventes') }}
  where customer_id is not null
)

select
  row_number() over (order by customer_id) as cle_client,
  customer_id as id_client,
  customer_prenom as prenom,
  customer_nom as nom,
  customer_email as email,
  null::text as segment,
  null::text as pays,
  null::text as ville,
  null::timestamp as date_inscription,
  true as flag_courant
from clients
group by customer_id, customer_prenom, customer_nom, customer_email
order by id_client;
