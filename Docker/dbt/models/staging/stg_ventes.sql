{{ config(materialized='view') }}

-- Simple staging view that expects a raw table named public.stg_ventes
select
  id_brut,
  batch_id,
  row_id,
  invoice_no,
  invoice_line,
  invoice_date::timestamp as invoice_date,
  store_id,
  store_name,
  store_city,
  store_region,
  country,
  customer_id,
  customer_prenom,
  customer_nom,
  customer_email,
  product_sku,
  product_name,
  brand,
  category,
  subcategory,
  quantity::numeric as quantity,
  unit_price::numeric as unit_price,
  discount::numeric as discount,
  currency,
  payment_method,
  loaded_at::timestamp as loaded_at,
  (case when jsonb_typeof(raw_json::jsonb) is not null then raw_json::jsonb else null end) as raw_json
from {{ ref('raw_ventes') }}
