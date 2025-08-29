{{ config(materialized='table') }}

with base as (
  select
    invoice_no,
    invoice_line,
    invoice_date,
    product_sku,
    quantity,
    unit_price,
    coalesce(discount,0) as discount,
    customer_id,
    store_id,
    currency,
    payment_method,
    loaded_at
  from {{ ref('stg_ventes') }}
)

, date_map as (
  select cle_date, date from {{ ref('dim_date') }}
)
, prod_map as (
  select cle_produit, code_produit from {{ ref('dim_produit') }}
)
, client_map as (
  select cle_client, id_client from {{ ref('dim_client') }}
)
, store_map as (
  select cle_magasin, id_magasin from {{ ref('dim_magasin') }}
)

select
  row_number() over (order by invoice_no, invoice_line) as cle_vente,
  invoice_no as numero_facture,
  invoice_line as ligne_facture,
  (select cle_date from date_map where date = date_trunc('day', base.invoice_date)::date) as cle_date,
  (select cle_produit from prod_map where code_produit = base.product_sku) as cle_produit,
  (select cle_client from client_map where id_client = base.customer_id) as cle_client,
  (select cle_magasin from store_map where id_magasin = base.store_id) as cle_magasin,
  quantity as quantite,
  unit_price as prix_unitaire,
  discount as remise,
  quantity * unit_price as montant_brut,
  (quantity * unit_price) - (coalesce(discount,0)) as montant_net,
  currency as devise,
  payment_method as moyen_paiement,
  now() as date_chargement
from base
order by invoice_no, invoice_line;
