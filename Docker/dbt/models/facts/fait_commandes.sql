{{ config(materialized='table') }}

with orders as (
  select
    invoice_no,
    invoice_date,
    customer_id,
    store_id,
    sum(quantity * unit_price) as total_montant_brut,
    sum(coalesce(discount,0)) as total_remise,
    sum(quantity * unit_price) - sum(coalesce(discount,0)) as total_montant_net,
    sum(quantity) as total_items,
    count(distinct invoice_line) as nombre_lignes,
    count(distinct product_sku) as nombre_produits_distincts
  from {{ ref('stg_ventes') }}
  group by invoice_no, invoice_date, customer_id, store_id
)

select
  row_number() over (order by invoice_no) as cle_commande,
  invoice_no as numero_facture,
  (select cle_date from {{ ref('dim_date') }} d where d.date = date_trunc('day', o.invoice_date)::date) as cle_date,
  null as cle_client,
  null as cle_magasin,
  total_montant_brut,
  total_remise,
  total_montant_net,
  total_items,
  nombre_lignes,
  nombre_produits_distincts,
  case when nombre_lignes>0 then total_montant_net / nombre_lignes else null end as panier_moyen,
  case when nombre_lignes>0 then total_montant_net::numeric / nombre_produits_distincts else null end as moyenne_prix_par_ligne,
  'EUR' as devise,
  null::text as moyen_paiement,
  false as est_retour,
  now() as date_chargement
from orders o
order by invoice_no
