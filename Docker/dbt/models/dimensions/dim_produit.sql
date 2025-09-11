{{ config(materialized='table') }}

with prod as (
  select
    product_sku,
    product_name,
    brand,
    category,
    subcategory,
    unit_price
  from {{ ref('stg_ventes') }}
  where product_sku is not null
)

select
  row_number() over (order by product_sku) as cle_produit,
  product_sku as code_produit,
  product_name as nom_produit,
  brand as marque,
  category as categorie,
  subcategory as sous_categorie,
  max(unit_price) as prix_unitaire_defaut,
  null::text as fournisseur,
  true as flag_courant
from prod
group by product_sku, product_name, brand, category, subcategory
order by code_produit
