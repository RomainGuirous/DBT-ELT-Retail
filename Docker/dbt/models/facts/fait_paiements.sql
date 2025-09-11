{{ config(materialized='table') }}

-- This model is a placeholder mapping payments if available in stg_ventes
select
  row_number() over (order by invoice_no) as cle_paiement,
  null::text as reference_paiement,
  invoice_no as numero_facture,
  null::int as cle_commande,
  null::int as cle_date_paiement,
  null::int as cle_magasin,
  null::int as cle_client,
  null::numeric as montant_paiement,
  currency as devise,
  payment_method as moyen_paiement,
  null::text as statut_paiement,
  null::numeric as frais_paiement,
  false as est_remboursement,
  null::numeric as montant_net,
  null::text as source_paiement,
  null::jsonb as payment_payload,
  null::timestamptz as date_paiement_timestamp,
  now() as date_chargement
from {{ ref('stg_ventes') }}
