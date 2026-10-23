# Frozen public-price evidence

Retrieved on 2026-10-22 America/Denver (raw metadata records UTC). Currency: USD. Selected regional meters: East US; NAT uses the provider's Global meter. No Azure account, deployment or billing data was used.

Every service directory contains untouched page responses, pagination URLs and SHA-256 checksums in `snapshot.json`, plus the assembled `records.json`. `dpe.cloud_model.catalog` checks these hashes and selects exact product/SKU/meter/region/purchase-model identities. Storage preserves all capacity tiers. Unused records remain as retrieval evidence; their presence does not mean they were charged.

The [Microsoft Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) describes filters, pagination and public-price semantics. `Global` NAT pricing is explicitly distinct from an East US regional meter. No reservations, spot rates, Windows licenses, savings-plan discounts or negotiated rates are selected.

To take a new snapshot, use a fresh directory:

```sh
python3 benchmarks/fetch_public_prices.py --out benchmarks/pricing/my-new-snapshot
```

The original exact filters are reused by default. Preserve old snapshots. Update the model's valuation interval when using a later price date; a frozen snapshot is not a guarantee of future prices.
