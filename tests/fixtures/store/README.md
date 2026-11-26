# A small cut of the real store, for the insights tests

Cut from the private bucket on 2026-11-26, read only, in the bucket's own layout so
`Storage.local` reads it as it would read R2:

- `clean/economy/ecb__fx/vintage=2026-11-24/`: the ECB euro reference rates as held when the
  Open Data canvas was made. Every currency still quoted, from 2023-01-01, plus the last five
  fixes of each of the 15 the ECB no longer quotes.
- `clean/trade/worldbank__pinksheet/vintage=2026-10-21/`: the Pink Sheet monthly prices from
  1990-01-01.
- `clean/economy/ecb__policy_rates/vintage=2026-10-21/`: the ECB key rates, whole.
- `catalog/series/`: the series titles and units of the three.

Both publishers allow reuse with attribution, which is why their values may sit in a public
repository:

- Source: European Central Bank, ECB Data Portal (ECB statistics reuse policy).
- Source: World Bank, Commodity Price Data (The Pink Sheet), CC BY 4.0.

No `commercial_ok: ask` source (the IMF ones) is in this folder, and none may be added.
