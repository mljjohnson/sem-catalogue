ACE-SEM Coupon and Affiliate Extraction Rules (v1)

Coupons
- Markers: coupon, promo, promotion, offer, discount, deal, voucher, code
- Components: any DOM node where class/id contains those markers
- Codes: tokens 5–20 chars, alphanumeric plus -/_; must include at least one letter; exclude phone numbers and dates
- Context window: +/- 100 chars around markers; collect tokens; uppercase normalize; unique

Affiliate Links
- Hosts matched in config/affiliate_networks.json
- Or query params containing: aff_id, aff_sub, subid, sid, utm_source
- Brand inference priority: data-brand/partner attributes; image alt near logo; heading/card text; fallback domain->brand map

Positions
- Determined by DOM order within list/compare modules; first visible item is P1

Primary Category & Template Name
- Extract from page-level data script (e.g., id="pageLevelData") where available; otherwise meta tags





