Decimal Precision Fixes

Excess fractional digits are now **rounded** half away from zero instead of truncated, so `1.239` at scale 2 stores `124`.
Values beyond a column's declared **precision** are **rejected** instead of silently stored.
Both fixes are **tested** and **migrated into the main project**; no regressions detected.
