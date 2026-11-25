DSN Secret Leak Fix

Invalid DSN errors now **redact credentials** while keeping the **scheme, host, port, database and key** for diagnosis.
The fix was **tested**, **migrated into the main project**, and verification detected **no regressions**.
