Bounded WAL segment size before allocation

**Untrusted** `xlp_seg_size` flowed into `Vec::resize`. A 36 byte file claiming 2 GiB forced a multi gigabyte allocation, and an 8 KiB file produced 64 MiB of output.

**Fix:** reject sizes above the **16 MiB** project cap, require a power of two and an input no larger than the claimed size, all before allocating.

**Tested** against the original reproduction with no amplification. **Migrated into the main project**; valid segments still pass and **no regressions detected**.
