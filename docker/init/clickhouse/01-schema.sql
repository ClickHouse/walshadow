-- walshadow creates tables, not databases. Pre-create the target database
-- so replicate_all can auto-create demo.users (and any other source table)
-- into it on first boot.

CREATE DATABASE IF NOT EXISTS demo;
