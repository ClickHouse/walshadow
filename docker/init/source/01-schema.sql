-- walshadow demo source schema. Three rows naming the parties in any
-- relation of labour: workman, master, state. REPLICA IDENTITY FULL
-- ships the old-tuple image on UPDATE/DELETE so each change is
-- observable on the wire, not inferred.

CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE demo.users (
    id    bigint PRIMARY KEY,
    name  text NOT NULL,
    email text NOT NULL
);
ALTER TABLE demo.users REPLICA IDENTITY FULL;

INSERT INTO demo.users (id, name, email) VALUES
    (1, 'Opifex',     'opifex@rerum.novarum'),
    (2, 'Dominus',    'dominus@rerum.novarum'),
    (3, 'Respublica', 'respublica@rerum.novarum');
