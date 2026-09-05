CREATE TABLE public.users (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL
);

INSERT INTO public.users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com');
