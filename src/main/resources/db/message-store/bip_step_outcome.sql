create table if not exists bip_step_outcome (
    idempotency_key text primary key,
    message_id text,
    step_id text not null,
    status text not null check (status in ('DELIVERED', 'FAILED', 'SKIPPED')),
    endpoint_uri text,
    target_ref text,
    error text,
    occurred_at timestamptz not null,
    checkpoint_storage text check (checkpoint_storage in ('postgres_inline', 's3_object')),
    checkpoint_body_bytes bytea,
    checkpoint_body_ref text,
    checkpoint_body_size_bytes bigint,
    checkpoint_body_sha256 text,
    checkpoint_body_encoding text check (checkpoint_body_encoding in ('null', 'bytes', 'utf8', 'json')),
    checkpoint_content_type text,
    checkpoint_expires_at timestamptz,
    checkpoint_headers_json jsonb not null default '{}'::jsonb,
    checkpoint_properties_json jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default current_timestamp
);

create index if not exists bip_step_outcome_message_id_idx
    on bip_step_outcome (message_id);

create index if not exists bip_step_outcome_status_idx
    on bip_step_outcome (status);
