create table if not exists bip_step_outcome (
    idempotency_key text primary key,
    message_id text,
    step_id text not null,
    status text not null check (status in ('DELIVERED', 'FAILED', 'SKIPPED')),
    endpoint_uri text,
    target_ref text,
    error text,
    occurred_at timestamptz not null,
    updated_at timestamptz not null default current_timestamp
);

create index if not exists bip_step_outcome_message_id_idx
    on bip_step_outcome (message_id);

create index if not exists bip_step_outcome_status_idx
    on bip_step_outcome (status);
