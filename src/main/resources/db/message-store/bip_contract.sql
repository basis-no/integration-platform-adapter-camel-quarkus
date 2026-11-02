-- Store schema version, checked at adapter startup. In production the
-- database operator provisions this table; this copy is a local/test schema
-- resource only.
create table if not exists bip_contract (
    contract_major integer not null,
    contract_version text not null,
    applied_at timestamptz not null default current_timestamp
);
