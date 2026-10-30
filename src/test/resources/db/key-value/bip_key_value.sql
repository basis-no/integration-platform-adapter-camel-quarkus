-- Disposable test fixture only. Platform-owned migrations008+009 provision production.
-- Key-Value Store: a small key-value store per integration (spec section 13,
-- ADR-006). Contract v1.2.0-rc2, additive: no existing table changes.
--
-- Rows belong to the integration that wrote them (`integration` is the same
-- identity as bip_message_log.integration). Adapters read and write only their
-- own rows; the dashboard/agent read path reads every row. Values are small JSON
-- documents, never payloads or secrets. Expired rows are invisible to reads and
-- swept by the adapter that owns them; message retention never touches them.

create table if not exists bip_key_value (
    integration text not null,
    key text not null,
    value jsonb not null,
    version bigint not null default 1,
    expires_at timestamptz,
    created_at timestamptz not null default current_timestamp,
    updated_at timestamptz not null default current_timestamp,
    updated_by text not null default 'runtime',
    primary key (integration, key),
    constraint bip_key_value_key_check
        check (char_length(key) between 1 and 256 and key !~ '[[:cntrl:]]'),
    constraint bip_key_value_value_check
        check (octet_length(value::text) <= 65536),
    constraint bip_key_value_version_check
        check (version > 0),
    constraint bip_key_value_writer_check
        check (updated_by = 'runtime' or updated_by like 'operator:%')
);

create index if not exists bip_key_value_expires_at_idx
    on bip_key_value (integration, expires_at)
    where expires_at is not null;

-- At most 10 000 live keys per integration, so one integration cannot fill the
-- Message Store. Checked on insert only; an upsert of an existing key is an
-- update and always passes. Concurrent inserts can overshoot by a few keys.
create or replace function bip_key_value_limit() returns trigger
language plpgsql as $$
begin
    if exists (
        select 1 from bip_key_value
        where integration = new.integration and key = new.key
    ) then
        return new;
    end if;
    if (
        select count(*) from bip_key_value
        where integration = new.integration
          and (expires_at is null or expires_at > current_timestamp)
    ) >= 10000 then
        raise exception 'key-value store key limit reached for %', new.integration
            using errcode = '54000';
    end if;
    return new;
end;
$$;

drop trigger if exists bip_key_value_limit on bip_key_value;
create trigger bip_key_value_limit
    before insert on bip_key_value
    for each row execute function bip_key_value_limit();
