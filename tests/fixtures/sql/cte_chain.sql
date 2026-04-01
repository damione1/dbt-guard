with base as (
    select id, name, email from users
),
enriched as (
    select b.id, b.name, b.email, a.status
    from base b
    left join accounts a on b.id = a.user_id
)
select id, name, email, status from enriched
