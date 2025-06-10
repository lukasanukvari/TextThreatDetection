select *
from (
    select *,
         row_number() over (partition by category order by created_at_utc desc) as rn
    from enriched_table
    where created_at_utc >= current_timestamp() - interval 7 days
    and category is not null
) latest
where rn = 1;
