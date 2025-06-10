select
  date(created_at_utc) as event_date,
  source,
  category,
  count(*) as threat_count
from enriched_table
where category is not null
group by event_date, source, category
order by event_date, source, category;
