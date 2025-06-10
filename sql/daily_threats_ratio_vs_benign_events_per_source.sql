select
  date(created_at_utc) as event_date,
  source,
  count(case when category is not null then 1 end) as threat_count,
  count(*) as total_events,
  round(
    count(case when category is not null then 1 end) * 1.0 / count(*),
    3
  ) as threat_ratio
from enriched_table
group by event_date, source
group by event_date, source;
