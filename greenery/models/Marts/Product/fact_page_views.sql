
with
events as (
    select * from {{ ref('stg_postgres__events') }}
),

order_items as (
    select * from {{ ref('stg_postgres__order_items') }}
)

{% set event_types = dbt_utils.get_column_values(
    table=ref('stg_postgres__events'), 
    column='event_type'
) %}

select
    e.session_id,
    e.user_id,
    coalesce(e.product_id, oi.product_id) as product_id,
    {% for event_type in event_types %}
    {{ sum_of('e.event_type', event_type) }} as {{ event_type }}s,
    {% endfor %}
from events e
left join order_items oi
    on oi.order_id = e.order_id
group by 1, 2, 3