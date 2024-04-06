select
    event_id,
    session_id,
    user_id,
    page_url,
    created_at,
    event_type,
    order_id,
    product_id
from {{ ref('int_page_views') }} p
JOIN {{ ref('int_users') }} u 
    ON p.user_id = u.user_id