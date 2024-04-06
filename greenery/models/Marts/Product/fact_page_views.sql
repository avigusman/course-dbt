select
pv.*,
p.product_name
from {{ ref('int_page_views') }} pv
JOIN {{ ref('int_product') }} p
    ON pv.product_id = p.product_id