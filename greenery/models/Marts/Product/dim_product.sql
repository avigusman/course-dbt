select
    product_id,
    name as product_name,
    price,
    inventory
from  {{ ref('int_product') }}
