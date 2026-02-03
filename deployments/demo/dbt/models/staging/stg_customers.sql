-- Staging model for raw customers
-- Source: seeds/raw_customers.csv

with source as (
    select * from {{ ref('raw_customers') }}
),

renamed as (
    select
        id as customer_id,
        name as customer_name
    from source
)

select * from renamed
