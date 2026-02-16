{% test payment_accepted_values(model, column_name) %}

with validation as (

    select
        {{ column_name }} as method

    from {{ model }}

),

validation_errors as (

    select
        method

    from validation
    -- in our case if the payment type is 6
    where (method) not in (1, 2, 3, 4, 5)

)

select *
from validation_errors

{% endtest %}