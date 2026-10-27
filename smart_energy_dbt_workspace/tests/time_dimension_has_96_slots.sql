-- This test returns a row if Dim_Time does not have exactly 96 slots.
select count(*) as actual_slots
from {{ ref('dim_time') }}
having count(*) <> 96
