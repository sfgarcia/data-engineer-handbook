-- select *
-- from game_details
-- where game_id = 22000071
--     and team_id = 1610612742
--     and player_id = 1630179

-- select
--     game_id,
--     team_id,
--     player_id
-- from game_details
-- group by 1, 2, 3
-- having count(*) > 1
-- limit 10

with deduped_game_details as (
    select
        *,
        row_number() over (partition by game_id, team_id, player_id) as row_num
    from game_details
)

select *
from deduped_game_details
where row_num = 1
limit 10