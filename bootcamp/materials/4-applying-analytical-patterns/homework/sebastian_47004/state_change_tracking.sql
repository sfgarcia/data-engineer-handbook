select *
from player_seasons
where player_name = 'A.C. Green';

select
    season,
    count(*)
from player_seasons
group by 1
order by 1;