create type film_struct as (
    film text,
    votes integer,
    rating real,
    filmid text
)

create type quality_class as enum ('star', 'good', 'average', 'bad')

create table actors (
    actorid text,
    actor text,
    films film_struct[],
    quality quality_class,
    is_active boolean,
    current_year integer,
    primary key (actorid, current_year)
)

insert into actors

with yesterday as (
    select *
    from actors
    where current_year = 1971
),
today as (
    select actorid,
        actor,
        year,
        array_agg(row(film, votes, rating, filmid)::film_struct) as films
    from actor_films
    where year = 1972
    group by 1, 2, 3
),
rating as (
    select actorid,
        case
            when avg(rating) > 8 then 'star'
            when avg(rating) > 7 then 'good'
            when avg(rating) > 6 then 'average'
            else 'bad'
        end::quality_class as quality_class
    from actor_films
    where year = 1972
    group by 1
)

select
    COALESCE(y.actorid, t.actorid) as actorid,
    COALESCE(y.actor, t.actor) as actor,
    case
        when y.films is null then t.films
        else y.films || t.films
    end as films,
    r.quality_class,
    case
        when t.actorid is null then false
        else true
    end as is_active,
    coalesce(t.year, y.current_year + 1) as current_year
from yesterday y
full outer join today t on y.actorid = t.actorid
left join rating r on t.actorid = r.actorid

create table actors_history_scd (
    actorid text,
    actor text,
    quality_class quality_class,
    is_active boolean,
    start_year integer,
    end_year integer,
    primary key (actorid, start_year)
)

with previous as (
    select
        actorid,
        actor,
        quality as quality_class,
        lag(quality, 1) over (partition by actorid order by current_year) as prev_quality_class,
        is_active,
        lag(is_active, 1) over (partition by actorid order by current_year) as prev_is_active,
        current_year
    from actors
    where current_year < 2000
),
detect_changes as (
    select
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        case
            when prev_quality_class != quality_class then 1
            when prev_is_active != is_active then 1
            else 0
        end as scd_change
    from previous
),
scd_groups as (
    select
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        sum(scd_change) over (partition by actorid order by current_year) as scd_group
    from detect_changes
)

select
    actorid,
    actor,
    quality_class,
    is_active,
    min(current_year) as start_year,
    max(current_year) as end_year
from scd_groups
group by 1, 2, 3, 4

create type scd_type as (
    quality_class quality_class,
    is_active boolean,
    start_year integer,
    end_year integer
)

insert into actors_history_scd
with historical_scd as (
    select *
    from actors_history_scd
    where end_year < 1971
),
actual_data as (
    select *
    from actors
    where current_year = 1971
),
unchanged_records as (
    select
        act.actorid,
        act.actor,
        act.quality as quality_class,
        act.is_active,
        hist.start_year,
        act.current_year as end_year
    from actual_data act
    join historical_scd hist
        on act.actorid = hist.actorid
    where act.quality = hist.quality_class
        and act.is_active = hist.is_active
),
changed_records as (
    select
        act.actorid,
        act.actor,
        unnest(
            array[
                row(
                    hist.quality_class,
                    hist.is_active,
                    hist.start_year,
                    hist.end_year
                )::scd_type,
                row(
                    act.quality,
                    act.is_active,
                    act.current_year,
                    act.current_year
                )::scd_type
            ]
        ) as records

    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where (act.quality <> hist.quality_class
        or act.is_active <> hist.is_active)
),
unnested_changed_records as (
    select
        actorid,
        actor,
        (records).quality_class,
        (records).is_active,
        (records).start_year,
        (records).end_year
    from changed_records
),
new_records as (
    select
        act.actorid,
        act.actor,
        act.quality as quality_class,
        act.is_active,
        act.current_year as start_year,
        act.current_year as end_year
    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where hist.actorid is null
),
union_records as (
    select *
    from unchanged_records
    union all
    select *
    from unnested_changed_records
    union all
    select *
    from new_records
)

select *
from union_records
limit 10
-- insert into actors_history_scd
-- with previous as (
--     select
--         actorid,
--         actor,
--         quality as quality_class,
--         lag(quality, 1) over (partition by actorid order by current_year) as prev_quality_class,
--         is_active,
--         lag(is_active, 1) over (partition by actorid order by current_year) as prev_is_active,
--         current_year
--     from actors
--     where current_year < 2000
-- ),
-- detect_changes as (
--     select
--         actorid,
--         actor,
--         quality_class,
--         is_active,
--         current_year,
--         case
--             when prev_quality_class != quality_class then 1
--             when prev_is_active != is_active then 1
--             else 0
--         end as scd_change
--     from previous
-- ),
-- scd_groups as (
--     select
--         actorid,
--         actor,
--         quality_class,
--         is_active,
--         current_year,
--         sum(scd_change) over (partition by actorid order by current_year) as scd_group
--     from detect_changes
-- )

-- select
--     actorid,
--     actor,
--     quality_class,
--     is_active,
--     min(current_year) as start_year,
--     max(current_year) as end_year
-- from scd_groups
-- group by 1, 2, 3, 4
--order by 1, 2, 3, 4
--limit 10

-- create type scd_type as (
--     quality_class quality_class,
--     is_active boolean,
--     start_year integer,
--     end_year integer
-- )

---- incremental history scd query
insert into actors_history_scd
with historical_scd as (
    select *
    from actors_history_scd
    where end_year < 1971
),
actual_data as (
    select *
    from actors
    where current_year = 1971
),
unchanged_records as (
    select
        act.actorid,
        act.actor,
        act.quality as quality_class,
        act.is_active,
        hist.start_year,
        act.current_year as end_year
    from actual_data act
    join historical_scd hist
        on act.actorid = hist.actorid
    where act.quality = hist.quality_class
        and act.is_active = hist.is_active
),
changed_records as (
    select
        act.actorid,
        act.actor,
        unnest(
            array[
                row(
                    hist.quality_class,
                    hist.is_active,
                    hist.start_year,
                    hist.end_year
                )::scd_type,
                row(
                    act.quality,
                    act.is_active,
                    act.current_year,
                    act.current_year
                )::scd_type
            ]
        ) as records

    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where (act.quality <> hist.quality_class
        or act.is_active <> hist.is_active)
),
unnested_changed_records as (
    select
        actorid,
        actor,
        (records).quality_class,
        (records).is_active,
        (records).start_year,
        (records).end_year
    from changed_records
),
new_records as (
    select
        act.actorid,
        act.actor,
        act.quality as quality_class,
        act.is_active,
        act.current_year as start_year,
        act.current_year as end_year
    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where hist.actorid is null
),
union_records as (
    select *
    from unchanged_records
    union all
    select *
    from unnested_changed_records
    union all
    select *
    from new_records
)

select *
from union_records