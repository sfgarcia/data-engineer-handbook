-- select year,count(*)
-- from actor_films
-- group by 1
-- order by 1


-- select *
-- from actors
-- limit 10

-- create table actors_history_scd (
--     actorid text,
--     actor text,
--     quality_class quality_class,
--     is_active boolean,
--     start_year integer,
--     end_year integer,
--     --current_year integer,
--     primary key (actorid, start_year)
-- )

-- full history scd query
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
limit 10

