-- select *
-- from actor_films
-- where year = 1970
-- limit 10

-- create type film_struct as (
--     film text,
--     votes integer,
--     rating real,
--     filmid text
-- )
-- create type quality_class as enum ('star', 'good', 'average', 'bad')
-- drop table actors
-- create table actors (
--     actorid text,
--     actor text,
--     films film_struct[],
--     quality quality_class,
--     is_active boolean,
--     current_year integer,
--     primary key (actorid, current_year)
-- )
insert into actors with yesterday as (
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
        group by 1,
            2,
            3
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
select COALESCE(y.actorid, t.actorid) as actorid,
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
select *
from actors
where actorid = 'nm0000003'
limit 10