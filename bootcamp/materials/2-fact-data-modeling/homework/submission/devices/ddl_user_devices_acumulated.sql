-- drop table if exists user_devices_cumulated;

create table if not exists user_devices_cumulated (
    user_id numeric,
    browser_type text,
    device_activity_datelist date[],
    curr_date date,
    primary key (curr_date, user_id, browser_type)
);
