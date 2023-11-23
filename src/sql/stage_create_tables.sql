drop table if exists STV2023081257__STAGING.dialogs cascade;
create table STV2023081257__STAGING.dialogs
(
    message_id int PRIMARY KEY,
    message_ts timestamp,
    message_from int REFERENCES members(id),
    message_to int REFERENCES members(id),
    message varchar(1000),
    message_group int
)
order by
message_id
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY
calendar_hierarchy_day(message_ts::date, 3, 2);

create projection STV2023081257__STAGING.dialogs_proj as 
select
	message_id,
	message_ts,
	message_from,
	message_to,
	message,
	message_group
from
	STV2023081257__STAGING.dialogs
order by
	message_id
segmented by hash(message_id) all nodes;


drop table if exists STV2023081257__STAGING.groups cascade;

create table STV2023081257__STAGING.groups
(
    id int PRIMARY KEY,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp(6),
    is_private boolean
)
order by id, admin_id
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY
calendar_hierarchy_day(registration_dt::date, 3, 2);

create projection STV2023081257__STAGING.groups_proj as 
select
	id,
    admin_id,
    group_name,
    registration_dt,
    is_private
from
	STV2023081257__STAGING.groups
order by
	id,
	admin_id
segmented by hash(id) all nodes;

drop table if exists STV2023081257__STAGING.users cascade;

create table STV2023081257__STAGING.users
(
    id int PRIMARY KEY,
    chat_name varchar(200),
    registration_dt timestamp(6),
    country varchar(200),
    age int
)
order by id
SEGMENTED BY hash(id) all nodes;

create projection STV2023081257__STAGING.users_proj as 
select
	id,
    chat_name,
    registration_dt,
    country,
    age
from
	STV2023081257__STAGING.users
order by
	id
segmented by hash(id) all nodes;


drop table if exists STV2023081257__STAGING.group_log cascade;
create table STV2023081257__STAGING.group_log
(
    group_id int PRIMARY KEY,
    user_id int,
    user_id_from int,
    event varchar(10),
    event_dt timestamp
)
order by
group_id
SEGMENTED BY hash(group_id) all nodes
PARTITION BY datetime::date
GROUP BY
calendar_hierarchy_day(datetime::date, 3, 2);

create projection STV2023081257__STAGING.group_log_proj as 
select
	group_id,
    user_id,
    user_id_from,
    event,
    event_dt
from STV2023081257__STAGING.group_log
order by group_id
segmented by hash(group_id) all nodes;