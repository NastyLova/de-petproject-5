INSERT INTO STV2023081257__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081257__STAGING.users
where hash(id) not in (select hk_user_id from STV2023081257__DWH.h_users);

INSERT INTO STV2023081257__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(d.message_id) as  hk_message_id,
       d.message_id  as message_id,
       d.message_ts ,
       now() as load_dt,
       's3' as load_src
       from STV2023081257__STAGING.dialogs d
where hash(d.message_id) not in (select hk_message_id from STV2023081257__DWH.h_dialogs);

INSERT INTO STV2023081257__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023081257__STAGING.groups 
where hash(id) not in (select hk_group_id from STV2023081257__DWH.h_groups);

INSERT INTO STV2023081257__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV2023081257__STAGING.groups as g
left join STV2023081257__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2023081257__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2023081257__DWH.l_admins);

INSERT INTO STV2023081257__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
hash(hd.hk_message_id,hg.hk_group_id),
hd.hk_message_id,
COALESCE(hg.hk_group_id, 0),
now() as load_dt,
's3' as load_src
from STV2023081257__STAGING.dialogs as d
left join STV2023081257__DWH.h_dialogs as hd on d.message_id = hd.message_id
left join STV2023081257__DWH.h_groups as hg on d.message_group  = hg.group_id
where hash(hd.hk_message_id,hg.hk_group_id) not in (select hk_l_groups_dialogs from STV2023081257__DWH.l_groups_dialogs);

INSERT INTO STV2023081257__DWH.l_user_message(hk_l_user_message, hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hd.hk_message_id,hu.hk_user_id),
hd.hk_message_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV2023081257__STAGING.dialogs as d
left join STV2023081257__DWH.h_dialogs hd on hd.message_id = d.message_id 
left join STV2023081257__DWH.h_users hu on hu.user_id = d.message_from 
where hash(hd.hk_message_id,hu.hk_user_id) not in (select hk_l_user_message from STV2023081257__DWH.l_user_message);

INSERT INTO STV2023081257__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.l_admins as la
left join STV2023081257__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

INSERT INTO STV2023081257__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select
hu.hk_user_id,
u.country,
u.age,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.h_users hu
left join STV2023081257__STAGING.users u on u.id = hu.user_id;

INSERT INTO STV2023081257__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
select
hu.hk_user_id,
u.chat_name,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.h_users hu
left join STV2023081257__STAGING.users u on u.id = hu.user_id;

INSERT INTO STV2023081257__DWH.s_dialog_info(hk_message_id, message,message_from, message_to, load_dt,load_src)
select
hd.hk_message_id,
d.message,
d.message_from,
d.message_to,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.h_dialogs hd
left join STV2023081257__STAGING.dialogs d on d.message_id = hd.message_id ;

INSERT INTO STV2023081257__DWH.s_group_name(hk_group_id, group_name, load_dt,load_src)
select
hg.hk_group_id,
g.group_name,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.h_groups hg
left join STV2023081257__STAGING.groups g on g.id = hg.group_id;


INSERT INTO STV2023081257__DWH.s_group_private_status(hk_group_id, is_privat, load_dt,load_src)
select
hg.hk_group_id,
g.is_private,
now() as load_dt,
's3' as load_src
from STV2023081257__DWH.h_groups hg
left join STV2023081257__STAGING.groups g on g.id = hg.group_id;

INSERT INTO STV2023081257__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
hash(hu.hk_user_id, hg.hk_group_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2023081257__STAGING.group_log g
left join STV2023081257__DWH.h_groups hg on hg.group_id = g.group_id 
left join STV2023081257__DWH.h_users hu on hu.user_id = g.user_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV2023081257__DWH.l_user_group_activity);

INSERT INTO STV2023081257__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select 
luga.hk_l_user_group_activity,
hu.hk_user_id, 
gl.event,
gl.event_dt,
now() as load_dt,
's3' as load_src
from STV2023081257__STAGING.group_log as gl
left join STV2023081257__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023081257__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023081257__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;
