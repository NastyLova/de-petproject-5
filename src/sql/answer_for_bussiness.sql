with user_group_messages as (
    select luga.hk_group_id, count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
    from STV2023081257__DWH.l_user_group_activity luga
    inner join STV2023081257__DWH.l_user_message lum on lum.hk_user_id = luga.hk_user_id 
    group by luga.hk_group_id
),
user_group_log as (
    select luga.hk_group_id, 
    		hg.registration_dt, 
    		count(distinct sah.user_id_from) cnt_added_users
    from STV2023081257__DWH.l_user_group_activity luga 
    inner join STV2023081257__DWH.s_auth_history sah 
	    on sah.hk_l_user_group_activity  = luga.hk_l_user_group_activity  
	    and sah.event = 'add'
	inner join STV2023081257__DWH.h_groups hg on hg.hk_group_id = luga.hk_group_id 
    group by luga.hk_group_id, hg.registration_dt
    order by hg.registration_dt 
)

select  ugm.hk_group_id,
		cnt_users_in_group_with_messages,
		cnt_added_users,
		round(cnt_users_in_group_with_messages / cnt_added_users * 100, 2) as group_conversion
from user_group_messages ugm
inner join user_group_log ugl on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;
