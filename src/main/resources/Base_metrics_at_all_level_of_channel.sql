select customerid,'${current_start_date_bases}' as  dt_local_tz, count(distinct msisdn) as count_uu, count(*) count_txn,currentstatus,
case when currentstatus=4  and isFreeTrialUser=true then 'trial'
when currentstatus=6 and isFreeTrialUser=true then 'free_grace'
when currentstatus=6 and isFreeTrialUser=false then 'paid_grace'
when currentstatus=7 and isFreeTrialUser=true then 'free_suspend'
when currentstatus=7 and isFreeTrialUser=false then 'paid_suspend'
 else b.name end as name,'ALL' as dim_type
from (select  row_number() over (partition by subscriptionid order  by dt_local_tz desc ) row_num  ,a.* from my_dataset.stg2_${PRODUCTNAME}_user_activity_history a
where date(dt_local_tz) <= '${current_start_date_bases}' ) a
left join  public.baas_subscription_status b on a.currentstatus=b.subscription_status_id
 where a.row_num=1 group by  1,5,6;