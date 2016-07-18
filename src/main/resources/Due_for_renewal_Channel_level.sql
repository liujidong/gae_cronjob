select  customerid,'${current_start_date_bases}' as dt_local_tz ,count(distinct  msisdn ) as count_uu, count(*) as count_txn, -1 as currentstatus,
case when isFreeTrialUser=true and currentstatus=4  then 'free_renewal_due'
when isFreeTrialUser=false then 'paid_renewal_due' end ,
itemid,itemtypeid,'Channel' as dim_type
from
(select *
from  (
	select row_number() over (partition by subscriptionid order  by dt_local_tz desc) row_num,*
	from my_dataset.stg2_${PRODUCTNAME}_user_activity_history
	where  date(dt_local_tz)<=date(DATE_ADD('${current_start_date_bases}', -1, 'DAY') )
 ) a
where a.row_num=1 and (currentstatus in ( 6,7) or ( currentstatus=4 and date(enddate) = '${current_start_date_bases}' ) )
) t
group by  1,5,6,7,8;