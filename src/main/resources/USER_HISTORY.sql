select  customerid,dt_local_tz,msisdn,subscriptionid,previousstatus,currentstatus,enddate,isFreeTrialUser,itemid,itemtypeid
from
(
  select ROW_NUMBER() OVER (PARTITION BY subscriptionid ,dt_local_tz ORDER BY sequence desc,dt_local_tz desc) row_num,*
  from
        (select  customerid,msisdn,logtype,activityresult,subscriptionid,currentStatus , previousStatus,enddate, date(dt_local_tz) as dt_local_tz,
          case when sequence is null then -999 else sequence end as sequence,isFreeTrialUser,itemid,itemtypeid
          from testdataset.pageviewlog_istconfirmation_20160531
          where 1=1
          and  date(dt_local_tz) between '${current_start_date_IST}' and '${current_end_date_IST}'
          and customerid=${CUSTOMERID}
        ),
        (select customerid,msisdn,logtype,activityresult,subscriptionid,  currentStatus , previousStatus,renewalduedate as enddate, date(dt_local_tz) as dt_local_tz,
			case when sequence is null then -999 else sequence end as sequence,isFreeTrialUser,itemid,itemtypeid
			from testdataset.pageviewlog_confirmation_deactivation_20160531
			where 1=1
      and  date(dt_local_tz) between '${current_start_date_IST}' and '${current_end_date_IST}'
      and customerid=${CUSTOMERID}
        ),
        (
        select   customerid,msisdn,logtype,activityresult,subscriptionid,currentStatus , previousStatus,renewalduedate as enddate, date(dt_local_tz) as dt_local_tz,
			case when sequence is null then -999 else sequence end as sequence,isFreeTrialUser,itemid,itemtypeid
			from testdataset.billing_info_20160531
            where 1=1
            and  date(dt_local_tz) between '${current_start_date_IST}' and '${current_end_date_IST}'
            and customerid=${CUSTOMERID}
        )
) b
where row_num=1