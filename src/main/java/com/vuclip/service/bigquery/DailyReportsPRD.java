package com.vuclip.service.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.vuclip.util.BigQueryUtil;
import com.vuclip.util.CommonUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.logging.Logger;

public class DailyReportsPRD {
	private static final Logger _logger = Logger.getLogger(DailyReportsPRD.class.getName());
	static String MY_DATASET="my_dataset";
	private final static long interval = 1000;//1 second
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("DailyReportsPRD.main()");
		String querySql = "select * from [my_dataset.billing_info_20160616] limit 10";

		TableReference destTable = new TableReference();
		destTable.setProjectId(CommonUtil.projectId);
		destTable.setDatasetId(MY_DATASET);
		destTable.setTableId("billing_info_201606062");

		Bigquery bigquery;
		try {
			bigquery = BigQueryUtil.createAuthorizedClientAppEngine();
			CommonUtil.selectInto(bigquery,querySql,destTable,"WRITE_APPEND");

			_logger.info("copy append ok!");
		} catch (IOException e) {
			_logger.warning(e.toString());
			e.printStackTrace();
		}
	}
	private static String fileToString(String file) throws IOException{
		StringBuffer sqlSB = new StringBuffer();
		Reader reader =new InputStreamReader(DailyReportsPRD.class.getResourceAsStream(file));
		int tempchar;
		while ((tempchar = reader.read()) != -1) {
			sqlSB.append((char)tempchar);
		}
		return sqlSB.toString();
	}

	public static void dailyReportsPRD(Integer customerId,String currentStartDateIST,String currentEndDateIST){

		_logger.info(String.format("The Start date is %s",currentStartDateIST));
		_logger.info(String.format("The end date is %s",currentEndDateIST));
		_logger.info(String.format("Customer ID is %s",customerId));

		String PRODUCT_SQL = String.format("select product_sk,product_name,product_type from [public.dim_product]\n" +
				"where customer_id= %s",customerId);
//		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
//		Transaction txn = datastore.beginTransaction();
		try {
			Bigquery bigquery = BigQueryUtil.createAuthorizedClient();
			List<TableRow> rows = BigQueryUtil.executeQuery(PRODUCT_SQL, bigquery, CommonUtil.projectId);
			//_logger.info("rows.size():" + rows.size());
			String PRODUCTSK = null;
			String PRODUCTNAME = null;
			String PRODUCTTYPE = null;
			for (TableRow row : rows) {
				PRODUCTSK = (String)CommonUtil.getRowV(row,0);
				PRODUCTNAME = (String)CommonUtil.getRowV(row,1);
				Object productType = CommonUtil.getRowV(row,2);
				if(null != productType) {
					//_logger.info("try.for.if:"+productType.toString());
					PRODUCTTYPE = (String)productType;
				}
				//System.out.println(row.get("f")); //[{"v":"26"}, {"v":"PPPAIVS"}, {"v":null}]
//				PRODUCTSK = (String)row.get("product_sk");
//				PRODUCTNAME = (String)row.get("product_name");
//				PRODUCTTYPE = (String)row.get("product_type");
			}

			_logger.info(String.format("Product_sk is %s",PRODUCTSK));
			_logger.info(String.format("Product Name is %s",PRODUCTNAME));
			_logger.info(String.format("Product type Name is %s", PRODUCTTYPE));
			_logger.info("DB:"+MY_DATASET);

			//--------USER HISTORY ----------------------
			String sql = fileToString("/USER_HISTORY.sql");
			sql = sql.replace("${current_start_date_IST}",currentStartDateIST);
			sql = sql.replace("${current_end_date_IST}",currentEndDateIST);
			sql = sql.replace("${CUSTOMERID}",customerId+"");

			TableReference destTable = new TableReference();
			destTable.setProjectId(CommonUtil.projectId);
			destTable.setDatasetId(MY_DATASET);
			destTable.setTableId("stg2_${PRODUCTNAME}_user_activity_history".replace("${PRODUCTNAME}",PRODUCTNAME));

			Bigquery.Jobs.Get jobGet = CommonUtil.selectInto(bigquery,sql,destTable);

			BigQueryUtil.pollJob(jobGet, interval);

			System.out.println("Job[USER HISTORY] is Done!");

			String currentStartDateBases = CommonUtil.date_d(currentStartDateIST,"YYYYMMdd");
			String currentEndDateBases = CommonUtil.date_d(currentEndDateIST,"YYYYMMdd");
			//------Base metrics on the level of channel
			sql = fileToString("/Base_metrics_on_the_level_of_channel.sql");
			sql = sql.replace("${current_start_date_bases}",currentStartDateBases);
			sql = sql.replace("${PRODUCTNAME}",PRODUCTNAME);

			destTable.setTableId("ppp_fact_user_base_metrics");

			CommonUtil.selectInto(bigquery,sql,destTable);

			//------Base metrics at all level of channel
			sql = fileToString("/Base_metrics_at_all_level_of_channel.sql");
			sql = sql.replace("${current_start_date_bases}",currentStartDateBases);
			sql = sql.replace("${PRODUCTNAME}",PRODUCTNAME);

			CommonUtil.selectInto(bigquery,sql,destTable);
			//----Due for renewal Channel level-----
			sql = fileToString("/Due_for_renewal_Channel_level.sql");
			sql = sql.replace("${current_start_date_bases}",currentStartDateBases);
			sql = sql.replace("${PRODUCTNAME}",PRODUCTNAME);

			CommonUtil.selectInto(bigquery,sql,destTable);

			//txn.commit();
		} catch (Exception e) {
			_logger.warning(e.toString());
			e.printStackTrace();
//			if (txn.isActive()) {
//				_logger.warning("rollback()");
//				txn.rollback();
//			}
		}
	}
}
