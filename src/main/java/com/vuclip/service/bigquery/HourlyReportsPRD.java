package com.vuclip.service.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableRow;
import com.vuclip.util.BigQueryUtil;
import com.vuclip.util.CommonUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by Administrator on 2016/7/7.
 */
public class HourlyReportsPRD {
    private static final Logger _logger = Logger.getLogger(HourlyReportsPRD.class.getName());
    public static void hourlyReportsPRD(){
        Integer customerId = 1;
        String today = (new SimpleDateFormat("yyyy-MM-dd")).format(new Date());
        String currentStartDateIST = today;
        String currentEndDateIST = today;

        String startTime = " 00:00:00";
        String endTime = " 23:59:59.999";

        currentStartDateIST = currentStartDateIST + startTime;
        currentEndDateIST = currentEndDateIST + endTime;

        _logger.info(String.format("The Start date is %s",currentStartDateIST));
        _logger.info(String.format("The end date is %s",currentEndDateIST));
        String productSql = String.format("select product_sk,product_name from [public.dim_product]\n" +
                "where customer_id= %s",customerId);
        try {
            Bigquery bigquery = BigQueryUtil.createAuthorizedClient();
            List<TableRow> rows = BigQueryUtil.executeQuery(productSql, bigquery, CommonUtil.projectId);
            String PRODUCTSK = null;
            String PRODUCTNAME = null;
            for (TableRow row : rows) {
                PRODUCTSK = (String)CommonUtil.getRowV(row,0);
                PRODUCTNAME = (String)CommonUtil.getRowV(row,1);
            }
            _logger.info(String.format("Product_sk is %s",PRODUCTSK));
            _logger.info(String.format("Product Name is %s",PRODUCTNAME));
        } catch (IOException e) {
            _logger.warning(e.toString());
            e.printStackTrace();
        }
    }
}
