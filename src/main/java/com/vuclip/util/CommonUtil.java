package com.vuclip.util;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Created by Administrator on 2016/7/7.
 */
public class CommonUtil {
    private static final Logger _logger = Logger.getLogger(CommonUtil.class.getName());
    public static String projectId = "vuclipdataflow-1301";
    public static Object getRowV(TableRow row, int index){
        //_logger.info("row.size:" + row.getF().size());
        int indexR = 0;
        for (TableCell field : row.getF()) {
            if(indexR == index) {
                System.out.println(String.format("<TableCell>%-50s", field.getV()));
                if(Data.isNull(field.getV())){
                    _logger.warning("getRowV() return null");
                    return null;
                }
                return field.getV();
            }
            indexR ++;
        }
        return null;
    }
    public static String date_d(String sourceDate,String format) throws ParseException {
        //SimpleDateFormat sdf_ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf_ = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdfNew_ = new SimpleDateFormat(format);
        return sdfNew_.format(sdf_.parse(sourceDate));
    }
    public static String timeNow(String format){
        if(null == format){
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date());
    }
    public static Bigquery.Jobs.Get selectInto(Bigquery bigquery, String sql, TableReference destTable, String writeDisposition) throws IOException {
        _logger.info(sql);
        JobConfigurationQuery copyConfig = new JobConfigurationQuery().setQuery(sql);
        copyConfig.setDestinationTable(destTable);
        //copyConfig.setWriteDisposition("CREATE_IF_NEEDED");
        if(null != writeDisposition){
            copyConfig.setWriteDisposition(writeDisposition);
        }
        Job job = new Job().setConfiguration(new JobConfiguration().setQuery(copyConfig));
        Job jobExe = bigquery.jobs().insert(CommonUtil.projectId, job).execute();

        return bigquery.jobs().get(CommonUtil.projectId, jobExe.getJobReference().getJobId());
    }
    public static Bigquery.Jobs.Get selectInto(Bigquery bigquery,String sql,TableReference destTable) throws IOException{
        return selectInto(bigquery,sql,destTable,"CREATE_IF_NEEDED");
    }
}
