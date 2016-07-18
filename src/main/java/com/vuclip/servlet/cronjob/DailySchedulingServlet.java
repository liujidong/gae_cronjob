package com.vuclip.servlet.cronjob;

import com.vuclip.service.bigquery.DailyReportsPRD;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;

//@WebServlet(name = "dataflowscheduler", value = "/dataflow/schedule")
public class DailySchedulingServlet extends HttpServlet {
	private static final Logger _logger = Logger.getLogger(DailySchedulingServlet.class.getName());
	  @Override
	  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		  resp.setContentType("text/html");
		  PrintWriter writer = resp.getWriter();
		  writer.println("<!DOCTYPE html>");
		  writer.println("<title>Daily Report Corn</title>");
		  try {
			  _logger.info("Cron Job has been executed");
			  writer.println("Cron Job has been executed");
//Put your logic here
//BEGIN
			  //Integer customerId = 2;
			  Integer customerId = Integer.parseInt(req.getParameter("customerId"));
			  //SimpleDateFormat sdf_ = new SimpleDateFormat("yyyy-MM-dd");
			  //String today = sdf_.format(new Date());
			  String today = "2016-05-31";
			  String currentStartDateIST = today;// + " 00:00:00";
			  String currentEndDateIST = today;// + " 23:59:59.999";
			  DailyReportsPRD.dailyReportsPRD(customerId,currentStartDateIST,currentEndDateIST);
//END
		  }
		  catch (Exception ex) {
//Log any exceptions in your Cron Job
			  _logger.warning(ex.toString());
			  writer.println(ex.toString());
		  }
		  writer.flush();
		  writer.close();
	  }
}
