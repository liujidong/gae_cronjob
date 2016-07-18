package com.vuclip.util;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BigQueryUtil {
	  /** View and manage your data in Google BigQuery. */
	  public static final String BIGQUERY = "https://www.googleapis.com/auth/bigquery";

	  /** Insert data into Google BigQuery. */
	  public static final String BIGQUERY_INSERTDATA = "https://www.googleapis.com/auth/bigquery.insertdata";
// [START build_service]
/**
 * Creates an authorized Bigquery client service using Application Default Credentials.
 *
 * @return an authorized Bigquery client
 * @throws IOException if there's an error getting the default credentials.
 */
public static Bigquery createAuthorizedClient() throws IOException {
  // Create the credential
  HttpTransport transport = new NetHttpTransport();
  JsonFactory jsonFactory = new JacksonFactory();
  GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    //GoogleCredential credential = GoogleCredential.getApplicationDefault();
  // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
  // Engine), the credentials may require us to specify the scopes we need explicitly.
  // Check for this case, and inject the Bigquery scope if required.
  if (credential.createScopedRequired()) {
    credential = credential.createScoped(Arrays.asList(BIGQUERY,BIGQUERY_INSERTDATA));
  }

  return new Bigquery.Builder(transport, jsonFactory, credential)
      .setApplicationName("GAE-cron").build();
}

private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
private static final JsonFactory JSON_FACTORY = new JacksonFactory();
public static Bigquery createAuthorizedClientAppEngine() throws IOException {
//    GoogleCredential credential = GoogleCredential.fromStream(BigQueryUtil.class.getResourceAsStream("/VuclipDataFlow-4db276ba902e.json"))
//            .createScoped(Arrays.asList(BigqueryScopes.BIGQUERY,BigqueryScopes.BIGQUERY_INSERTDATA));
    AppIdentityCredential credential =
            new AppIdentityCredential.Builder(Arrays.asList(BIGQUERY,BIGQUERY_INSERTDATA)).build();
    GoogleClientRequestInitializer initializer = new CommonGoogleJsonClientRequestInitializer() {
        public void initialize(AbstractGoogleJsonClientRequest request) {
            BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
            bigqueryRequest.setPrettyPrint(true);
        }
    };
    return new Bigquery.Builder(
            HTTP_TRANSPORT, JSON_FACTORY, credential).setHttpRequestInitializer(credential)
            .setGoogleClientRequestInitializer(initializer).setApplicationName("Bigquery Samples").build();
}
static Bigquery newBigQuery() {
    AppIdentityCredential credential =
            new AppIdentityCredential(Arrays.asList(BigqueryScopes.BIGQUERY,BigqueryScopes.BIGQUERY_INSERTDATA));
    return new Bigquery.Builder(new UrlFetchTransport(), new JacksonFactory(), credential)
            .build();
}
// [END build_service]

// [START run_query]
/**
 * Executes the given query synchronously.
 *
 * @param querySql the query to execute.
 * @param bigquery the Bigquery service object.
 * @param projectId the id of the project under which to run the query.
 * @return a list of the results of the query.
 * @throws IOException if there's an error communicating with the API.
 */
public static List<TableRow> executeQuery(String querySql, Bigquery bigquery, String projectId)
    throws IOException {
  QueryResponse query = bigquery.jobs().query(
      projectId,
      new QueryRequest().setQuery(querySql))
      .execute();

  // Execute it
  GetQueryResultsResponse queryResult = bigquery.jobs().getQueryResults(
      query.getJobReference().getProjectId(),
      query.getJobReference().getJobId()).execute();

  return queryResult.getRows();
}
// [END run_query]

// [START print_results]
/**
 * Prints the results to standard out.
 *
 * @param rows the rows to print.
 */
public static void printResults(List<TableRow> rows) {
  System.out.print("\nQuery Results:\n------------\n");
  for (TableRow row : rows) {
    for (TableCell field : row.getF()) {
      System.out.printf("%-50s", field.getV());
    }
    System.out.println();
  }
}
// [END print_results]


/**
 * Polls the job for completion.
 * @param request The bigquery request to poll for completion
 * @param interval Number of milliseconds between each poll
 * @return The finished job
 * @throws IOException IOException
 * @throws InterruptedException InterruptedException
 */
// [START poll_job]
public static Job pollJob(final Bigquery.Jobs.Get request, final long interval)
    throws IOException, InterruptedException {
  Job job = request.execute();
  while (!job.getStatus().getState().equals("DONE")) {
    System.out.println("Job is "
        + job.getStatus().getState()
        + " waiting " + interval + " milliseconds...");
    Thread.sleep(interval);
    job = request.execute();
  }
  return job;
}
// [END poll_job]


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
