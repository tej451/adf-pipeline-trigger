package com.test.adf;

import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//import okhttp3.FormBody;
//import okhttp3.MediaType;
//import okhttp3.OkHttpClient;
//import okhttp3.Request;
//import okhttp3.RequestBody;
//import okhttp3.Response;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ADFTrigger {

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {

		// make it '>=' to '>' after testing
		if (args.length > 0) {
			/*String resourceGroups = args[0];
			String factorieName = args[1];
			String pipelineName = args[2];*/
			
			String resourceGroups = "g-rsg";
			String factorieName = "g-adf";
			String pipelineName = "";
			System.out.println("resourceGroups :  " + resourceGroups);
			System.out.println("factorieName :  " + factorieName);
			System.out.println("pipelineName :  " + pipelineName);
			// Get Token for authentication
			String access_token = ADFTrigger.getToken();
			// Run pipeline
			String runId = ADFTrigger.pipeRun(access_token, resourceGroups, factorieName, pipelineName);

			String status = ADFTrigger.pipeStatus(access_token, runId, resourceGroups, factorieName);
			// System.out.println("Status :: " + status);

			while (!status.equals("Succeeded")) {
				status = ADFTrigger.pipeStatus(access_token, runId, resourceGroups, factorieName);
				System.out.println("Status :: " + status);
				Thread.sleep(30000);
			}

			if (status.equals("Succeeded")) {
				System.out.println("pipe execution completed...");
			} else if (status.equals("Failed")) {
				System.out.println("pipe execution failed...");
			}

		} else {
			System.out.println("Please pass the argument...");
		}

	}

	public static String getToken() throws IOException, ParseException {

		OkHttpClient client = new OkHttpClient();
		MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
		RequestBody body = RequestBody.create(mediaType,
				"grant_type=client_credentials&client_id=<ClinetID>&client_secret=<Client-Secret>&resource=https%3A%2F%2Fmanagement.azure.com");
		String endpoint = "https://login.microsoftonline.com/<tenant-id>/oauth2/token";
		RequestBody requestBody = new FormBody.Builder().addEncoded("grant_type", "client_credentials")
				.addEncoded("client_id", "<ClinetID>")
				.addEncoded("client_secret", "<Client-Secret>")
				.addEncoded("resource", "https://management.azure.com/").build();
		Request request = new Request.Builder().url(endpoint).post(requestBody).build();
		Response response = client.newCall(request).execute();
		Object object = new JSONParser().parse(response.body().string());
		JSONObject jo = (JSONObject) object;
		return (String) jo.get("access_token");
	}

	public static String pipeRun(String access_token, String resourceGroups, String factorieName, String pipelineName) {

		OkHttpClient client = new OkHttpClient();
		try {
			RequestBody requestBody = RequestBody.create(null, "");
			Request request = new Request.Builder().method("POST", requestBody).url(
					"https://management.azure.com/subscriptions/<Subscription-ID>/resourceGroups/"
							+ resourceGroups + "/providers/Microsoft.DataFactory/factories/" + factorieName
							+ "/pipelines/" + pipelineName + "/createRun?api-version=2018-06-01")
					.addHeader("authority", "management.azure.com").addHeader("content-length", "0")
					.addHeader("Authorization", "Bearer " + access_token)
					.addHeader("origin", "https://docs.microsoft.com").addHeader("content-type", "application/json")
					.addHeader("referer", "https://docs.microsoft.com/en-us/rest/api/datafactory/pipelines/createrun")
					.addHeader("Cache-Control", "no-cache")
					//.addHeader("Postman-Token",
					//		"8f36fd3e-533f-4c57-bc8a-be698b6636d9,73d3e39d-7131-43ae-9b5c-33344678e35b")
					.addHeader("Host", "management.azure.com").build();

			Response response = client.newCall(request).execute();
			// System.out.println("response :::::::::: " + response.message());
			String message = response.message();
			if (message.equals("OK")) {
				Object object = new JSONParser().parse(response.body().string());
				JSONObject jo = (JSONObject) object;
				// String runId = (String) jo.get("runId");
				System.out.println("runId : " + (String) jo.get("runId"));
				return (String) jo.get("runId");

			} else {
				System.out.println("Trigger failed....");
				return null;
			}
		} catch (ParseException p) {
			System.out.println("Exception ============>" + p);
			return null;
		} catch (IOException e) {
			System.out.println("Exception ============>" + e);
			return null;
		}
	}

	public static String pipeStatus(String access_token, String runId, String resourceGroups, String factorieName)
			throws IOException, ParseException {
		OkHttpClient client = new OkHttpClient();

		Request request = new Request.Builder()
				.url("https://management.azure.com/subscriptions/7ceab26f-148c-455c-bdec-84510ab01220/resourceGroups/"
						+ resourceGroups + "/providers/Microsoft.DataFactory/factories/" + factorieName
						+ "/pipelineruns/" + runId + "?api-version=2018-06-01")
				.get().addHeader("Authorization", "Bearer " + access_token)
				.addHeader("User-Agent", "PostmanRuntime/7.20.1")
				// .addHeader("Accept", "*/*")
				.addHeader("content-type", "application/json").addHeader("Cache-Control", "no-cache")
				.addHeader("Postman-Token", "beaa8691-4173-45d6-a78e-50615f12bf64,1576bdad-05ee-4bf1-ab53-6ace893874e6")
				.addHeader("Host", "management.azure.com").build();

		Response response = client.newCall(request).execute();
		// System.out.println("response ;:: " + response);
		Object object = new JSONParser().parse(response.body().string());
		JSONObject jo = (JSONObject) object;
		return (String) jo.get("status");
	}

}
