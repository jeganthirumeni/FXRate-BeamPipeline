
package com.aliz.ai;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import com.aliz.ai.common.ExampleBigQueryTableOptions;
import com.aliz.ai.common.ExampleOptions;
import com.aliz.ai.common.ExampleUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

public class FXRate {

	static final int WINDOW_DURATION = 10; // Default sliding window duration in minutes
	static final int WINDOW_SLIDE_EVERY = 1; // Default window 'slide every' setting in minutes
	private static final Logger LOG = LoggerFactory.getLogger(FXRate.class);

	@DefaultCoder(AvroCoder.class)
	static class FXRateInfo {
		@Nullable
		String venue;
		@Nullable
		String currency;
		@Nullable
		Float bidValue;
		@Nullable
		Float askValue;

		public FXRateInfo() {
		}

		public FXRateInfo(String venue, String currency, Float bidValue, Float askValue) {
			this.venue = venue;
			this.currency = currency;
			this.bidValue = bidValue;
			this.askValue = askValue;
		}

		public String getVenue() {
			return venue;
		}

		public String getCurrency() {
			return currency;
		}

		public Float getBidValue() {
			return bidValue;
		}

		public Float getAskValue() {
			return askValue;
		}

	}

	/**
	 * Extract the time stamp field from the input string, and use it as the element
	 * time stamp.
	 */
	static class ExtractDateTime extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;
		private static final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

		@ProcessElement
		public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
			String[] items = c.element().split(",");
			String timestamp = tryParseTimestamp(items);

			if (timestamp != null) {
				try {
					c.outputWithTimestamp(c.element(), new Instant(dateTimeFormat.parseMillis(timestamp)));

				} catch (IllegalArgumentException e) {
					// Skip the invalid input.
				}
			}
		}
	}

	/**
	 * Extract the FX rate and have it as domain object
	 */
	static class ExtractFXRates extends DoFn<String, KV<String, FXRateInfo>> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {
			String[] items = c.element().split(",");
			String venue = tryParseVenue(items);
			String currency = tryParseCurrencies(items);
			Float bidValue = tryParseBidPrice(items);
			Float askValue = tryParseAskPrice(items);
			if (currency.equalsIgnoreCase("GBP/USD")) {
				FXRateInfo fxrateinfo = new FXRateInfo(venue, currency, bidValue, askValue);
				KV<String, FXRateInfo> outputValue = KV.of(venue + "-" + currency, fxrateinfo);
				c.output(outputValue);
			}

		}
	}

	static class GatherAverage extends DoFn<KV<String, Iterable<FXRateInfo>>, KV<String, FXRateInfo>> {

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			String venue = c.element().getKey().split("-")[0];
			String currency = c.element().getKey().split("-")[1];
			Float bidAverageValue = 0.0F;
			Float askAverageValue = 0.0F;
			Float bidTotalValue = 0.0F;
			Float askTotalValue = 0.0F;
			int numberOfValues = 0;
			List<FXRateInfo> fxRateList = Lists.newArrayList(c.element().getValue());
			for (FXRateInfo item : fxRateList) {
				bidTotalValue = bidTotalValue + item.getBidValue();
				askTotalValue = askTotalValue + item.getAskValue();
				numberOfValues++;
			}

			bidAverageValue = bidTotalValue / numberOfValues;
			askAverageValue = askTotalValue / numberOfValues;
			FXRateInfo fxRateInfo = new FXRateInfo(venue, currency, bidAverageValue, askAverageValue);
			c.output(KV.of(venue, fxRateInfo));
		}
	}

	static class FormatFxRate extends DoFn<KV<String, FXRateInfo>, TableRow> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			FXRateInfo fxRateInfo = c.element().getValue();
			TableRow row = new TableRow().set("venue", fxRateInfo.getVenue()).set("currency", fxRateInfo.getCurrency())
					.set("avg_bid_value", fxRateInfo.getBidValue()).set("avg_ask_value", fxRateInfo.getAskValue())
					.set("window_timestamp", c.timestamp().toString());
			c.output(row);
		}

		/** Defines the BigQuery schema used for the output. */
		static TableSchema getSchema() {
			List<TableFieldSchema> fields = new ArrayList<>();
			fields.add(new TableFieldSchema().setName("venue").setType("STRING"));
			fields.add(new TableFieldSchema().setName("currency").setType("STRING"));
			fields.add(new TableFieldSchema().setName("avg_bid_value").setType("FLOAT"));
			fields.add(new TableFieldSchema().setName("avg_ask_value").setType("FLOAT"));
			fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
			return new TableSchema().setFields(fields);
		}
	}

	/**
	 * This PTransform extracts speed info from traffic station readings. It groups
	 * the readings by 'route' and analyzes traffic slowdown for that route. Lastly,
	 * it formats the results for BigQuery.
	 */
	static class FXRateAverage extends PTransform<PCollection<KV<String, FXRateInfo>>, PCollection<TableRow>> {
		private static final long serialVersionUID = 1L;

		@Override
		public PCollection<TableRow> expand(PCollection<KV<String, FXRateInfo>> fxRateInfo) {
			// Apply a GroupByKey transform to collect a list of all station
			// readings for a given route.
			PCollection<KV<String, Iterable<FXRateInfo>>> timegrouping = fxRateInfo.apply(GroupByKey.create());

			// Analyze 'slowdown' over the route readings.
			PCollection<KV<String, FXRateInfo>> groupingStats = timegrouping.apply(ParDo.of(new GatherAverage()));

			// Format the results for writing to BigQuery
			PCollection<TableRow> results = groupingStats.apply(ParDo.of(new FormatFxRate()));

			return results;
		}
	}

	static class ReadFileAndExtractDateTime extends PTransform<PBegin, PCollection<String>> {
		private final String inputFile;
		private static final long serialVersionUID = 1L;

		public ReadFileAndExtractDateTime(String inputFile) {
			this.inputFile = inputFile;
		}

		@Override
		public PCollection<String> expand(PBegin begin) {

			return begin.apply(TextIO.read().from(inputFile)).apply(ParDo.of(new ExtractDateTime()));
		}
	}

	/**
	 * Options supported by {@link TrafficRoutes}.
	 *
	 * <p>
	 * Inherits standard configuration options.
	 */
	// TODO : updated the input file
	public interface FXRateOptions extends ExampleOptions, ExampleBigQueryTableOptions {
		@Description("Path of the file to read from")
		String getInputFile();

		void setInputFile(String value);

		@Description("Numeric value of sliding window duration, in minutes")
		@Default.Integer(WINDOW_DURATION)
		Integer getWindowDuration();

		void setWindowDuration(Integer value);

		@Description("Numeric value of window 'slide every' setting, in minutes")
		@Default.Integer(WINDOW_SLIDE_EVERY)
		Integer getWindowSlideEvery();

		void setWindowSlideEvery(Integer value);
	}

	public static void runFXRate(FXRateOptions options) throws IOException {
		ExampleUtils exampleUtils = new ExampleUtils(options);
		exampleUtils.setup();

		Pipeline pipeline = Pipeline.create(options);
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(options.getBigQueryTable());

		pipeline.apply("ReadLines", new ReadFileAndExtractDateTime(options.getInputFile()))
				.apply(ParDo.of(new ExtractFXRates()))
				// map the incoming data stream into sliding windows.
				.apply(Window.into(SlidingWindows.of(Duration.standardMinutes(options.getWindowDuration()))
						.every(Duration.standardMinutes(options.getWindowSlideEvery()))))
				.apply(new FXRateAverage())
				.apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatFxRate.getSchema()));

		// Run the pipeline.
		PipelineResult result = pipeline.run();

		// ExampleUtils will try to cancel the pipeline and the injector before the
		// program exists.
		exampleUtils.waitToFinish(result);
	}

	/**
	 * Sets up and starts streaming pipeline.
	 *
	 * @throws IOException if there is a problem setting up resources
	 */
	public static void main(String[] args) throws IOException {
		FXRateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FXRateOptions.class);
		options.setBigQuerySchema(FormatFxRate.getSchema());
		//options.setTempLocation("gs://aliz-tech-challenge/temp");
		runFXRate(options);
	}

	private static String tryParseVenue(String[] inputItems) {
		return tryParseString(inputItems, 0);
	}

	private static String tryParseCurrencies(String[] inputItems) {
		return tryParseString(inputItems, 1);
	}

	private static String tryParseTimestamp(String[] inputItems) {
		return tryParseString(inputItems, 2);
	}

	private static Float tryParseBidPrice(String[] inputItems) {
		try {
			return Float.parseFloat(tryParseString(inputItems, 3));
		} catch (NumberFormatException | NullPointerException e) {
			return null;
		}
	}

	private static Float tryParseAskPrice(String[] inputItems) {
		try {
			return Float.parseFloat(tryParseString(inputItems, 4));
		} catch (NumberFormatException | NullPointerException e) {
			return null;
		}
	}

	private static String tryParseString(String[] inputItems, int index) {
		return inputItems.length > index ? inputItems[index] : null;
	}

}
