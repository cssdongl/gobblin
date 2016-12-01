package gobblin.example.simplejson;

import com.google.common.base.Optional;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestJsonFieldGet {
	private static final Logger logger = LoggerFactory.getLogger(TestJsonFieldGet.class);

	private long getRecordTimestamp(Optional<Long> writerPartitionColumnValue) {
		return writerPartitionColumnValue.orNull() instanceof Long ? (Long) writerPartitionColumnValue.get()
				: System.currentTimeMillis();
	}

	private long getRecordTimestamp(byte[] record) {
		return getRecordTimestamp(getWriterPartitionColumnValue(record));
	}

	private Optional<Long> getWriterPartitionColumnValue(byte[] record) {

		Optional<Long> fieldValue = Optional.absent();

		JSONObject jsonObject;
		try {
			jsonObject = new JSONObject(new String(record));
			
			if (jsonObject.get("time") != null) {
				logger.info("the special time partition field value is " + jsonObject.get("time"));
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd/HH");
				Date date;
				try {
					date = format.parse((String) jsonObject.get("time"));
					long finalTime = date.getTime();
					fieldValue = Optional.of(finalTime);
				} catch (ParseException e) {
					logger.info(e.getMessage());
				}
			}
			if (fieldValue.isPresent()) {
				return fieldValue;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return fieldValue;
	}

	public static void main(String[] args) {
		String jsonString = "{\"time\":\"2016/09/18/02\",\"dataquantity\":4096}";
		TestJsonFieldGet tjf = new TestJsonFieldGet();
		long time = tjf.getRecordTimestamp(jsonString.getBytes());
		System.out.println(time);
	}
}
