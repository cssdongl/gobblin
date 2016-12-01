package gobblin.example.simplejson;

import com.google.common.base.Optional;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(TimeBasedJsonWriterPartitioner.class);

	public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";

	private final Optional<List<String>> partitionColumns;

	public TimeBasedJsonWriterPartitioner(State state) {
		this(state, 1, 0);
	}

	public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
		this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
	}

	private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
		String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
		return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
	}

	private long getRecordTimestamp(Optional<Long> writerPartitionColumnValue) {
		return writerPartitionColumnValue.orNull() instanceof Long ? (Long) writerPartitionColumnValue.get()
				: System.currentTimeMillis();
	}

	@Override
	public long getRecordTimestamp(byte[] record) {
		return getRecordTimestamp(getWriterPartitionColumnValue(record));
	}
	
	
	/**
	 * get the timestamp field in the json record that partition the hdfs dirs.
	 */
	private Optional<Long> getWriterPartitionColumnValue(byte[] record) {
		logger.info("Get the json field begin");
		if (!this.partitionColumns.isPresent()) {
			return Optional.absent();
		}

		Optional<Long> fieldValue = Optional.absent();

		for (String partitionColumn : this.partitionColumns.get()) {
			JSONObject jsonObject;
			try {
				jsonObject = new JSONObject(new String(record));
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date;
				try {
					date = format.parse((String) jsonObject.get(partitionColumn));
					long finalTime = date.getTime();
					fieldValue = Optional.of(finalTime);
				} catch (ParseException e) {
					logger.info(e.getMessage());
				}

				if (fieldValue.isPresent()) {
					return fieldValue;
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return fieldValue;
	}
}
