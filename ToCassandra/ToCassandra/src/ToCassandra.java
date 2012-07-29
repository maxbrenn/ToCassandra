import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ToCassandra extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();

		DataBag bag = bagFactory.newDefaultBag();

		String rowkey = (String) input.get(0) + (String) input.get(1);

		String text = (String) input.get(2);
		String token = (String) input.get(3);

		String[] splitText = text.split("\\" + token);

		int n = splitText.length;

		Tuple tuple;

		for (int i = 0; i < n; i++) {
			
			tuple = tupleFactory.newTuple(2);
			
			tuple.set(0, "item" + i);
			tuple.set(1, splitText[i]);

			bag.add(tuple);
		}

		Tuple output = tupleFactory.newTuple(2);

		output.set(0, rowkey);
		output.set(1, bag);

		return output;
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema tokenFs = new Schema.FieldSchema("token",
					DataType.CHARARRAY);
			Schema tupleSchema = new Schema(tokenFs);


			Schema.FieldSchema tupleFs = new Schema.FieldSchema("tuple", tupleSchema,
					DataType.TUPLE);

			Schema bagSchema = new Schema(tupleFs);
			bagSchema.setTwoLevelAccessRequired(true);
			Schema.FieldSchema bagFs = new Schema.FieldSchema(
					"bag", bagSchema, DataType.BAG);

			Schema schema = new Schema();
			schema.add(tokenFs);
			schema.add(bagFs);

			
			
			return schema;
		} catch (Exception e) {
			return null;
		}
	}

}
