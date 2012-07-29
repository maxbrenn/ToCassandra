import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class SplitString extends EvalFunc<DataBag> {
	
	@Override
	public DataBag exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();
		
		DataBag output = bagFactory.newDefaultBag();


		String text = (String) input.get(0);		
		String token = (String) input.get(1);
		
		String[] splitText = text.split("\\" + token);
		
		int n = splitText.length;
		
		Tuple tuple;
		
		for(int i = 0; i < n; i++) {
			
			tuple = tupleFactory.newTuple(2);
			
			tuple.set(0, "item" + i);
			tuple.set(1, splitText[i]);
			
			output.add(tuple);
		}
		
		return output;
	}

	
	
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema keyValueStringSchema = new Schema.FieldSchema("key",
					DataType.CHARARRAY);
			
			Schema tupleSchema = new Schema(keyValueStringSchema);


			Schema.FieldSchema tupleFieldSchema = new Schema.FieldSchema("tuple", tupleSchema,
					DataType.TUPLE);

			Schema bagSchema = new Schema(tupleFieldSchema);
			
			bagSchema.setTwoLevelAccessRequired(true);
			
			Schema.FieldSchema BagFieldSchema = new Schema.FieldSchema("bag", bagSchema, DataType.BAG);

			Schema outerBagSchema = new Schema();
			
			outerBagSchema.add(BagFieldSchema);

			
			
			return outerBagSchema;
		} catch (Exception e) {
			return null;
		}
	}
	
	
}
