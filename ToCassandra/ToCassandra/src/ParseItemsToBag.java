import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class ParseItemsToBag extends EvalFunc<DataBag> {
	
	@Override
	public DataBag exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();
		
		DataBag outerBag = bagFactory.newDefaultBag();

		DataBag innerBag = bagFactory.newDefaultBag();
		
		String text = (String) input.get(0);		
		String token = (String) input.get(1);
		
		String[] splitText = text.split("\\" + token);
		
		int n = splitText.length;
		
		
		
		for(int i = 0; i < n; i++) {
			
			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, "item" + i);
			tuple.set(1, splitText[i]);
			
			innerBag.add(tuple);
			
			outerBag.addAll(innerBag);
			
		}
		
		return outerBag;
	}

	
	
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema keyValueFieldSchema = new Schema.FieldSchema("key",DataType.CHARARRAY);
			
			Schema tupleSchema = new Schema(keyValueFieldSchema);

			Schema.FieldSchema tupleFieldSchema = new Schema.FieldSchema("tuple", tupleSchema, DataType.TUPLE);

			Schema innerBagSchema = new Schema(tupleFieldSchema);
			
			innerBagSchema.setTwoLevelAccessRequired(true);
			
			Schema.FieldSchema innerBagFieldSchema = new Schema.FieldSchema("innerBag", innerBagSchema, DataType.BAG);

			
			Schema outerBagSchema = new Schema(innerBagSchema);
			
			outerBagSchema.setTwoLevelAccessRequired(true);
			
			Schema.FieldSchema outerBagFieldSchema = new Schema.FieldSchema("outerBag", outerBagSchema, DataType.BAG);

			
			
			Schema returnSchema = new Schema();
			
			returnSchema.add(outerBagFieldSchema);

			
			
			return returnSchema;
		} catch (Exception e) {
			return null;
		}
	}
	
	
}
