import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class RetreiveItems extends EvalFunc<DataBag> {
	
	@Override
	public DataBag exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();
		
		DataBag output = bagFactory.newDefaultBag();


		String text = (String) input.get(0);		
		String firstToken = (String) input.get(1);
		String secondToken = (String) input.get(2);
		
		
		String[] itemsArray;
		String[] productDataArray;
		Tuple tuple;
		
		itemsArray = text.split("\\" + firstToken);
				
		for(String item:itemsArray) {
			
			productDataArray = item.split("\\" + secondToken);
			
				
			
			if(productDataArray[3].trim().equalsIgnoreCase("1")) {
				

				tuple = tupleFactory.newTuple(2);
				
				tuple.set(0, productDataArray[0]);
				tuple.set(1, "");
				
				output.add(tuple);
					
						
				
			}
			
			
		}
		
		
		return output;
	}

	
	
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema keyValueStringSchema = new Schema.FieldSchema("product_id",
					DataType.CHARARRAY);
			
			Schema tupleSchema = new Schema(keyValueStringSchema);


			Schema.FieldSchema tupleFieldSchema = new Schema.FieldSchema("product", tupleSchema,
					DataType.TUPLE);

			Schema bagSchema = new Schema(tupleFieldSchema);
			
			bagSchema.setTwoLevelAccessRequired(true);
			
			Schema.FieldSchema BagFieldSchema = new Schema.FieldSchema("bag_of_products", bagSchema, DataType.BAG);

			Schema outerBagSchema = new Schema();
			
			outerBagSchema.add(BagFieldSchema);

			
			
			return outerBagSchema;
		} catch (Exception e) {
			return null;
		}
	}
	
	
}
