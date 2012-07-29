import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;




public class TestSchema extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();

		DataBag itemsBag = bagFactory.newDefaultBag();
		
				Tuple item1Tuple = tupleFactory.newTuple(2);
		
					item1Tuple.set(0, "item1" );
					
						DataBag item1Bag = bagFactory.newDefaultBag();
					
							Tuple sCCS1Tuple = tupleFactory.newTuple(2);
								
									sCCS1Tuple.set(0, "prodCode");
									sCCS1Tuple.set(1, "1234");
								
							Tuple quant1Tuple = tupleFactory.newTuple(2);
								
								quant1Tuple.set(0, "quant");
								quant1Tuple.set(1, "8");
						
							item1Bag.add(sCCS1Tuple);
							item1Bag.add(quant1Tuple);
						
						item1Tuple.set(1, item1Bag);
						
						
						Tuple item2Tuple = tupleFactory.newTuple(2);
						
						item2Tuple.set(0, "item2" );
						
							DataBag item2Bag = bagFactory.newDefaultBag();
						
								Tuple sCCS2Tuple = tupleFactory.newTuple(2);
									
										sCCS2Tuple.set(0, "prodCode");
										sCCS2Tuple.set(1, "9876");
									
								Tuple quant2Tuple = tupleFactory.newTuple(2);
									
									quant2Tuple.set(0, "quant");
									quant2Tuple.set(1, "2");
							
								item2Bag.add(sCCS2Tuple);
								item2Bag.add(quant2Tuple);
							
							item2Tuple.set(1, item2Bag);	
						
						
				itemsBag.add(item1Tuple);
				itemsBag.add(item2Tuple);
		
			
		
		return itemsBag;
	}

	public Schema outputSchema(Schema input) {
		try {

	
			Schema.FieldSchema dataFs = new Schema.FieldSchema("data",DataType.CHARARRAY);
			Schema tupleSchema = new Schema(dataFs);
			
			Schema.FieldSchema tupleFs;
			tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
			DataType.TUPLE);
			
			Schema innerBagSchema = new Schema(tupleFs);
			innerBagSchema.setTwoLevelAccessRequired(true);
			Schema.FieldSchema innerBagFs = new Schema.FieldSchema(
			"bag_of_tokenTuples",innerBagSchema, DataType.BAG);
			
						
			Schema outerBagSchema = new Schema(innerBagFs);
			outerBagSchema.setTwoLevelAccessRequired(true);
			Schema.FieldSchema outerBagFs = new Schema.FieldSchema(
			"bag_of_bags",outerBagSchema, DataType.BAG);
			
			
			
			Schema schema = new Schema();
			schema.add(outerBagFs);
			
			
			
			return schema;
		} catch (Exception e) {
			return null;
		}
	}

}
