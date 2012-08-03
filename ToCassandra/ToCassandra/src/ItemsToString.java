import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ItemsToString extends EvalFunc<DataBag> {

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
		String itemsString = "";
		Tuple tuple;

		itemsArray = text.split("\\" + firstToken);

		if (itemsArray.length >= 2) {

			for (String item : itemsArray) {

				productDataArray = item.split("\\" + secondToken);

				if (productDataArray[3].trim().equalsIgnoreCase("1")) {

					itemsString = itemsString + productDataArray[0] + " ";

				} // end if(productDataArray[3].trim().equalsIgnoreCase("1"))

			} // end for(String item:itemsArray)

		} // end if(itemsArray.length >= 2)

		else {

			itemsString = "";

		}

		tuple = tupleFactory.newTuple(2);

		if (!itemsString.equalsIgnoreCase("")
				&& itemsString.split("\\ ").length >= 2) {

			tuple.set(0, "items");
			tuple.set(1, itemsString.trim());
			
			output.add(tuple);
			

		} else {

			output = null;

		}

		

		return output;
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema keyValueStringSchema = new Schema.FieldSchema(
					"product_id", DataType.CHARARRAY);

			Schema tupleSchema = new Schema(keyValueStringSchema);

			Schema.FieldSchema tupleFieldSchema = new Schema.FieldSchema(
					"product", tupleSchema, DataType.TUPLE);

			Schema bagSchema = new Schema(tupleFieldSchema);

			bagSchema.setTwoLevelAccessRequired(true);

			Schema.FieldSchema BagFieldSchema = new Schema.FieldSchema(
					"bag_of_products", bagSchema, DataType.BAG);

			Schema outerBagSchema = new Schema();

			outerBagSchema.add(BagFieldSchema);

			return outerBagSchema;
		} catch (Exception e) {
			return null;
		}
	}

}
