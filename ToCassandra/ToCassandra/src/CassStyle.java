import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class CassStyle extends EvalFunc<Tuple> {

	
	
	
	@Override
	public Tuple exec(Tuple input) throws IOException {

		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory bagFactory = BagFactory.getInstance();
		
		DataBag bag = bagFactory.newDefaultBag();


		String rowkey = (String) input.get(0);
		
	
		
		Tuple tuple = tupleFactory.newTuple(2);
		
		for(int i = 0; i < 4; i++) {
			tuple.set(0, "" + i);
			tuple.set(1, "Hallo Welt");
			
			bag.add(tuple);
		}
		
		Tuple output = tupleFactory.newTuple(2);
		
		output.set(0, rowkey);
		output.set(1, bag);
		
		
		return output;
	}

	
	
	

	
	
}
