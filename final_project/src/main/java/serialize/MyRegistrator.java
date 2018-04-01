package serialize;

import be.ac.ulg.montefiore.run.jahmm.ObservationVector;
import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator{
  public void registerClasses(Kryo kryo) {
    kryo.register(ObservationVector.class);
  }
}
