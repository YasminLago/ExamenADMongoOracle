package pasaxeirosvoosoraclemongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author oracle
 */
public class PasaxeirosVoosOracleMongo {

    MongoCollection<Document> coleccion;
    MongoClient cliente;
    MongoDatabase base;
    BasicDBObject consulta;
    BasicDBObject claves;
    
    

    public void conexion(String bd, String colection) {
        cliente = new MongoClient("localhost", 27017);
        base = cliente.getDatabase(bd);
        coleccion = base.getCollection(colection);
    }
    
    public void lerReservas(){
        FindIterable<Document> cursor = coleccion.find();
        MongoCursor<Document> iterator = cursor.iterator();
        int i = 1;
        while (iterator.hasNext()) {
            Document d = iterator.next();
            Double codr = d.getDouble("codr");
            String dni = d.getString("dni");
            Double idVooIda = d.getDouble("idvooida");
            Double idVooVolta = d.getDouble("idvoovolta");
            Double prezoReserva = d.getDouble("prezoreserva");
            Double confirmado = d.getDouble("confirmado");

            System.out.println("Reserva " + i + "\n"
                    + "Codr: " + codr 
                    + " Dni: " + dni 
                    + " IdVooIDA: " + idVooIda
                    + " IdVooVOLTA: " + idVooVolta
                    + " PrezoReserva: " + prezoReserva
                    + " Confirmado: " + confirmado);
        }
    }
    
    public void cambiarConfirmado(){
        coleccion.updateOne(new Document("_id", idx),(new Document("$set",new Document("score",9898))));

    }
    
    public void cerrarConexion() {
        cliente.close();
    }
    
    public static void main(String[] args) {
        PasaxeirosVoosOracleMongo mongo = new PasaxeirosVoosOracleMongo();
        mongo.conexion("internacional", "reserva");
        mongo.lerReservas();
        mongo.cerrarConexion();
    
    }

}
