package pasaxeirosvoosoraclemongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

    String user = "hr";
    String password = "hr";
    String driver = "jdbc:oracle:thin:";
    String host = "localhost.localdomain"; //String host = "1982.168.1.14";
    String porto = "1521";
    String sid = "orcl";
    String url = driver + user + "/" + password + "@" + host + ":" + porto + ":" + sid;
    public static Connection conn;

    /**
     * Conexion a BD
     *
     * @return Retorna a conexion
     */
    public Connection conexionBD() {
        try {
            // Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection(url);
        } catch (Exception e) {
        }
        if (conn != null) {
            System.out.println("Abierta la conexión a la BD");
        } else {
            System.out.println("Conexion fallida");
        }
        return conn;
    }

    public void closeConexion() throws SQLException {
        conn.close();
    }

    public void conexion(String bd, String colection) {
        cliente = new MongoClient("localhost", 27017);
        base = cliente.getDatabase(bd);
        coleccion = base.getCollection(colection);
    }

    public void lerReservas() throws SQLException {
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

            //Cambia o atributo 'confirmado' ao valor 1
            //coleccion.updateOne(new Document("confirmado", confirmado),(new Document("$set",new Document("confirmado",1.0))));
            //Aumenta en 1 o numero de reservas
           /*String aumentar = "UPDATE pasaxeiros SET nreservas=nreservas+1 WHERE dni=" + "'" + dni + "'";
             PreparedStatement stUp = conn.prepareStatement(aumentar);
             stUp.executeUpdate();
             pasajeros();*/
            System.out.println("Reserva " + i++ + "\n"
                    + "Codr: " + codr
                    + " Dni: " + dni
                    + " IdVooIDA: " + idVooIda
                    + " IdVooVOLTA: " + idVooVolta
                    + " PrezoReserva: " + prezoReserva
                    + " Confirmado: " + confirmado);
        }
        cliente.close();

    }

    public void actualizaPrezoReserva() throws SQLException {
        FindIterable<Document> cursor = coleccion.find();
        MongoCursor<Document> iterator = cursor.iterator();
        while (iterator.hasNext()) {
            Document d = iterator.next();
            Double idVooIda = d.getDouble("idvooida");
            Double idVooVolta = d.getDouble("idvoovolta");
            Double prezoReserva = d.getDouble("prezoreserva");

            String prezoVooIda = "SELECT prezo FROM voos WHERE voo=" + idVooIda;
            String prezoVooVolta = "SELECT prezo FROM voos WHERE voo=" + idVooVolta;

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(prezoVooIda);

            Statement stt = conn.createStatement();
            ResultSet rst = stt.executeQuery(prezoVooVolta);

            int idIda = 0;
            int idVolta = 0;

            while (rs.next()) {
                idIda = rs.getInt(1);
            }

            while (rst.next()) {
                idVolta = rst.getInt(1);
            }

            int total = idIda + idVolta;

            coleccion.updateOne(new Document("prezoreserva", prezoReserva), (new Document("$set", new Document("prezoreserva", total))));
        }
        pasajeros();
        cliente.close();
    }

    public void pasajeros() {
        try {
            PreparedStatement st = conn.prepareStatement("SELECT * FROM pasaxeiros");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                System.out.println("Taboa Pasaxeiros:\n"
                        + "DNI: " + rs.getString(1)
                        + ", NOME: " + rs.getString(2)
                        + ", TELEFONO: " + rs.getString(3)
                        + ", CIDADE: " + rs.getString(4)
                        + ", NRESERVAS: " + rs.getInt(5));
            }
        } catch (SQLException ex) {
            System.out.println("Non se pode mostrar a taboa clientes");
        }
    }

    public void cerrarConexion() {
        cliente.close();
    }

    public static void main(String[] args) throws SQLException {
        PasaxeirosVoosOracleMongo mongo = new PasaxeirosVoosOracleMongo();
        mongo.conexionBD();
        mongo.conexion("internacional", "reserva");
        //mongo.lerReservas();
        mongo.actualizaPrezoReserva();
        mongo.closeConexion();

    }

}
