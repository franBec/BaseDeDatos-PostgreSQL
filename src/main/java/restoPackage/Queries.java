package restoPackage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

public abstract class Queries {
    
    private static final String DB_NAME = "restoDB";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/" + DB_NAME;
    private static final String DB_USER = "postgres";
    private static final String DB_PWD = "admin";
    
    //Objetos utilizados para interactuar con la base de datos
    private static Connection conn = null;              //conexión
    private static Statement query = null;              //realizar consultas con parámetros
    private static PreparedStatement p_query = null;    //realizar consultas sin parámetros
    
    //método para que alguien inicie la conexión
    public static void connect() throws SQLException{
       conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PWD);
        try {
            firstTimeExecution();
        }
        catch (SQLException ex) {
            //do nothing
        }
        catch (IOException ex) {
            catchIOException(ex);
        }
    }
    
    //intenta ejecutar todo lo que se encuentre en sql\\FirstTimeExecution.sql
    private static void firstTimeExecution() throws SQLException, IOException{
        query = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        String line;
        BufferedReader buffer = new BufferedReader(new FileReader("sql\\FirstTimeExecution.sql"));        
        while ((line = buffer.readLine()) != null){
            sb.append(line);
            sb.append("\n");
        }
        String q = sb.toString();
        query.execute(q);
    }
    
    //--QUERIES

    //Query 1 Mozos
    public static ResultSet selectAll_Mozos() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT * "+
            "FROM mozos;"
        );
    }
    
    //Query 1 Platos
    public static ResultSet selectAll_Platos() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT * "+
            "FROM platos;"
        );
    }
    
    //Query 2
    public static int insert_Plato(int p_Cod, String p_Nombre, String p_Descripcion, String p_Tipo, int p_PrecioCosto, int p_PrecioVenta, int p_PrecioPromocion) throws SQLException{
        p_query = conn.prepareStatement("INSERT INTO platos VALUES (?, ?, ?, ?, ?, ?, ?);");
        p_query.setInt(1, p_Cod);
        p_query.setString(2, p_Nombre);
        p_query.setString(3, p_Descripcion);
        p_query.setString(4, p_Tipo);
        p_query.setInt(5, p_PrecioCosto);
        p_query.setInt(6, p_PrecioVenta);
        p_query.setInt(7, p_PrecioPromocion);
        
        return p_query.executeUpdate();
    }
    
    //Query 3
    public static int delete_Mozo(int mo_Cod)throws SQLException{
        p_query = conn.prepareStatement("DELETE FROM mozos WHERE mo_Cod = ?;");
        p_query.setInt(1, mo_Cod);
        return p_query.executeUpdate();
    }
    
    //Query 4
    public static ResultSet mesasAtendidas_byMozoCOD(int mo_Cod_Atiende) throws SQLException{
        p_query = conn.prepareStatement(
            "SELECT Me_Cod, Me_Sector " +
            "FROM mesas " +
            "WHERE Mo_Cod_Atiende = ?;"
        );
        p_query.setInt(1, mo_Cod_Atiende);
        return p_query.executeQuery();
    }
    
    //Query 5
    public static ResultSet platosConsumidos_byMesaCOD(int me_Cod_Realiza) throws SQLException{
        p_query = conn.prepareStatement(
            "SELECT platos.P_Cod, P_Descripcion, COUNT(platos.P_Cod) " +
            "FROM platos, se_consume, consumos " +
            "WHERE Me_Cod_Realiza = ? " +
            "AND se_consume.C_Cod = consumos.C_Cod " +
            "AND platos.P_Cod = se_consume.P_Cod " +
            "GROUP BY platos.P_Cod, P_Descripcion;"
        );
        p_query.setInt(1, me_Cod_Realiza);
        return p_query.executeQuery();
    }
    
    //Query 6
    public static ResultSet platosBetweenFechas(java.util.Date inicio, java.util.Date fin) throws SQLException{
        p_query = conn.prepareStatement(
            "SELECT DISTINCT platos.P_Cod, platos.P_Nombre " +
            "FROM platos, consumos, se_consume " +
            "WHERE " +
                "se_consume.C_Cod = consumos.C_Cod " +
                "AND platos.P_Cod = se_consume.P_Cod " +
                "AND consumos.C_Fecha BETWEEN ? AND ?;"
        );
        p_query.setDate(1, new java.sql.Date(inicio.getTime()));
        p_query.setDate(2, new java.sql.Date(fin.getTime()));

        return p_query.executeQuery();
    }
    
    //Query 7
    public static ResultSet platosMasConsumidos_byTipo() throws SQLException{
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT TABLA_TodasLasComidas2.P_Nombre, TABLA_TipoxMax.P_Tipo, TABLA_TipoxMax.max " +
            "FROM (" +
                "SELECT P_Tipo,MAX(count) " +
                "FROM (SELECT P_Tipo, P_Nombre, COUNT(platos.P_Cod) " +
                    "FROM platos, se_consume " +
                    "WHERE platos.P_Cod = se_consume.P_Cod " +
                    "GROUP BY P_Tipo, P_Nombre " +
                ") AS TABLA_TodasLasComidas1 " +
                "GROUP BY TABLA_TodasLasComidas1.P_Tipo) AS TABLA_TipoxMax, " +
                "(SELECT P_Tipo, P_Nombre, COUNT(platos.P_Cod) " +
                    "FROM platos, se_consume " +
                    "WHERE platos.P_Cod = se_consume.P_Cod " +
                    "GROUP BY P_Tipo, P_Nombre " +
                ") AS TABLA_TodasLasComidas2 " +
            "WHERE TABLA_TodasLasComidas2.P_Tipo=TABLA_TipoxMax.P_Tipo " +
            "AND TABLA_TipoxMax.max=TABLA_TodasLasComidas2.count;"
        );
    }
    
    //Query 8 Mozos
    public static ResultSet count_Mozos() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT COUNT (*) " +
            "FROM mozos;"
        );
    }
    
    //Query 8 Mesas
    public static ResultSet count_Mesas() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT COUNT (*) " +
            "FROM mesas;"
        );
    }
    
    //Query 8 Entradas
    public static ResultSet count_Entradas() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT COUNT (*) " +
            "FROM platos " +
            "WHERE P_Tipo = 'Entrada';"
        );
    }

    //Query 8 Platos Principales
    public static ResultSet count_PlatosPrincipales() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT COUNT (*) " +
            "FROM platos " +
            "WHERE P_Tipo = 'Plato Principal';"
        );
    }
    
    //Query 8 Platos Principales
    public static ResultSet count_Postres() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT COUNT (*) " +
            "FROM platos " +
            "WHERE P_Tipo = 'Postre';"
        );
    }
    
    //Query 9
    public static ResultSet mozos_x_cantidadMesas() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT Mo_nombreapellido, COUNT (*) " +
            "FROM mozos,mesas " +
            "WHERE Mo_Cod = Mo_Cod_Atiende " +
            "GROUP BY Mo_nombreapellido " +
            "ORDER BY Mo_nombreapellido ASC;"
        );
    }
    
    //Query 10
    public static ResultSet mozosDisponibles() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT DISTINCT Mo_cod, Mo_nombreapellido " +
            "FROM mozos,mesas " +
            "WHERE Mo_Cod NOT IN ( " +
            "	SELECT Mo_Cod_Atiende " +
            "	FROM mesas " +
            ");"
        );
    }
    
    //Query 11
    public static ResultSet maxMinAvg_CostoPlatosPrincipales() throws SQLException {
        //parametros soluciona PSQLException: Operation requires a scrollable ResultSet, but this ResultSet is FORWARD_ONLY
        query = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return query.executeQuery(
            "SELECT MAX(P_PrecioCosto), MIN(P_PrecioCosto), AVG(P_PrecioCosto) " +
            "FROM platos;"
        );
    }
    
    //Query 12
    public static ResultSet platosNuncaConsumidos() throws SQLException {
        query = conn.createStatement();
        return query.executeQuery(
            "SELECT P_Nombre, P_Descripcion " +
            "FROM platos " +
            "WHERE P_Cod NOT IN ( " +
            "	SELECT P_Cod " +
            "   FROM Se_Consume " +
            "   GROUP BY P_Cod " +
            ");"
        );
    }
    
    //Query 13
    public static ResultSet count_PlatosConsumidos_byMesaCOD(int me_Cod_Realiza) throws SQLException{
        p_query = conn.prepareStatement(
            "SELECT COUNT (*) " +
            "FROM ( "+
                "SELECT platos.P_Cod " +
                "FROM platos, se_consume, consumos " +
                "WHERE Me_Cod_Realiza = ? " +
                "AND se_consume.C_Cod = consumos.C_Cod " +
                "AND platos.P_Cod = se_consume.P_Cod) AS foo;"
        );
        p_query.setInt(1, me_Cod_Realiza);
        return p_query.executeQuery();
    }
    
    //file not found
    private static void catchIOException(Exception e){
        Logger.getLogger(Queries.class.getName()).log(Level.SEVERE, null, e);
        String mensaje = "Algo salió mal con la lectura de un archivo\nPosible causa: El archivo del que se desea leer no se encontró";
        JOptionPane.showMessageDialog(null, mensaje,"Error en la creación de las tablas",JOptionPane.ERROR_MESSAGE);
    }
}
