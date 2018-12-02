package pdzd.UkAccidents;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient {

public static void main(String[] args) throws IOException{
		
		Configuration conf = HBaseConfiguration.create();
		
//		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
//		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		
		try{
			TableName liczbaKierowcow = TableName.valueOf("Liczba_Kierowcow");
			TableName liczbaOfiar = TableName.valueOf("Liczba_Ofiar");
			TableName liczbaWypadkow = TableName.valueOf("Liczba_Wypadkow");
			
			HTableDescriptor htd1 = new HTableDescriptor(liczbaKierowcow);
			htd1.addFamily(new HColumnDescriptor("accidentIndex"));
			htd1.addFamily(new HColumnDescriptor("rok"));
			htd1.addFamily(new HColumnDescriptor("miesiac"));
			htd1.addFamily(new HColumnDescriptor("dzien"));
			htd1.addFamily(new HColumnDescriptor("grupaWiekowa"));
			htd1.addFamily(new HColumnDescriptor("plec"));
			htd1.addFamily(new HColumnDescriptor("procentowaZmiana"));
			
			System.out.println("Create table " + htd1.getNameAsString());
			admin.createTable(htd1);
			
			HTableDescriptor htd2 = new HTableDescriptor(liczbaOfiar);
			htd2.addFamily(new HColumnDescriptor("accidentIndex"));
			htd2.addFamily(new HColumnDescriptor("strona_uderzenia"));
			htd2.addFamily(new HColumnDescriptor("typ_pojazdu"));
			htd2.addFamily(new HColumnDescriptor("rok"));
			System.out.println("Create table " + htd2.getNameAsString());
			admin.createTable(htd2);
			
			HTableDescriptor htd3 = new HTableDescriptor(liczbaWypadkow);
			htd3.addFamily(new HColumnDescriptor("accidentIndex"));
			htd3.addFamily(new HColumnDescriptor("polozenie"));
			htd3.addFamily(new HColumnDescriptor("pora_roku"));
			htd3.addFamily(new HColumnDescriptor("warunki_pogodowe"));
			htd3.addFamily(new HColumnDescriptor("warunki_jezdni"));
			System.out.println("Create table " + htd3.getNameAsString());
			admin.createTable(htd3);
			
			HTableDescriptor[] tables = admin.listTables();
			
			/*
			if(tables.length < 3){
				if( Bytes.equals(liczbaOfiar.getName(), tables[0].getTableName())
					throw new IOException("Failed to create Liczba_Kierowcow table");
				else if(Bytes.equals(liczbaOfiar.getName(), tables[0].getTableName()))
					throw new IOException("Failed to create Liczba_Kierowcow table");
				else if(Bytes.equals(liczbaOfiar.getName(), tables[0].getTableName()))
					throw new IOException("Failed to create Liczba_Kierowcow table");
			}*/
		}
		catch(Exception e){
			System.out.println("Exception: ");
			e.printStackTrace();
		}
		finally {
			System.out.println("Closing all connections");
			admin.close();
			con.close();
		}
				
	}

}
