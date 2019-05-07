package cc.karakoram.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;

public class HiveJdbc4Kerberos {

    public static Connection newInstance(String kerberosUser, String krb5ConfPath, String keytabPath, String driverName, String url) {
        Connection conn = null;
        try {
            //登录Kerberos账号
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            System.setProperty("java.security.krb5.conf", krb5ConfPath);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kerberosUser, keytabPath);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url);
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        return conn;
    }
}
