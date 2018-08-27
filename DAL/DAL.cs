using System;
using System.Data;
using System.Configuration;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.EnterpriseServices;
using System.Data.SqlClient;
using System.Linq;
/*
* This is Intellectual Property of ArachnidCreations. 
* Use is allowed on a limited basis with express and explicit permission of Arachnid Creations or direct employees/members thereof.
*/
namespace ArachnidCreations.DevTools
{
    public class DAL
    {
        
        public static string ConnectionString = string.Empty;
        public static String connectionType = string.Empty;
        private string connString = string.Empty;
        public static List<String> dbLog = new List<String>();
        public DAL()
        {
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ACDALConnString"].ToString();
        }
        public DAL(string _connectionString)
        {
            connString = _connectionString;
        }
        public void SetConnectionStringByName(string name)
        {
            connString = System.Configuration.ConfigurationManager.ConnectionStrings[name].ToString();
        }
        public static void SExec(string txtQuery)
        {
            if (ConnectionString == string.Empty) ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ACDALConnString"].ToString();
           // try
            {
                //The Try is commented out because we WANT things to fail while we are developing to make sure they are going in the DB correctly.

                //string ConnectionString = string.Format("Data Source={0};Initial Catalog={1};User Id={2};Password={3};", MSSQLDatabaseServer, MSSQLDatabaseName, MSSQLDatabaseLogin, MSSQLDatabasePassword);
                System.Data.SqlClient.SqlConnection dbConn = new System.Data.SqlClient.SqlConnection(ConnectionString);
                System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand();
                dbConn.Open();
                sqlCommand = dbConn.CreateCommand();
                sqlCommand.CommandText = txtQuery;
                sqlCommand.ExecuteNonQuery();
                dbConn.Close();
            }
           // catch (Exception e)
            {
               // dbLog += e.Message;
            }
        }
        public void Exec(string txtQuery)
        {
            if (connString == string.Empty) connString = System.Configuration.ConfigurationManager.ConnectionStrings["ACDALConnString"].ToString();
            // try
            {
                //The Try is commented out because we WANT things to fail while we are developing to make sure they are going in the DB correctly.

                //string ConnectionString = string.Format("Data Source={0};Initial Catalog={1};User Id={2};Password={3};", MSSQLDatabaseServer, MSSQLDatabaseName, MSSQLDatabaseLogin, MSSQLDatabasePassword);
                System.Data.SqlClient.SqlConnection dbConn = new System.Data.SqlClient.SqlConnection(connString);
                System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand();
                dbConn.Open();
                sqlCommand = dbConn.CreateCommand();
                sqlCommand.CommandText = txtQuery;
                sqlCommand.ExecuteNonQuery();
                dbConn.Close();
            }
            // catch (Exception e)
            {
                // dbLog += e.Message;
            }
        }
        public static DataTable SLoad(string Commandtext)
        {
            if (ConnectionString == string.Empty) ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ACDALConnString"].ToString();
            DataSet DS = new DataSet();
            //try
            {
                DataTable DT = new DataTable();
                //string ConnectionString = string.Format("Data Source={0};Initial Catalog={1};User Id={2};Password={3};", MSSQLDatabaseServer, MSSQLDatabaseName, MSSQLDatabaseLogin, MSSQLDatabasePassword);
                System.Data.SqlClient.SqlConnection dbConn = new System.Data.SqlClient.SqlConnection(ConnectionString);
                System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand();
                dbConn.Open();
                sqlCommand.CommandTimeout = 2000;
                sqlCommand = dbConn.CreateCommand();
                //Commandtext = "select * from tbl_user_permissions";
                System.Data.SqlClient.SqlDataAdapter DB = new System.Data.SqlClient.SqlDataAdapter(Commandtext, dbConn);
                DS.Reset();
                DB.Fill(DS);
                if (DS.Tables.Count > 0)
                    DT = DS.Tables[0];
                //int rowcount = DT.Rows.Count;
                dbConn.Close();
                // dbLog += "\r\n Rowcount:" + rowcount.ToString();
                return DT;
            }
            //catch (Exception e)
            //{
            //    dbLog.Add(e.Message);
            //    throw e;
            //}
        }
        public DataTable Load(string Commandtext)
        {
            DataSet DS = new DataSet();
            try
            {
                DataTable DT = new DataTable();
                //string ConnectionString = string.Format("Data Source={0};Initial Catalog={1};User Id={2};Password={3};", MSSQLDatabaseServer, MSSQLDatabaseName, MSSQLDatabaseLogin, MSSQLDatabasePassword);
                System.Data.SqlClient.SqlConnection dbConn = new System.Data.SqlClient.SqlConnection(connString);
                System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand();
                dbConn.Open();

                sqlCommand = dbConn.CreateCommand();
                //Commandtext = "select * from tbl_user_permissions";
                System.Data.SqlClient.SqlDataAdapter DB = new System.Data.SqlClient.SqlDataAdapter(Commandtext, dbConn);
                DS.Reset();
                DB.Fill(DS);
                DT = DS.Tables[0];
                //int rowcount = DT.Rows.Count;
                dbConn.Close();
                // dbLog += "\r\n Rowcount:" + rowcount.ToString();
                return DT;
            }
            catch (Exception e)
            {
                dbLog.Add(e.Message);
                return null;
            }
        }
        public static string cleanHTMLString(string html)
        {
            html = html.Replace("\"","'");
            html = html.Replace("'", "''");
            return html;
        }
        public static string sanitize(string strSan)
        {
            strSan.Replace("'", "''");
            strSan.Replace("insert", "_insert_");
            strSan.Replace("delete", "_del_");
            strSan.Replace("select", "_select_");
            strSan.Replace("join", "_join_");
            strSan.Replace("drop", "_drop_");
            strSan.Replace("alter", "_alter_");
            strSan.Replace(";", "");
            strSan.Replace("--", "");
            strSan.Replace("update", "_upate_");
            strSan.Replace("script", "_script_");
            //strSan.Replace("<", "_");
            //strSan.Replace(">", "_");
            return strSan;
        }
        public DataTable getInstancedTableStructure(string tablename)
        {
            string sql = string.Format(@"SELECT     c.name 'ColumnName',    t.Name 'Datatype',    c.max_length 'MaxLength',    c.precision ,    c.scale ,    c.is_nullable 'nullable',
                ISNULL(i.is_primary_key, 0) 'PrimaryKey', is_identity 'Identity', * FROM        sys.columns c
                INNER JOIN     sys.types t ON c.system_type_id = t.system_type_id
                LEFT OUTER JOIN     sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT OUTER JOIN     sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE    c.object_id = OBJECT_ID('{0}')", tablename);
            if (connString != string.Empty) return Load(sql);
            else return SLoad(sql);

        }
        public static DataTable getTableStructure(string tablename)
        {
            var db = "";
            if (tablename.Contains(".dbo"))
            {
                db = tablename.Substring(0, tablename.IndexOf("."));
                db = string.Format("use {0} ;", db);
            }
            if (tablename.Contains("dbo.")) tablename = tablename.Substring(tablename.LastIndexOf(".")+1);
            
            
            string sql = string.Format(@"{1} SELECT     c.name 'ColumnName',    t.Name 'Datatype',    c.max_length 'MaxLength',    c.precision ,    c.scale ,    c.is_nullable 'nullable',
                ISNULL(i.is_primary_key, 0) 'PrimaryKey', is_identity 'Identity', * FROM        sys.columns c
                INNER JOIN     sys.types t ON c.system_type_id = t.system_type_id
                LEFT OUTER JOIN     sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT OUTER JOIN     sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE    c.object_id = OBJECT_ID('{0}')", tablename, db);
            return SLoad(sql);

        }
        public static DataTable BulkInsert<T>(DataTable data)
        {

               // Copy the DataTable to SQL Server using SqlBulkCopy
            using (SqlConnection dbConnection = new System.Data.SqlClient.SqlConnection(ConnectionString))
            {
                dbConnection.Open();
                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection, SqlBulkCopyOptions.KeepNulls, null))
                {
                    s.DestinationTableName = data.TableName;

                    foreach (var column in data.Columns)
                        s.ColumnMappings.Add(column.ToString(), column.ToString());

                    s.WriteToServer(data);
                }
            }
            return data;
        }
    }
}
