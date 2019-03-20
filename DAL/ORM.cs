using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Data;
using System.Collections;

namespace ArachnidCreations.DevTools
{
    public class ORM
    {
        public static DAL dal = null;
        public bool WithNoLock = true;
        private static Dictionary<string, DataTable> TableStructures = new Dictionary<string, DataTable>();
        /// <summary>
        /// Generates an insert statement based off table structure information and an object of your choosing (any object)
        /// this will ignore any properties on your object that do not match a table name.
        /// </summary>
        ///
        /// <param name="userClass"></param>
        /// <param name="tablename"></param>
        /// <param name="fieldprefix"></param>
        /// <param name="returnId"></param>
        /// <param name="dt">Only pass null if your table matches the class EXACTLY. DataTable of structure defined in the getTableStructureData sql statement. Or the TableStructure object.</param>
        /// <returns></returns>
        public static string Insert<T>(T userClass, string tablename, string fieldprefix = "", bool returnId = false, DataTable dt = null)
        {
            string pk = string.Empty;
            //Compile a List<T> of colmumn names (with datatypes) from the target table
            if (dt == null)
            {
                dt = GetDT(tablename);
            }

            List<string> cols = new List<string>();
            if (dt != null)
            {
                foreach (DataRow row in dt.Rows)
                {
                    cols.Add(row["columnname"].ToString());
                }
            }
            if (!cols.Contains("id") && !cols.Contains("ID") && !cols.Contains("Id"))
            {
                string pkt = GetPrimaryKey(tablename);
                if (pkt != null) pk = pkt.ToLower();
            }
            else pk = "id";

            List<PropertyInfo> matchedProps = new List<PropertyInfo>();
            StringBuilder sql = new StringBuilder();
            //Select only the properties that are system types (they will match sql data types mostly) as a type of "User" wont match any sql field.
            List<PropertyInfo> props = userClass.GetType().GetProperties().Where(p => p.PropertyType.ToString().ToLower().Contains("system.") && p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumnIgnoreOnInsert").FirstOrDefault() == null).ToList();
            //props = props.Where(p => p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumnIgnoreOnInsert").FirstOrDefault() == null).ToList();
            foreach (var col in cols.Distinct())
            {
                //skip the id - we assume this is here for all tables
                //one day well have a primary key variable passsed in... but not today.
                if (col.ToLower() != pk)
                {
                    //WE only want to create sql code for the properties that are in our object that MATCH the sql fields. 
                    //otherwise we will create empty insert values that wont match the table.
                    PropertyInfo item = null;
                    var att = props.Where(p => p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault() != null).ToList();
                    if (att != null)
                    {
                        item = att.Where(p => p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault().ConstructorArguments.FirstOrDefault().Value.ToString().ToLower() == col.ToLower()).FirstOrDefault();
                        if (item != null) matchedProps.Add(item);
                    }
                    if (item == null)
                    {
                        var propMatch = props.Where(p => p.Name.ToLower() == col.ToLower()).FirstOrDefault();
                        if (propMatch != null)
                        {
                            matchedProps.Add(propMatch);
                        }
                    }
                    else
                    {
                        Console.WriteLine(col + " does not match properties");
                    }
                }
            }
            //Console.Write(matchedProps);
            sql.Append(string.Format("insert into {0} (", tablename));
            int propcount = 0;
            foreach (PropertyInfo prop in matchedProps)
            {

                propcount++;
                //"id" shouldnt be in here so removed the if statement - Will 7/12/2013
                if (prop.Name != "id" && prop.Name != "Id" && prop.Name != "ID" && prop.Name != "Id")
                {
                    if (prop.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault() != null)
                        sql.Append(prop.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault().ConstructorArguments.FirstOrDefault().Value.ToString().ToLower());
                    else sql.Append(prop.Name);
                    if (propcount != matchedProps.Count) sql.Append(",");
                }


            }
            sql.Append(") values (");
            propcount = 0; //starting at one because we are going to skip a property called id 
            foreach (PropertyInfo prop in matchedProps)
            {
                propcount++;
                //ToDo: make sure the property is not the primary key before inserting it
                if (prop.Name.ToLower() != pk && prop.Name != "id" && prop.Name != "Id" && prop.Name != "ID" && prop.Name != "Id") // we want to skip ID since it usually cant be inserted
                {
                    if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                    {
                        sql.Append(string.Format("'{0:s}'", prop.GetValue(userClass)));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("bool"))
                    {
                        string insert = "";
                        if (prop.GetValue(userClass) != null)
                        {
                            if (prop.GetValue(userClass).ToString().ToLower() == "true") insert = "1";
                            else insert = "0";
                        }
                        sql.Append(string.Format("'{0}'", insert));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("int32"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("'{0}'", prop.GetValue(userClass).ToString()));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("int64"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("'{0}'", prop.GetValue(userClass).ToString()));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("long"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("'{0}'", prop.GetValue(userClass).ToString()));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("{0}", prop.GetValue(userClass).ToString()));
                    }
                    else
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("'{0}'", prop.GetValue(userClass).ToString().Replace("'", "''")));
                        else sql.Append(string.Format("'{0}'", ""));
                    }
                    if (propcount != matchedProps.Count) sql.Append(",");
                }
            }
            sql.Append(");");
            if (returnId == true)
            {
                sql.Append(" select SCOPE_IDENTITY();");
                var dto = DAL.SLoad(sql.ToString());
                if (dt.Rows.Count > 0)
                {
                    return dto.Rows[0][0].ToString();
                }
            }
            else
            {
                DAL.SExec(sql.ToString());
            }
            return "";
        }

        /// <summary>
        /// Automatically assumes your object and your table has an "id" column/property. If it doesnt. This wont work.
        /// </summary>
        /// <param name="userClass"></param>
        /// <param name="tablename"></param>
        /// <param name="fieldprefix"></param>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static string Update<T>(T userClass, string tablename, string primarykey = "", DataTable dt = null)
        {
            if (primarykey == "") primarykey = "id";
            if (dt == null)
            {
                dt = GetDT(tablename);
            }
            List<string> cols = new List<string>();
            if (dt != null)
            {
                foreach (DataRow row in dt.Rows)
                {
                    cols.Add(row["columnname"].ToString());
                }
            }
            List<PropertyInfo> matchedProps = new List<PropertyInfo>();
            StringBuilder sql = new StringBuilder();
            List<PropertyInfo> props = userClass.GetType().GetProperties().Where(p => p.PropertyType.ToString().ToLower().Contains("system.") && p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumnIgnoreOnUpdate").FirstOrDefault() == null).ToList();
            foreach (var col in cols.Distinct())
            {
                if (String.Compare(col, primarykey, true) != 0)
                {
                    //WE only want to create sql code for the properties that are in our object that MATCH the sql fields. 
                    //otherwise we will create empty insert values that wont match the table.
                    PropertyInfo item = null;
                    var att = props.Where(p => p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault() != null).ToList();
                    if (att != null)
                    {
                        item = att.Where(p => p.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault().ConstructorArguments.FirstOrDefault().Value.ToString().ToLower() == col.ToLower()).FirstOrDefault();
                        if (item != null) matchedProps.Add(item);
                    }
                    if (item == null)
                    {
                        var propMatch = props.Where(p => p.Name.ToLower() == col.ToLower()).FirstOrDefault();
                        if (propMatch != null)
                        {
                            matchedProps.Add(propMatch);
                        }
                    }
                }
            }
            sql.Append(string.Format("update  {0} set ", tablename));
            int propcount = 0;
            foreach (PropertyInfo prop in matchedProps)
            {

                propcount++;
                //ToDo: make sure the property is not the primary key before inserting it
                if (String.Compare(prop.Name, primarykey, true) != 0) // we want to skip ID since it usually cant be inserted... and make sure the property exists in the column names
                {
                    string propname = prop.Name;
                    if (prop.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault() != null)
                        propname = (prop.CustomAttributes.Where(a => a.AttributeType.Name == "DBColumn").FirstOrDefault().ConstructorArguments.FirstOrDefault().Value.ToString().ToLower());

                    if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                    {
                        sql.Append(string.Format("{1} ='{0:s}'", prop.GetValue(userClass), propname));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("bool"))
                    {
                        string insert = "";
                        if (prop.GetValue(userClass) != null)
                        {
                            if (prop.GetValue(userClass).ToString().ToLower() == "true") insert = "1";
                            else insert = "0";
                        }
                        sql.Append(string.Format("{1} ='{0}'", insert, propname));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("int32"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("{1} ='{0}'", prop.GetValue(userClass).ToString().Replace("'", "''"), propname));
                    }
                    else if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("{1} ={0}", prop.GetValue(userClass).ToString(), propname));
                    }
                    else
                    {
                        if (prop.GetValue(userClass) != null) sql.Append(string.Format("{1} ='{0}'", prop.GetValue(userClass).ToString().Replace("'", "''"), propname));
                        else sql.Append(string.Format("{0} =''", propname));
                    }
                    if (propcount != (matchedProps.Count)) sql.Append(",");
                }
            }
            var property = userClass.GetType().GetProperty(primarykey);
            if (property == null) property = userClass.GetType().GetProperty(primarykey);
            var altPrimarykey = primarykey.Substring(0, 1);
            var capAltPrimaryKey = primarykey = altPrimarykey.ToUpper() + primarykey.Substring(1, primarykey.Length - 1);
            if (property == null) property = userClass.GetType().GetProperty(capAltPrimaryKey);
            sql.Append(string.Format(" where {1}='{0}';", property.GetValue(userClass), primarykey));
            return sql.ToString();
        }
        public static string Update<T>(T userClass)
        {
            // Using reflection.
            System.Attribute[] attrs = System.Attribute.GetCustomAttributes(userClass.GetType());  // Reflection. 
            var TableName = "";
            foreach (System.Attribute attr in attrs)
            {
                if (attr is DBTable)
                {
                    TableName = ((DBTable)attr).GetName();
                }
            }
            return Update(userClass, TableName, GetPrimaryKey(TableName), GetDT(TableName));
        }
        private static DataTable GetDT(string tablename)
        {
            DataTable dt = null;
            dt = TableStructures.Where(t => t.Key == tablename).FirstOrDefault().Value;
            if (dt == null)
            {
                if (dal == null) dt = DAL.getTableStructure(tablename);
                else dt = dal.getInstancedTableStructure(tablename);
                if (dt != null)
                {
                    TableStructures.Add(tablename, dt);
                }
            }
            return dt;
        }
        public static T GetSingle<T>(string tablename, int id, string keyname)
        {
            return GetSingle<T>(null, tablename, id.ToString(), keyname);
        }
        public static T GetSingle<T>(DAL dal, string tablename, int id, string keyname)
        {
            return GetSingle<T>(dal, tablename, id.ToString(), keyname);
        }
        public static T GetSingle<T>(DAL dal, string tablename, string id, string keyname, string orderby = null)
        {
            var sql = string.Format("select top 1 * from {0} where {2} = '{1}' {3}", tablename, id, keyname, orderby);
            DataTable dt = null;
            if (dal == null) dt = DAL.SLoad(sql);
            else dt = dal.Load(sql);
            if (dt == null) return default(T);
            if (dt != null && dt.Rows.Count == 0) return default(T);
            int icount = 0;
            var instance = (T)Activator.CreateInstance(typeof(T));

            //foreach (DataRow row in dt.Rows)
            {
                icount++;
                DataRow row = null;
                if (dt.Rows.Count == 0) DAL.dbLog.Add("No rows in result: " + sql);
                else row = dt.Rows[0];
                List<string> ColumnNames = new List<string>();
                foreach (DataColumn item in row.Table.Columns)
                {
                    string columnName = item.ColumnName;
                    ColumnNames.Add(columnName.ToLower());
                }

                PropertyInfo[] props = instance.GetType().GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    if (prop.PropertyType.ToString().ToLower().Contains("string"))
                    {
                        if (ColumnNames.Where(s => s == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, row[prop.Name].ToString());
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("int"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            int i = 0;
                            int.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("float"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, float.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                    {
                        DateTime now = DateTime.Now;

                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            DateTime.TryParse(row[prop.Name].ToString(), out now);
                            prop.SetValue(instance, now);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("double"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, double.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            decimal i = 0;
                            decimal.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("bool") && ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                    {
                        if (row[prop.Name].ToString() != "" && row[prop.Name].ToString() != null)
                        {
                            int torfint = 0;
                            int.TryParse(row[prop.Name].ToString(), out torfint);
                            bool torf = false;
                            if (torfint == 1) torf = true;
                            if (torfint == 0) torf = false;
                            if (row[prop.Name].ToString().ToLower() == "false") torf = false;
                            if (row[prop.Name].ToString().ToLower() == "true") torf = true;
                            prop.SetValue(instance, torf);
                        }
                        else
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, false);
                        }
                    }
                }//
            }
            return instance;
        }
        public static string GetSingleValue(string tablename, string key, string id, string column)
        {
            DataTable dt = DAL.SLoad(string.Format("select top 1 {0} from {1} where {2} = '{3}'", column, tablename, key, id));
            if (dt != null && dt.Rows.Count > 0)
            {
                return dt.Rows[0][column].ToString();
            }
            else return null;
        }
        /// <summary>
        /// This assumes you have decorated your class with your table name - gets all the records and loads them in the cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static List<T> GetList<T>()
        {
            var tablename = GetTableName<T>();
            return GetList<T>(null, tablename, null);
        }
        public static List<T> GetList<T>(DAL dal)
        {
            var tablename = GetTableName<T>();
            return GetList<T>(dal, tablename, null);
        }
        /// <summary>
        /// This will get an IList<T> of objects from your database - you just have to pass in the type and the tablename thats accessible via the connection string in your app.config/web.config
        /// </summary>
        /// <param name="objtype"></param>
        /// <param name="tablename"></param>
        /// <param name="where">NOT IMPLEMENTED: a dictionary<string, string> of where clauses such as ("uploaddate", "between 2013-01-01 and 2014-01-01")</param>
        /// <returns></returns>
        public static List<T> GetList<T>(DAL dal, string tablename, Dictionary<string, string> where, bool or = false, int limit = 10000)
        {
            Type customList = typeof(List<>).MakeGenericType(typeof(T));
            IList objectList = (IList)Activator.CreateInstance(customList);

            string sql = string.Format("select top {1} * from {0}", tablename, limit);
            if (where != null)
            {
                if (where.Count > 0)
                {
                    var iCount = 0;
                    foreach (var item in where)
                    {
                        iCount++;
                        if (or)
                        {
                            if (iCount == 1) sql += string.Format(" where [{0}] = '{1}' ", item.Key, item.Value);
                            else sql += string.Format(" or [{0}] = '{1}' ", item.Key, item.Value);
                        }
                        else
                        {
                            if (iCount == 1) sql += string.Format(" where [{0}] = '{1}' ", item.Key, item.Value);
                            else sql += string.Format(" and [{0}] = '{1}' ", item.Key, item.Value);
                        }
                    }
                }
            }
            DataTable dt = null;
            if (dal == null) dt = DAL.SLoad(sql);
            else dt = dal.Load(sql);
            int icount = 0;
            if (dt == null || dt.Rows.Count == 0) return (List<T>)objectList;
            List<string> ColumnNames = new List<string>();
            foreach (DataColumn item in dt.Rows[0].Table.Columns)
            {
                string columnName = item.ColumnName;
                ColumnNames.Add(columnName.ToLower());
            }

            foreach (DataRow row in dt.Rows)
            {
                icount++;
                var instance = Activator.CreateInstance(typeof(T));

                PropertyInfo[] props = instance.GetType().GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    if (prop.PropertyType.ToString().ToLower().Contains("string"))
                    {
                        if (ColumnNames.Where(s => s == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, row[prop.Name].ToString());
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("int"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            int i = 0;
                            int.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("int32"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            Int32 i = 0;
                            Int32.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("int64"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            Int64 i = 0;
                            Int64.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("long"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            long i = 0;
                            long.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("float"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, float.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                    {
                        DateTime now = DateTime.Now;

                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            DateTime.TryParse(row[prop.Name].ToString(), out now);
                            prop.SetValue(instance, now);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("double"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, double.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            decimal i = 0;
                            decimal.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("bool") && ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                    {
                        if (row[prop.Name].ToString() != "" && row[prop.Name].ToString() != null)
                        {
                            int torfint = 0;
                            int.TryParse(row[prop.Name].ToString(), out torfint);
                            bool torf = false;
                            if (torfint == 1) torf = true;
                            if (torfint == 0) torf = false;
                            if (row[prop.Name].ToString().ToLower() == "false") torf = false;
                            if (row[prop.Name].ToString().ToLower() == "true") torf = true;
                            prop.SetValue(instance, torf);
                        }
                        else
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, false);
                        }
                    }
                }
                objectList.Add(instance);
            }
            return (List<T>)objectList;
        }
        public static List<T> convertDataTabletoObject<T>(DataTable dataTable)
        {
            int icount = 0;
            DataTable dt = dataTable;
            // determine type here
            List<T> objects = new List<T>();

            if (dataTable != null)
            {
                foreach (DataRow row in dt.Rows)
                {
                    var type = typeof(T);
                    // create an object of the type
                    var obj = (T)Activator.CreateInstance(type);

                    icount++;

                    List<string> ColumnNames = new List<string>();
                    foreach (DataColumn item in row.Table.Columns)
                    {
                        string columnName = item.ColumnName;
                        ColumnNames.Add(columnName.ToLower());
                    }

                    PropertyInfo[] props = type.GetProperties();
                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.PropertyType.ToString().ToLower().Contains("string"))
                        {
                            if (ColumnNames.Where(s => s == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(obj, row[prop.Name].ToString());
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("int"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                int i = 0;
                                int.TryParse(row[prop.Name].ToString(), out i);
                                prop.SetValue(obj, i);
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("float"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(obj, float.Parse(row[prop.Name].ToString()));
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                        {
                            DateTime now = DateTime.Now;

                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                DateTime.TryParse(row[prop.Name].ToString(), out now);
                                prop.SetValue(obj, now);
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("double"))
                        {

                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                double i = 0;
                                if (row[prop.Name] != null)
                                {
                                    double.TryParse(row[prop.Name].ToString(), out i);
                                    prop.SetValue(obj, i);
                                }
                                //prop.SetValue(obj, double.Parse(row[prop.Name].ToString()));

                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                decimal i = 0;
                                if (row[prop.Name] != null)
                                {
                                    decimal.TryParse(row[prop.Name].ToString(), out i);
                                    prop.SetValue(obj, i);
                                }
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("bool") && ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            if (row[prop.Name].ToString() != "" && row[prop.Name].ToString() != null)
                            {
                                int torfint = 0;
                                int.TryParse(row[prop.Name].ToString(), out torfint);
                                bool torf = false;
                                if (torfint == 1) torf = true;
                                if (torfint == 0) torf = false;
                                if (row[prop.Name].ToString().ToLower() == "false") torf = false;
                                if (row[prop.Name].ToString().ToLower() == "true") torf = true;
                                prop.SetValue(obj, torf);
                            }
                            else
                            {
                                if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(obj, false);
                            }
                        }
                    }//
                    objects.Add(obj);
                }
                return objects;
            }
            else
            {
                return null;
            }
        }
        public static T convertDataRowtoObject<T>(DataRow row, string fieldprefix = "")
        {
            int icount = 0;
            DataRow dt = row;
            var type = typeof(T);
            // create an object of the type
            var userClass = (T)Activator.CreateInstance(type);
            //if (dt != null && userClass != null && userClass != null)
            {
                //foreach (DataRow row in dt.Rows)
                {
                    icount++;

                    List<string> ColumnNames = new List<string>();
                    foreach (DataColumn item in row.Table.Columns)
                    {
                        string columnName = item.ColumnName;
                        if (fieldprefix != "") columnName = columnName.ToLower().Replace(fieldprefix.ToLower(), "");
                        ColumnNames.Add(columnName.ToLower());
                    }

                    PropertyInfo[] props = userClass.GetType().GetProperties();
                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.PropertyType.ToString().ToLower().Contains("string"))
                        {
                            if (ColumnNames.Where(s => s == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(userClass, row[prop.Name].ToString());
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("int"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                int i = 0;
                                int.TryParse(row[prop.Name].ToString(), out i);
                                prop.SetValue(userClass, i);
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("float"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(userClass, float.Parse(row[prop.Name].ToString()));
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                        {
                            DateTime now = DateTime.Now;

                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                DateTime.TryParse(row[prop.Name].ToString(), out now);
                                prop.SetValue(userClass, now);
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("double"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(userClass, double.Parse(row[prop.Name].ToString()));
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                            {
                                decimal i = 0;
                                decimal.TryParse(row[prop.Name].ToString(), out i);
                                prop.SetValue(userClass, i);
                            }
                        }
                        if (prop.PropertyType.ToString().ToLower().Contains("bool") && ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            if (row[prop.Name].ToString() != "" && row[prop.Name].ToString() != null)
                            {
                                int torfint = 0;
                                int.TryParse(row[prop.Name].ToString(), out torfint);
                                bool torf = false;
                                if (torfint == 1) torf = true;
                                if (torfint == 0) torf = false;
                                if (row[prop.Name].ToString().ToLower() == "false") torf = false;
                                if (row[prop.Name].ToString().ToLower() == "true") torf = true;
                                prop.SetValue(userClass, torf);
                            }
                            else
                            {
                                if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(userClass, false);
                            }
                        }
                    }//
                }
                return userClass;
            }
        }
        public static T PopulateObjectFromObject<T>(object fromObject, object toObject, bool overwriteNonNulls = false)
        {
            PropertyInfo[] props = toObject.GetType().GetProperties();
            PropertyInfo[] fromProps = fromObject.GetType().GetProperties();
            foreach (var prop in props)
            {
                var item = fromProps.Where(p => p.Name.ToLower() == prop.Name.ToLower()).FirstOrDefault();
                if (!overwriteNonNulls && item != null && prop.CanWrite || overwriteNonNulls && item != null && prop.CanWrite)
                {
                    var value = item.GetValue(fromObject);
                    if (prop.PropertyType == typeof(string) && value != null) prop.SetValue(toObject, value.ToString());
                    else if (prop.PropertyType == typeof(string) && value == null) prop.SetValue(toObject, "");
                    else prop.SetValue(toObject, value);
                }
            }
            return (T)toObject;
        }
        public static List<T> SelectAll<T>()
        {
            var sql = String.Format("Select * from {0};", GetTableName<T>());
            return convertDataTabletoObject<T>(DAL.SLoad(sql));
        }
        public static List<T> SelectAll<T>(string tablename)
        {
            var sql = String.Format("Select * from {0};", tablename);
            return convertDataTabletoObject<T>(DAL.SLoad(sql));
        }
        public static T ObjectFromNVP<T>(string tablename, DAL dal, int id = 0, string keyname = null)
        {
            string sql = string.Empty;
            if (keyname != null && id != 0) sql = string.Format("select top 1 * from {0} where {2} = '{1}'", tablename, id, keyname);
            else sql = string.Format("select * from {0}", tablename);

            DataTable dt = dal.Load(sql);
            if (dt == null) return default(T);
            if (dt != null && dt.Rows.Count == 0) return default(T);
            int icount = 0;
            var instance = (T)Activator.CreateInstance(typeof(T));
            PropertyInfo[] props = typeof(T).GetProperties();
            foreach (DataRow row in dt.Rows)
            {

                var prop = props.Where(p => p.Name == row["Name"].ToString()).FirstOrDefault();
                if (prop != null)
                {
                    if (prop.PropertyType.Name.ToLower().Contains("string")) prop.SetValue(instance, row["Value"].ToString());
                    if (prop.PropertyType.Name.ToLower().Contains("integer")) prop.SetValue(instance, int.Parse(row["Value"].ToString()));
                    if (prop.PropertyType.Name.ToLower().Contains("datetime")) prop.SetValue(instance, DateTime.Parse(row["Value"].ToString()));
                }


            }
            return instance;
        }
        public static string GetTableName<T>()
        {
            // Using reflection.
            System.Attribute[] attrs = System.Attribute.GetCustomAttributes(typeof(T));  // Reflection. 
            var TableName = "";
            foreach (System.Attribute attr in attrs)
            {
                if (attr is DBTable)
                {
                    TableName = ((DBTable)attr).GetName();
                }
            }
            return TableName;
        }
        public static string InsertsFromDT(DataTable dt, string tableName)
        {

            if (dt == null) return "DT is null or empty.";
            Dictionary<string, Type> ColumnNames = new Dictionary<string, Type>();
            foreach (DataColumn item in dt.Columns)
            {
                string columnName = item.ColumnName;
                ColumnNames.Add(columnName.ToLower(), item.DataType);
            }
            var sql = string.Empty;
            foreach (DataRow row in dt.Rows)
            {

                sql += string.Format("insert into {0} (", tableName);
                foreach (var column in ColumnNames)
                {
                    sql += column.Key + ",";
                }
                var lastcomma = sql.LastIndexOf(',');
                sql = sql.Substring(0, lastcomma);
                sql += ") values (";
                foreach (var column in ColumnNames)
                {
                    var val = row[column.Key].ToString().Replace("'", "''");
                    //sql += "[" + column.Key + "|" + column.Value +"]"; //in case something is missing label the outputs
                    if (column.Value.Name == "Decimal")
                    {
                        float flt = 0;
                        float.TryParse(val, out flt);
                        sql += string.Format("{0},", flt);
                    }
                    else if (column.Value.Name == "DateTime" && val.Length > 0)
                    {
                        sql += (string.Format("'{0:s}',", DateTime.Parse(row[column.Key].ToString())));
                    }
                    else if (column.Value.Name == "Integer")
                    {
                        int flt = 0;
                        int.TryParse(val, out flt);
                        sql += string.Format("{0},", flt);
                    }
                    else if (column.Value.Name == "Float")
                    {
                        float flt = float.Parse(val);
                        float.TryParse(val, out flt);
                        sql += string.Format("{0},", flt);
                    }
                    else if (column.Value.Name == "String")
                    {
                        sql += string.Format("'{0}',", val);
                    }
                    else
                    {
                        val = row[column.Key].ToString().Replace("'", "''");
                        sql += string.Format("'{0}',", val);
                    }

                }
                lastcomma = sql.LastIndexOf(',');
                sql = sql.Substring(0, lastcomma);
                sql += "); \r\n";
            }
            return sql;
        }
        public static string GetPrimaryKeyName<T>()
        {

            PropertyInfo[] props = typeof(T).GetProperties();
            foreach (PropertyInfo prop in props)
            {
                if (prop.CustomAttributes.Where(a => a.AttributeType == typeof(DBPrimaryKey)).FirstOrDefault() != null)
                    return prop.Name;
            }
            return "";
        }
        public static string GetPrimaryKey(string tableName, DAL Dal = null)
        {
            var db = "";
            if (tableName.Contains(".dbo"))
            {
                db = tableName.Substring(0, tableName.IndexOf("."));
                db = string.Format("use {0} ;", db);
            }
            if (tableName.Contains("dbo.")) tableName = tableName.Substring(tableName.LastIndexOf(".") + 1);

            var sql = string.Format(@"SELECT column_name as primarykeycolumn
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                    INNER JOIN
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                    TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                    and ku.table_name='{0}'
                    ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;", tableName, db);
            DataTable dt = null;
            if (Dal != null) dt = Dal.Load(sql);
            else { dt = DAL.SLoad(sql); }

            if (dt != null)
            {
                if (dt.Rows.Count > 0)
                {
                    var key = dt.Rows[0][0].ToString();
                    return key;
                }
            }
            return null;
        }
        public static T GetSingle<T>(DAL dal, string tablename, int id)
        {
            ORM.dal = dal;
            var sql = string.Format("select * from {0} where {2} = '{1}'", tablename, id, GetPrimaryKeyName<T>());
            DataTable dt = null;
            if (dal == null) dt = DAL.SLoad(sql);
            else dt = dal.Load(sql);
            int icount = 0;
            Type tType = typeof(T);
            var instance = Activator.CreateInstance(tType);
            //foreach (DataRow row in dt.Rows)
            {
                icount++;
                if (dt.Rows.Count == 0) return default(T);
                DataRow row = dt.Rows[0];

                List<string> ColumnNames = new List<string>();
                foreach (DataColumn item in row.Table.Columns)
                {
                    string columnName = item.ColumnName;
                    ColumnNames.Add(columnName.ToLower());
                }

                PropertyInfo[] props = instance.GetType().GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    if (prop.PropertyType.ToString().ToLower().Contains("string"))
                    {
                        if (ColumnNames.Where(s => s == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, row[prop.Name].ToString());
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("int"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            int i = 0;
                            int.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("float"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, float.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("datetime"))
                    {
                        DateTime now = DateTime.Now;

                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            DateTime.TryParse(row[prop.Name].ToString(), out now);
                            prop.SetValue(instance, now);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("double"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, double.Parse(row[prop.Name].ToString()));
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("decimal"))
                    {
                        if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                        {
                            decimal i = 0;
                            decimal.TryParse(row[prop.Name].ToString(), out i);
                            prop.SetValue(instance, i);
                        }
                    }
                    if (prop.PropertyType.ToString().ToLower().Contains("bool") && ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null)
                    {
                        if (row[prop.Name].ToString() != "" && row[prop.Name].ToString() != null)
                        {
                            int torfint = 0;
                            int.TryParse(row[prop.Name].ToString(), out torfint);
                            bool torf = false;
                            if (torfint == 1) torf = true;
                            if (torfint == 0) torf = false;
                            if (row[prop.Name].ToString().ToLower() == "false") torf = false;
                            if (row[prop.Name].ToString().ToLower() == "true") torf = true;
                            prop.SetValue(instance, torf);
                        }
                        else
                        {
                            if (ColumnNames.Where(s => s.ToLower() == prop.Name.ToLower()).FirstOrDefault() != null) prop.SetValue(instance, false);
                        }
                    }
                }//
            }
            return (T)instance;
        }
        public static string InsertOrUpdate<T>(T userObj, string TableName, string whereClause, string PrimaryKey)
        { //where appid = @key and code = @code
            if (whereClause.Length > 0)
            {
                var insertSQL = Insert(userObj, TableName);
                var updateSQL = Update(userObj, TableName, PrimaryKey);
                var sql = string.Format(@"
                begin tran
                if exists (select * from {0} with (updlock,serializable) {3})
                begin
                    {1}
                end
                else
                begin
                   {2}
                end
                commit tran", TableName, updateSQL, insertSQL, whereClause);
                return sql;
            }
            return "";
        }
        public static string generateCreateSQL<T>(string tablename = "", string fieldprefix = "")
        {
            string returnSQL = string.Empty;
            Type tType = typeof(T);
            Attribute[] attrs = Attribute.GetCustomAttributes(tType);  // Reflection. 
            if (tablename == string.Empty)
            {
                var attr = attrs.Where(a => a is DBTable).FirstOrDefault();
                tablename = ((DBTable)attr).GetName();
            }
            returnSQL += (string.Format("CREATE TABLE {0} ( ", tablename));
            int icount = 0;
            string comma = "";
            foreach (var prop in tType.GetProperties())
            {
                Attribute[] pattrs = Attribute.GetCustomAttributes(prop);  // Reflection. 
                Attribute pk = null;
                if (pattrs.Count() > 0)
                {
                    pk = attrs.Where(a => a is DBTable).FirstOrDefault();
                }
                icount++;
                if (icount < tType.GetProperties().Count()) comma = ",\r\n";
                else comma = "";
                if (prop.PropertyType.ToString() == "System.Char")
                {
                    string sqlvartype = "Varchar(250)";
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.String")
                {
                    string sqlvartype = "varchar(250)";
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.Int64")
                {
                    string sqlvartype = "[int]";
                    sqlvartype = (pk != null) ? "[int] IDENTITY(1,1) PRIMARY KEY" : sqlvartype;
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.Int32")
                {
                    string sqlvartype = "[int]";
                    sqlvartype = (pk != null) ? "[int] IDENTITY(1,1) PRIMARY KEY" : sqlvartype;
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.Single")//float
                {
                    string sqlvartype = "[float]";
                    sqlvartype = (pk != null) ? "[float] IDENTITY(1,1) PRIMARY KEY" : sqlvartype;
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.Boolean")//float
                {
                    string sqlvartype = "[bit]";
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
                else if (prop.PropertyType.ToString() == "System.Drawing.Image")//float
                {
                    string sqlvartype = "varchar(250)";
                    returnSQL += (string.Format("[{1}{2}] {3}{0}", comma, fieldprefix, prop.Name, sqlvartype));
                }
            }
            returnSQL += (string.Format(");", tablename));
            return returnSQL;
        }
        public static string Delete<T>(T userObj)
        {
            var tablename = GetTableName<T>();
            var sql = string.Format("delete from {0} where {1} = {2}", tablename, GetPrimaryKey(tablename), GetPrimaryKeyValue(userObj));
            return sql;
        }
        public static int GetPrimaryKeyValue<T>(T obj)
        {
            PropertyInfo[] props = obj.GetType().GetProperties();
            foreach (PropertyInfo prop in props)
            {
                if (prop.CustomAttributes.Where(a => a.AttributeType == typeof(DBPrimaryKey)).FirstOrDefault() != null)
                    return int.Parse(prop.GetValue(obj).ToString());
            }
            return 0;
        }
    }
}
