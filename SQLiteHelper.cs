// needs reference to: System.Data.SQLite 1.0.103
// Compiled with .Net 4.5.2 
// tested on x86

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;

namespace Itsho.DataSetToSQLite
{
    public static class SQLiteHelper
    {
        public static void ConvertDataSetToSQLite(DataSet p_dataSet, string p_strOutputFullPath)
        {
            try
            {
                var con = new SQLiteConnection(@"data source=" + p_strOutputFullPath);
                con.Open();

                //CREATE Tables with PK and FK
                foreach (DataTable dtTable in p_dataSet.Tables)
                {
                    var strSqlCreateTable = CreateSqliteTable(dtTable);

                    using (var cmd = new SQLiteCommand(strSqlCreateTable, con))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // Fill data
                foreach (DataTable dtTable in p_dataSet.Tables)
                {
                    foreach (DataRow row in dtTable.Rows)
                    {
                        var strInsert = "INSERT INTO " + dtTable.TableName + " VALUES (";
                        for (int intColumn = 0; intColumn < dtTable.Columns.Count; intColumn++)
                        {
                            strInsert += "@" + dtTable.Columns[intColumn].ColumnName + ",";
                        }
                        strInsert = strInsert.TrimEnd(',') + ")";

                        using (var cmdInsertRow = new SQLiteCommand(strInsert, con))
                        {
                            for (int intColumn = 0; intColumn < dtTable.Columns.Count; intColumn++)
                            {
                                cmdInsertRow.Parameters.AddWithValue(dtTable.Columns[intColumn].ColumnName, row[intColumn]);
                            }
                            cmdInsertRow.ExecuteNonQuery();
                        }
                    }
                }
                con.Close();
            }
            catch (Exception ex)
            {
                //Logger.Log.Error(ex);
            }
        }

        public static string CreateSqliteTable(DataTable table)
        {
             const string NEW_LINE_IN_TABLE_DEF = ",\n";
						
            // NOTE:, in SQLite type is recommended, not required. Any column can still store any type of data
            // https://www.sqlite.org/datatype3.html
            var typesToSqliteDictionary = new Dictionary<Type, string>();
            typesToSqliteDictionary.Add(typeof(System.Int16), "INTEGER");
            typesToSqliteDictionary.Add(typeof(System.Int32), "INTEGER");
            typesToSqliteDictionary.Add(typeof(System.Int64), "INTEGER");
            typesToSqliteDictionary.Add(typeof(System.Byte), "INTEGER");
            typesToSqliteDictionary.Add(typeof(System.Boolean), "INTEGER");
            typesToSqliteDictionary.Add(typeof(System.Decimal), "NUMERIC");
            typesToSqliteDictionary.Add(typeof(float), "REAL");
            typesToSqliteDictionary.Add(typeof(System.Double), "REAL");
            typesToSqliteDictionary.Add(typeof(System.String), "NVARCHAR");
            typesToSqliteDictionary.Add(typeof(System.DateTime), "TEXT"); // TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").

            var strSqlCreateTable = "CREATE TABLE " + table.TableName + "(";
            

            #region Columns definition

            for (int intColToGetDef = 0; intColToGetDef < table.Columns.Count; intColToGetDef++)
            {
                #region Column definition

                strSqlCreateTable += " [" + table.Columns[intColToGetDef].ColumnName + "] ";

                if (table.Columns[intColToGetDef].DataType == typeof(System.String))
                {
                    if (table.Columns[intColToGetDef].MaxLength == -1)
                    {
                        strSqlCreateTable += " TEXT ";
                    }
                    else
                    {
                        strSqlCreateTable += $" NVARCHAR({table.Columns[intColToGetDef].MaxLength}) ";
                    }
                }
                else
                {
                    strSqlCreateTable += " " + typesToSqliteDictionary[table.Columns[intColToGetDef].DataType] + " ";
                }

                // if this column is the only primary key
                if (table.PrimaryKey.Length == 1 && table.PrimaryKey.Contains(table.Columns[intColToGetDef]))
                {
                    strSqlCreateTable += " PRIMARY KEY ";

                    // using AUTOINCREMENT explicitly without specifiyng PRIMARY KEY,
                    // will throw error - "near "AUTOINCREMENT": syntax error"
                    // have fun - http://stackoverflow.com/a/6157337/426315
                    if (table.Columns[intColToGetDef].AutoIncrement)
                    {
                        //strSqlCreateTable += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                        strSqlCreateTable += " AUTOINCREMENT ";
                    }
                }

                if (table.Columns[intColToGetDef].Unique)
                {
                    strSqlCreateTable += " UNIQUE ";
                }

                if (!table.Columns[intColToGetDef].AllowDBNull)
                {
                    strSqlCreateTable += " NOT NULL ";
                }

                #endregion Column definition

                // add newline
                strSqlCreateTable += NEW_LINE_IN_TABLE_DEF;
            }

            #endregion Columns definition

            #region Relations to FK

            for (int intColToGetRelation = 0; intColToGetRelation < table.Columns.Count; intColToGetRelation++)
            {
                // if this column needs FK
                var lstRelations =
                    table.ParentRelations.Cast<DataRelation>()
                        .Where(r => r.ChildColumns.Contains(table.Columns[intColToGetRelation]))
                        .ToList();

                if (lstRelations.Any())
                {
                    foreach (var relation in lstRelations)
                    {
                        // row for Example: FOREIGN KEY(trackartist) REFERENCES artist(artistid)
                        // https://www.sqlite.org/foreignkeys.html
                        strSqlCreateTable += " FOREIGN KEY(" + table.Columns[intColToGetRelation].ColumnName + ") REFERENCES " +
                                             relation.ParentTable.TableName + "(" +
                                             string.Join(",", relation.ParentColumns.Select(pc => pc.ColumnName)) +
                                             ")" + NEW_LINE_IN_TABLE_DEF;
                    }
                }
            }

            #endregion Relations to FK

            #region Multiple PrimaryKeys

            // if there are multiple primary keys
            if (table.PrimaryKey.Length > 1)
            {
                strSqlCreateTable += " PRIMARY KEY (" + string.Join(",", table.PrimaryKey.Select(pk => pk.ColumnName)) + ")" + NEW_LINE_IN_TABLE_DEF;
            }

            #endregion Multiple PrimaryKeys

            // remove last comma
            strSqlCreateTable = strSqlCreateTable.TrimEnd(NEW_LINE_IN_TABLE_DEF.ToCharArray()) + ")";

            return strSqlCreateTable;
        }
    }
}
