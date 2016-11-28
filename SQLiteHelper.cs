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
        private const string NEW_LINE_IN_TABLE_DEF = ",\n";

        public static void CreateSQLiteFromDataSet(DataSet p_dataSetSource, string p_strSQLiteFullPathTarget)
        {
            try
            {
                using (var conn = new SQLiteConnection(@"data source=" + p_strSQLiteFullPathTarget))
                {
                    conn.Open();

                    //CREATE Tables with PK and FK
                    foreach (DataTable dtTable in p_dataSetSource.Tables)
                    {
                        var strSqlCreateTable = GetSQLiteCreateTableIfNotExists(dtTable);

                        using (var cmd = new SQLiteCommand(strSqlCreateTable, conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // without transaction, each command will commit, and therefore, the save process will be very slow.
                    // that's why we use transaction.
                    var transaction = conn.BeginTransaction();

                    // Fill data
                    foreach (DataTable dtTableSource in p_dataSetSource.Tables)
                    {
                        FillSQLiteSingleTableFromDataTable(dtTableSource, conn, transaction);
                    }

                    transaction.Commit();
                    conn.Close();
                }
            }
            catch (Exception ex)
            {
                //Logger.Log.Error(ex);
            }
        }

        private static void FillSQLiteSingleTableFromDataTable(DataTable p_dtTableSource, SQLiteConnection p_connectionToTarget, SQLiteTransaction p_transaction)
        {
            #region Manualy create INSERT command - not in use since SQLiteCommandBuilder is creating the INSERT command

            /* foreach (DataRow row in dtTable.Rows)
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
            }*/

            #endregion Manualy create INSERT command - not in use since SQLiteCommandBuilder is creating the INSERT command

            #region Truncate all data from table

            using (var dbCommand = p_connectionToTarget.CreateCommand())
            {
                dbCommand.Transaction = p_transaction;

                // SQLite got TRUNCATE optimizer
                // https://www.techonthenet.com/sqlite/truncate.php
                dbCommand.CommandText = "DELETE FROM " + p_dtTableSource.TableName;

                dbCommand.ExecuteNonQuery();
            }

            #endregion Truncate all data from table

            #region Fill SQLite Table from DataTable

            using (var dbCommand = p_connectionToTarget.CreateCommand())
            {
                dbCommand.Transaction = p_transaction;
                dbCommand.CommandText = "SELECT * FROM [" + p_dtTableSource.TableName + "]";
                using (var sqliteAdapterToTarget = new SQLiteDataAdapter(dbCommand))
                {
                    // Need this line (without it Update line would fail)
                    //https://www.devart.com/dotconnect/sqlite/docs/Devart.Data.SQLite~Devart.Data.SQLite.SQLiteCommandBuilder.html
                    using (new SQLiteCommandBuilder(sqliteAdapterToTarget))
                    {
                        Stopwatch sw = new Stopwatch();
                        sw.Start();

                        int intTotalRowsUpdatedFromSource = sqliteAdapterToTarget.Update(p_dtTableSource);
                        sw.Stop();
                        Logger.Log.DebugFormat(
                            "Finished update table - <TableName = {0}> <TotalRowsUpdated = {1}> <ElapsedTime = {2}>",
                            p_dtTableSource.TableName,
                            intTotalRowsUpdatedFromSource,
                            sw.Elapsed);
                    }
                }
            }

            #endregion Fill SQLite Table from DataTable
        }

        /// <summary>
        /// Send new instance of StronglyTyped Dataset
        /// returns same instance filled with data
        /// </summary>
        /// <param name="p_dataSetStronglyTypedTarget"></param>
        /// <param name="p_strDBFullPathSource"></param>
        /// <returns></returns>
        public static bool FillDataSetFromSQLite(string p_strDBFullPathSource, DataSet p_dataSetStronglyTypedTarget)
        {
            try
            {
                if (p_dataSetStronglyTypedTarget == null)
                {
                    Logger.Log.Error("Parameter " + nameof(p_dataSetStronglyTypedTarget) + " should not be null");
                    throw new Exception("Parameter " + nameof(p_dataSetStronglyTypedTarget) + " should not be null");
                }

                if (!File.Exists(p_strDBFullPathSource))
                {
                    Logger.Log.Error(string.Format("File '{0}' not exists", p_strDBFullPathSource));
                    return false;
                }

                using (var con = new SQLiteConnection(@"data source=" + p_strDBFullPathSource))
                {
                    con.Open();

                    foreach (DataTable dataTable in p_dataSetStronglyTypedTarget.Tables)
                    {
                        using (var adapter = new SQLiteDataAdapter("SELECT * FROM " + dataTable.TableName, con))
                        {
                            adapter.FillLoadOption = LoadOption.Upsert;
                            adapter.Fill(dataTable);
                        }
                    }

                    con.Close();
                }
                return true;
            }
            catch (Exception ex)
            {
                Logger.Log.Fatal(ex);
                return false;
            }
        }

        public static bool SaveAllDataToDB(DataSet p_dataSetSource, string p_strDBFullPathTarget)
        {
            // if file exists - we will erase it, since the whole flow is to re-create everything
            // to create logic that dynamically UPDATES the data from DataSet is too complicated for now...
            if (File.Exists(p_strDBFullPathTarget))
            {
                File.Delete(p_strDBFullPathTarget);
            }

            // connection will create new file if needed
            using (var conn = new SQLiteConnection(@"data source=" + p_strDBFullPathTarget))
            {
                conn.Open();

                // without transaction, each command will commit, and therefore, the save process will be very slow.
                // that's why we use transaction.
                var transaction = conn.BeginTransaction();

                foreach (DataTable dataTableSource in p_dataSetSource.Tables)
                {
                    // create table if not exists
                    var strSqlCreateTable = GetSQLiteCreateTableIfNotExists(dataTableSource);
                    using (var cmdCreateTable = new SQLiteCommand(strSqlCreateTable, conn))
                    {
                        cmdCreateTable.ExecuteNonQuery();
                    }

                    // fill all data (auto-generate INSERT command for each row)
                    FillSQLiteSingleTableFromDataTable(dataTableSource, conn, transaction);
                }

                transaction.Commit();

                conn.Close();
            }
            return true;
        }

        public static string GetSQLiteCreateTableIfNotExists(DataTable p_dtTableSource)
        {
            // NOTE: in SQLite type is recommended, not required. Any column can still store any type of data!
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

            var strSqlCreateTable = "CREATE TABLE IF NOT EXISTS " + p_dtTableSource.TableName + "(";

            #region Columns definition

            for (int intColToGetDef = 0; intColToGetDef < p_dtTableSource.Columns.Count; intColToGetDef++)
            {
                #region Column definition

                strSqlCreateTable += " [" + p_dtTableSource.Columns[intColToGetDef].ColumnName + "] ";

                if (p_dtTableSource.Columns[intColToGetDef].DataType == typeof(System.String))
                {
                    if (p_dtTableSource.Columns[intColToGetDef].MaxLength == -1)
                    {
                        strSqlCreateTable += " TEXT ";
                    }
                    else
                    {
                        strSqlCreateTable += $" NVARCHAR({p_dtTableSource.Columns[intColToGetDef].MaxLength}) ";
                    }
                }
                else
                {
                    strSqlCreateTable += " " + typesToSqliteDictionary[p_dtTableSource.Columns[intColToGetDef].DataType] + " ";
                }

                // if this column is the only primary key
                // multiple primary key will be handled later
                if (p_dtTableSource.PrimaryKey.Length == 1 && p_dtTableSource.PrimaryKey.Contains(p_dtTableSource.Columns[intColToGetDef]))
                {
                    strSqlCreateTable += " PRIMARY KEY ";

                    // using AUTOINCREMENT explicitly without specifiyng PRIMARY KEY,
                    // will throw error - "near "AUTOINCREMENT": syntax error"
                    // have fun - http://stackoverflow.com/a/6157337/426315
                    if (p_dtTableSource.Columns[intColToGetDef].AutoIncrement)
                    {
                        //strSqlCreateTable += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                        strSqlCreateTable += " AUTOINCREMENT ";
                    }
                }

                if (p_dtTableSource.Columns[intColToGetDef].Unique)
                {
                    strSqlCreateTable += " UNIQUE ";
                }

                if (!p_dtTableSource.Columns[intColToGetDef].AllowDBNull)
                {
                    strSqlCreateTable += " NOT NULL ";
                }

                #endregion Column definition

                // add newline
                strSqlCreateTable += NEW_LINE_IN_TABLE_DEF;
            }

            #endregion Columns definition

            #region Relations to FK

            for (int intColToGetRelation = 0; intColToGetRelation < p_dtTableSource.Columns.Count; intColToGetRelation++)
            {
                // if this column needs FK
                var lstRelations =
                    p_dtTableSource.ParentRelations.Cast<DataRelation>()
                        .Where(r => r.ChildColumns.Contains(p_dtTableSource.Columns[intColToGetRelation]))
                        .ToList();

                if (lstRelations.Any())
                {
                    foreach (var relation in lstRelations)
                    {
                        // row for Example: FOREIGN KEY(trackartist) REFERENCES artist(artistid)
                        // https://www.sqlite.org/foreignkeys.html
                        strSqlCreateTable +=
                            string.Format(" FOREIGN KEY({0}) REFERENCES {1}({2}){3}",
                                            p_dtTableSource.Columns[intColToGetRelation].ColumnName,
                                            relation.ParentTable.TableName,
                                            string.Join(",", relation.ParentColumns.Select(pc => pc.ColumnName)),
                                            NEW_LINE_IN_TABLE_DEF);
                    }
                }
            }

            #endregion Relations to FK

            #region Multiple PrimaryKeys

            // if there are multiple primary keys
            if (p_dtTableSource.PrimaryKey.Length > 1)
            {
                strSqlCreateTable +=
                    string.Format(" PRIMARY KEY ({0}){1}",
                                    string.Join(",", p_dtTableSource.PrimaryKey.Select(pk => pk.ColumnName)),
                                    NEW_LINE_IN_TABLE_DEF);
            }

            #endregion Multiple PrimaryKeys

            // remove last comma
            strSqlCreateTable = strSqlCreateTable.TrimEnd(NEW_LINE_IN_TABLE_DEF.ToCharArray()) + ")";

            return strSqlCreateTable;
        }
    }
}
