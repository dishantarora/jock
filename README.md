using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text;

namespace CSharpLearning
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Program p = new Program();
            var connectionString = "";
            var filePath = "";
            string primaryKey = "";
            string tableName = "";
            var datetimeColumns = new List<string>();
            var columns = new List<string>();
            var fileWriter = new StreamWriter(filePath);
            var header = p.GetCsvHeader(columns);
            fileWriter.WriteLine(header);
            var countQuery = p.GetCountQuery(tableName);
            var selectQuery = p.GetSelectQuery(columns, primaryKey, tableName);            
            using var connection = new SqlConnection(connectionString);
            connection.Open();
            var countCommand = new SqlCommand(countQuery, connection);
            double count = (double)countCommand.ExecuteNonQuery();
            int range = p.SetRange(count);
            var group = Math.Ceiling(count / range);
            var start = 0;
            var end = range;
            for(int i = 0; i < group; i++)
            {
                var selectCommand = new SqlCommand(selectQuery, connection);
                selectCommand.Parameters.AddWithValue("@startRow", start);
                selectCommand.Parameters.AddWithValue("@endRow", end);
                using var reader = selectCommand.ExecuteReader();
                var csvBatch = new StringBuilder();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var row = p.GetColumnValues(reader);
                        var columnsToBeDateTimeFormatted = p.GetIndexOfDateTimeColumns(reader, datetimeColumns);
                        p.FormatDateTimeColumns(row, columnsToBeDateTimeFormatted);
                        var line = String.Join(",", row);
                        csvBatch.AppendLine(line);
                    }
                }
                fileWriter.WriteLine(csvBatch);
                start = end + 1;
                if ((start + range - 1) > count)
                {
                    end = (int)count;
                }
                else
                {
                    end = start + range - 1;
                }
            }
            fileWriter.Close();
        }
        public string GetSelectQuery(List<string> columns, string primaryKey, string tableName)
        {
            return "SELECT " + string.Join(",", columns) + " FROM (" +
                "SELECT " + string.Join(",", columns) + ", ROW_NUMBER() OVER(ORDER BY " + primaryKey + ") AS RowNum" +
                "FROM " + tableName + "" +
                ") sub" +
                "WHERE sub.RowNum BETWEEN @startRow AND @endRow";
        }
        public string GetCountQuery(string tableName)
        {
            return "SELECT COUNT(*) FROM " + tableName;
        }
        public string GetCsvHeader(List<string> columns)
        {
            return string.Join(",", columns);
        }
        public void FormatDateTimeColumns(List<string> row, List<int> columnsToBeDateTimeFormatted)
        {
            foreach (var index in columnsToBeDateTimeFormatted)
            {
                row[index] = DateTime.ParseExact(row[index], "yy/MM/dd h:mm:ss ttyy/MM/dd h:mm:ss tt", CultureInfo.InvariantCulture).ToLongDateString();
            }
        }
        public List<int> GetIndexOfDateTimeColumns(SqlDataReader reader, List<string> datetimeColumns)
        {
            var columnsToBeDateTimeFormatted = new List<int>();
            if (datetimeColumns.Count != 0)
            {
                foreach (var column in datetimeColumns)
                {
                    columnsToBeDateTimeFormatted.Add(reader.GetOrdinal(column));
                }
            }
            return columnsToBeDateTimeFormatted;
        }
        public List<string> GetColumnValues(SqlDataReader reader)
        {
            var row = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row.Add(reader.GetString(i));
            }
            return row;
        }
        public int SetRange(double count)
        {
            if (count > 2000000)
            {
                return (int)Math.Ceiling(count / 100000);
            }
            else if (count > 50000 && count < 2000000)
            {
                return (int)Math.Ceiling(count / 50000);
            }
            else
            {
                return (int)Math.Ceiling(count / 1);
            }
        }
    }
}
