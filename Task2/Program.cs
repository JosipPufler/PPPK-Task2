using Npgsql;

const string CONNECTION_STRING = "Host=dowdily-glamorous-malamute.data-1.use1.tembo.io;Port=5432;Username=postgres;Password=o26aDiEouQnd6CDS;Database=postgres";
const string exitString = "exit";


using (var connection = new NpgsqlConnection(CONNECTION_STRING))
{
    try
    {
        connection.Open();
        Console.WriteLine("Connection open");


    }
    catch (Exception ex)
    {
        Console.WriteLine("Connection failed. " + ex.Message);
        throw;
    }

    string prompt;
    do
    {
        Console.Write("\n>");
        prompt = Console.ReadLine();
        Console.WriteLine();
        if (prompt.ToLower() == exitString)
        {
            break;
        }

        try
        {
            var command = new NpgsqlCommand(prompt, connection);

            if (prompt != null && prompt.ToLower().StartsWith("select *"))
            {
                string tableName = prompt.ToLower().Substring(prompt.ToLower().IndexOf("from") + 5).Trim();
                tableName = tableName.Split(' ')[0];

                IList<KeyValuePair<string, string>> columns = getColumnNames(tableName, connection);

                printTable(command, columns.Select(x => x.Key).ToList());
            }
            else if (prompt != null && prompt.ToLower().StartsWith("select")) {
                string tableName = prompt.ToLower().Substring(prompt.ToLower().IndexOf("from") + 5).Trim();
                tableName = tableName.Split(' ')[0];

                string[] columns = prompt.ToLower().Substring(prompt.ToLower().IndexOf("select") + 6).Trim().Split(",").Select(x => x.Trim()).ToArray();

                columns[columns.Length - 1] = columns.Last().Split(' ')[0].Trim();
                printTable(command, columns);
            }
            else if (prompt != null && prompt.StartsWith(@"\t ")) {
                string tableName = prompt.Split(" ")[1];

                IList<string> constraints = getConstraints(tableName, connection);
                Console.WriteLine("Constraints:");
                if (constraints.Count == 0)
                {
                    Console.WriteLine("None");
                }
                else
                {
                    foreach (var constraint in constraints)
                    {
                        Console.WriteLine("\t" + constraint + ", ");
                    }
                }

                Console.WriteLine("\n");

                IList<KeyValuePair<string, string>> columns = getColumnNames(tableName, connection);
                Console.WriteLine("Columns:");
                foreach (KeyValuePair<string, string> column in columns)
                {
                    Console.WriteLine("\t" + column.Key + $" ({column.Value}), ");
                }

                Console.WriteLine();

                printTable(new NpgsqlCommand($"select * from {tableName}", connection), columns.Select(x => x.Key).ToList());
            }
            else if (prompt != null) {
                using (var reader = command.ExecuteReader())
                {
                    Console.WriteLine("done");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error!");
            Console.WriteLine(ex.Message);
            throw;
        }    
    } while (prompt.ToLower() != exitString);
}

static IList<KeyValuePair<string, string>> getColumnNames(string tableName, NpgsqlConnection connection) {
    string tableColumnQuery = $@"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tableName}';";
    var command = new NpgsqlCommand(tableColumnQuery, connection);
    List<KeyValuePair<string, string>> columns = new();
    
    using (var reader = command.ExecuteReader())
    {
        while (reader.Read())
        {
            columns.Add(new KeyValuePair<string, string>(reader["column_name"].ToString(), reader["data_type"].ToString()));
        }
    }
    return columns;
}

static IList<string> getConstraints(string tableName, NpgsqlConnection connection) {
    string constraintsQuery = $@"SELECT con.* FROM pg_catalog.pg_constraint con 
        INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid 
        INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace WHERE rel.relname = '{tableName}';";
    var command = new NpgsqlCommand(constraintsQuery, connection);
    List<string> constraints = new();

    using (var reader = command.ExecuteReader())
    {
        while (reader.Read())
        {
            constraints.Add(reader["conname"].ToString());
        }
    }
    return constraints;
}

static void printTable(NpgsqlCommand command, IList<string> columns)
{
    List<string>[] dataTable = new List<string>[columns.Count];
    int rows = 0;
    int[] dataLength = new int[columns.Count];

    for (int i = 0; i < dataTable.Length; i++)
    {
        dataTable[i] = new();
        dataLength[i] = columns[i].Length;
    }

    using (var reader = command.ExecuteReader())
    {
        while (reader.Read())
        {
            rows++;
            
            for (int i = 0; i < columns.Count; i++)
            {
                dataTable[i].Add(reader[columns.ElementAt(i)].ToString().Trim());
                if (reader[columns.ElementAt(i)].ToString().Trim().Length > dataLength[i])
                {
                    dataLength[i] = reader[columns.ElementAt(i)].ToString().Trim().Length;
                }
            }
        }
    }

    for(int i = 0; i < columns.Count; i++)
    {
        if (i != 0)
        {
            Console.Write(" ");
        }
        Console.Write($"{{0, -{dataLength[i] + 1}}}|", columns[i]);
    }

    Console.WriteLine();
    
    for (int i = 0; i < columns.Count; i++)
    {
        if (i != 0)
        {
            Console.Write("-");
        }
        for (int j = 0; j < dataLength[i] + 1; j++)
        {
            Console.Write("-");
        }
        Console.Write("+");
    }
    
    for (int i = 0; i < rows; i++)
    {
        Console.WriteLine();
        for (int j = 0; j < columns.Count; j++)
        {
            if (j != 0)
            {
                Console.Write(" ");
            }
            Console.Write($"{{0, -{dataLength[j] + 1}}}|", dataTable[j].ElementAt(i));
        }
    }
    Console.WriteLine();

}