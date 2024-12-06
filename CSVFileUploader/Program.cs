//using System;
//using System.Collections.Generic;
//using System.Globalization;
//using System.IO;
//using System.Linq;
//using CsvHelper;
//using CsvHelper.Configuration;
//using Microsoft.Extensions.Configuration;
//using MySql.Data.MySqlClient;


//    public class OpenAlexRecord
//    {
//        public Guid Id { get; set; }
//        public int? Unnamed_0 { get; set; }
//        public string? OpenAlex_id { get; set; }
//        public string? Name { get; set; }
//        public int? total_no_publications { get; set; }
//        public int? no_publications_first_author { get; set; }
//        public string? orcid { get; set; }
//        public List<string>? publications_list { get; set; }
//        public List<double>? vec { get; set; }

//        public List<TopTerms>? top_terms { get; set; }

//        public List<TpConnection>? Tp_connections { get; set; }

//    }
//    // Define a model for TpConnection
//    public class TpConnection
//    {
//        public Guid OpenAlexRecordId { get; set; }
//        public int SerialNo { get; set; }
//        public string? Url { get; set; } // Represents the URL
//        public double? Weight { get; set; } // Represents the second value
//        public string? Topic { get; set; } // Represents the third value
//    }
//    public class TopTerms
//    {
//        public Guid OpenAlexRecordId { get; set; }
//        public string? key { get; set; }
//        public double? Weight { get; set; }
//    }

//public class Program
//{
//    public static IConfiguration Configuration { get; private set; }

//    public static void Main(string[] args)
//    {
//        try
//        {
//            // Load configuration
//            // Load configuration
//            var builder = new ConfigurationBuilder()
//                .SetBasePath(Directory.GetCurrentDirectory())
//                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true); // Ensure optional is false to enforce loading
//            Configuration = builder.Build();

//            // Retrieve connection string
//            var connectionString = Configuration.GetConnectionString("DefaultConnection"); if (string.IsNullOrEmpty(connectionString))
//            {
//                Console.WriteLine("Recorrect connection string again please");
//            }
//                // Input CSV path
//           var inputCsvPath = "complete_data.csv";

//            // Step 1: Read CSV data into a list of records
//            var records = ReadCsv(inputCsvPath);
//            Console.WriteLine("show capacity "+records.Capacity);
//            // Step 2: Insert data into MySQL
//            InsertDataIntoMySql(records, connectionString);

//            Console.WriteLine("Data inserted into MySQL successfully!");
//        }
//        catch (Exception ex)
//        {
//            Console.WriteLine($"An error occurred: {ex.Message}");
//        }
//    }

//    /// <summary>
//    /// Reads CSV file and maps it to a list of OpenAlexRecord objects.
//    /// </summary>
//    public static List<OpenAlexRecord> ReadCsv(string inputCsvPath)
//    {
//        var records = new List<OpenAlexRecord>();

//        using (var reader = new StreamReader(inputCsvPath))
//        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
//        {
//            csv.Read();
//            csv.ReadHeader(); // Skip the header row

//            while (csv.Read())
//            {
//                var record = new OpenAlexRecord
//                {
//                    Id = Guid.NewGuid(), // Generate a unique ID for each record
//                    Unnamed_0 = csv.GetField<int>("Unnamed: 0"),
//                    OpenAlex_id = csv.GetField<string>("OpenAlex_id"),
//                    Name = csv.GetField<string>("Name"),
//                    total_no_publications = csv.GetField<int>("total_no_publications"),
//                    no_publications_first_author = csv.GetField<int>("no_publications_first_author"),
//                    orcid = csv.GetField<string>("orcid"),
//                    publications_list = ParseList(csv.GetField<string>("publications_list")),
//                    vec = ParseVec(csv.GetField<string>("vec")),
//                    top_terms = ParseList(csv.GetField<string>("top_terms")),
//                    Tp_connections = ParseList(csv.GetField<string>("Tp_connections"))
//                };
//                records.Add(record);
//            }
//        }

//        return records;
//    }

//    /// <summary>
//    /// Inserts a list of OpenAlexRecord objects into a MySQL database.
//    /// </summary>
//    public static void InsertDataIntoMySql(List<OpenAlexRecord> records, string connectionString)
//    {
//        using (var connection = new MySqlConnection(connectionString))
//        {
//            connection.Open();

//            foreach (var record in records)
//            {
//                var query = @"
//                    INSERT INTO openalexrecords 
//                    (Id, Unnamed_0, OpenAlex_id, Name, total_no_publications, no_publications_first_author, orcid, 
//                     publications_list, vec, top_terms, Tp_connections)
//                    VALUES 
//                    (@Id, @Unnamed_0, @OpenAlex_id, @Name, @total_no_publications, @no_publications_first_author, @orcid, 
//                     @PublicationsList, @Vec, @TopTerms, @TpConnections)";

//                using (var command = new MySqlCommand(query, connection))
//                {
//                    command.Parameters.AddWithValue("@Id", record.Id);
//                    command.Parameters.AddWithValue("@Unnamed_0", record.Unnamed_0);
//                    command.Parameters.AddWithValue("@OpenAlex_id", record.OpenAlex_id);
//                    command.Parameters.AddWithValue("@Name", record.Name);
//                    command.Parameters.AddWithValue("@total_no_publications", record.total_no_publications);
//                    command.Parameters.AddWithValue("@no_publications_first_author", record.no_publications_first_author);
//                    command.Parameters.AddWithValue("@orcid", record.orcid);
//                    command.Parameters.AddWithValue("@PublicationsList", string.Join(",", record.publications_list));
//                    command.Parameters.AddWithValue("@Vec", string.Join(",", record.vec));
//                    command.Parameters.AddWithValue("@TopTerms", string.Join(",", record.top_terms));
//                    command.Parameters.AddWithValue("@TpConnections", string.Join(",", record.Tp_connections));

//                    command.ExecuteNonQuery();
//                }
//            }
//        }
//    }

//    /// <summary>
//    /// Parses a comma-separated string into a list of strings.
//    /// </summary>
//    public static List<string> ParseList(string input)
//    {
//        if (string.IsNullOrEmpty(input))
//            return new List<string>();

//        return input
//            .Trim('[', ']')
//            .Split(',')
//            .Select(x => x.Trim().Trim('\'')) // Remove surrounding spaces and quotes
//            .ToList();
//    }

//    /// <summary>
//    /// Parses a comma-separated string into a list of doubles.
//    /// </summary>
//    public static List<double> ParseVec(string input)
//    {
//        if (string.IsNullOrEmpty(input))
//            return new List<double>();

//        return input
//            .Trim('[', ']')
//            .Split(',')
//            .Select(x => double.TryParse(x.Trim(), out var num) ? num : 0)
//            .ToList();
//    }
//}

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
public class ViewOpenAlexRecordFromCSV
{
    public Guid Id { get; set; }
    public int? Unnamed_0 { get; set; }
    public string? OpenAlex_id { get; set; }
    public string? Name { get; set; }
    public int? total_no_publications { get; set; }
    public int? no_publications_first_author { get; set; }
    public string? orcid { get; set; }
    public List<string>? publications_list { get; set; }
    public List<double>? vec { get; set; }
    public List<string>? top_terms { get; set; }
    public List<string>? Tp_connections { get; set; }
}
public class OpenAlexRecord
{
    public Guid Id { get; set; }
    public int? Unnamed_0 { get; set; }
    public string? OpenAlex_id { get; set; }
    public string? Name { get; set; }
    public int? total_no_publications { get; set; }
    public int? no_publications_first_author { get; set; }
    public string? orcid { get; set; }
    public List<string>? publications_list { get; set; }
    public List<double>? vec { get; set; }
    public List<TopTerms>? top_terms { get; set; }
    public List<TpConnection>? Tp_connections { get; set; }
}

public class TpConnection
{
    public Guid Id { get; set; }    
    public Guid OpenAlexRecordId { get; set; }
    public int SerialNo { get; set; }
    public string? Url { get; set; }
    public double? Weight { get; set; }
    public string? Topic { get; set; }
}

public class TopTerms
{
    public Guid Id { get; set; }    
    public Guid OpenAlexRecordId { get; set; }
    public string? Key { get; set; }
    public double? Weight { get; set; }
}

public class Program
{
    public static IConfiguration Configuration { get; private set; }

    public static void Main(string[] args)
    {
        try
        {
            // Load configuration
            //var builder = new ConfigurationBuilder()
            //    .SetBasePath(Directory.GetCurrentDirectory())
            //    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            //Configuration = builder.Build();

            //var connectionString = Configuration.GetConnectionString("DefaultConnection");
            //if (string.IsNullOrEmpty(connectionString))
            //{
            //    throw new Exception("Connection string is missing or invalid.");
            //}

            var inputCsvPath = "complete_data.csv";

            var records = ReadCsv(inputCsvPath);

            //foreach (var record in records) {


            //foreach (var row in record.Tp_connections) {
            //    var tpconnections = new List<TpConnection>();
            //    tpconnections= ParseTpConnections(row,record.Id);
            //    foreach (var connection in tpconnections) { 
            //    Console.WriteLine(connection.SerialNo);
            //    }


            //}

            //var top_terms = ParseTopTerms(record.top_terms, record.Id);
            //Console.WriteLine(top_terms.Count);


            //    break;
            //}
            string connectionString = "Server=MYSQL5050.site4now.net;Database=db_a66689_cyberma;Uid=a66689_cyberma;Pwd=Root@pass1;";
            InsertDataIntoMySql(records, connectionString);

            Console.WriteLine("Data inserted into MySQL successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    public static List<ViewOpenAlexRecordFromCSV> ReadCsv(string inputCsvPath)
    {
        var records = new List<ViewOpenAlexRecordFromCSV>();

        using (var reader = new StreamReader(inputCsvPath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            csv.Read();
            csv.ReadHeader();

            while (csv.Read())
            {
                var record = new ViewOpenAlexRecordFromCSV
                {
                    // Generate a unique ID for each record
                    Id = Guid.NewGuid(),
                    Unnamed_0 = csv.GetField<int>("Unnamed: 0"),
                    OpenAlex_id = csv.GetField<string>("OpenAlex_id"),
                    Name = csv.GetField<string>("Name"),
                    total_no_publications = csv.GetField<int>("total_no_publications"),
                    no_publications_first_author = csv.GetField<int>("no_publications_first_author"),
                    orcid = csv.GetField<string>("orcid"),
                    publications_list = ParseList(csv.GetField<string>("publications_list")),
                    vec = ParseVec(csv.GetField<string>("vec")),
                    top_terms = ParseTopTerms(csv.GetField<string>("top_terms")),
                    Tp_connections = ParseTpConnections(csv.GetField<string>("Tp_connections"))
                    //top_terms = ParseList(csv.GetField<string>("top_terms")),
                    //Tp_connections = ParseList(csv.GetField<string>("Tp_connections"))
                };
                records.Add(record);
            }
        }

        return records;
    }

    public static void InsertDataIntoMySql(List<ViewOpenAlexRecordFromCSV> records, string connectionString)
    {
        using (var connection = new MySqlConnection(connectionString))
        {
            connection.Open();

            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    foreach (var record in records)
                    {
                        // Insert into OpenAlexRecord
                        var openAlexQuery = @"
                    INSERT INTO openalexrecords 
                    (Id, Unnamed_0, OpenAlex_id, Name, total_no_publications, no_publications_first_author, orcid, publications_list, vec) 
                    VALUES (@Id, @Unnamed_0, @OpenAlex_id, @Name, @TotalPublications, @FirstAuthorPublications, @Orcid, @PublicationsList, @Vec);";

                        using (var command = new MySqlCommand(openAlexQuery, connection, transaction))
                        {
                            command.Parameters.AddWithValue("@Id", record.Id);
                            command.Parameters.AddWithValue("@Unnamed_0", record.Unnamed_0);
                            command.Parameters.AddWithValue("@OpenAlex_id", record.OpenAlex_id);
                            command.Parameters.AddWithValue("@Name", record.Name);
                            command.Parameters.AddWithValue("@TotalPublications", record.total_no_publications);
                            command.Parameters.AddWithValue("@FirstAuthorPublications", record.no_publications_first_author);
                            command.Parameters.AddWithValue("@Orcid", record.orcid);
                            command.Parameters.AddWithValue("@PublicationsList", string.Join(",", record.publications_list ?? new List<string>()));
                            command.Parameters.AddWithValue("@Vec", string.Join(",", record.vec ?? new List<double>()));

                            command.ExecuteNonQuery();
                        }

                        // Bulk Insert TopTerms
                        if (record.top_terms != null)
                        {
                            var topTerms = ParseTopTerms(record.top_terms, record.Id);
                            var topTermsQuery = new StringBuilder("INSERT INTO topterms (Id, OpenAlexRecordId, `Key`, Weight) VALUES ");

                            var parameters = new List<MySqlParameter>();
                            int paramIndex = 0;

                            foreach (var term in topTerms)
                            {
                                topTermsQuery.Append($"(@Id{paramIndex}, @OpenAlexRecordId{paramIndex}, @Key{paramIndex}, @Weight{paramIndex}),");

                                parameters.Add(new MySqlParameter($"@Id{paramIndex}", term.Id));
                                parameters.Add(new MySqlParameter($"@OpenAlexRecordId{paramIndex}", record.Id));
                                parameters.Add(new MySqlParameter($"@Key{paramIndex}", term.Key));
                                parameters.Add(new MySqlParameter($"@Weight{paramIndex}", term.Weight));

                                paramIndex++;
                            }

                            // Remove trailing comma and execute
                            topTermsQuery.Length--;
                            topTermsQuery.Append(";");

                            using (var command = new MySqlCommand(topTermsQuery.ToString(), connection, transaction))
                            {
                                command.Parameters.AddRange(parameters.ToArray());
                                command.ExecuteNonQuery();
                            }
                        }

                        // Bulk Insert TpConnections
                        if (record.Tp_connections != null)
                        {
                            var tpConnectionsQuery = new StringBuilder("INSERT INTO tpconnections (Id, OpenAlexRecordId, SerialNo, Url, Weight, Topic) VALUES ");

                            var parameters = new List<MySqlParameter>();
                            int paramIndex = 0;

                            foreach (var connectionItem in record.Tp_connections)
                            {
                                var tpConnections = ParseTpConnections(connectionItem, record.Id);

                                if (tpConnections != null)
                                {
                                    foreach (var tp in tpConnections)
                                    {
                                        tpConnectionsQuery.Append($"(@Id{paramIndex}, @OpenAlexRecordId{paramIndex}, @SerialNo{paramIndex}, @Url{paramIndex}, @Weight{paramIndex}, @Topic{paramIndex}),");

                                        parameters.Add(new MySqlParameter($"@Id{paramIndex}", tp.Id));
                                        parameters.Add(new MySqlParameter($"@OpenAlexRecordId{paramIndex}", record.Id));
                                        parameters.Add(new MySqlParameter($"@SerialNo{paramIndex}", tp.SerialNo));
                                        parameters.Add(new MySqlParameter($"@Url{paramIndex}", tp.Url));
                                        parameters.Add(new MySqlParameter($"@Weight{paramIndex}", tp.Weight));
                                        parameters.Add(new MySqlParameter($"@Topic{paramIndex}", tp.Topic));

                                        paramIndex++;
                                    }
                                }
                            }

                            // Remove trailing comma and execute
                            tpConnectionsQuery.Length--;
                            tpConnectionsQuery.Append(";");

                            using (var command = new MySqlCommand(tpConnectionsQuery.ToString(), connection, transaction))
                            {
                                command.Parameters.AddRange(parameters.ToArray());
                                command.ExecuteNonQuery();
                            }
                        }
                    }

                    // Commit the transaction after processing all records
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    // Rollback in case of error
                    transaction.Rollback();
                    throw;
                }
            }
        }

    }

    public static List<TpConnection> ParseTpConnections(string rawData, Guid openAlexRecordId)
    {
        var result = new List<TpConnection>();

        if (string.IsNullOrWhiteSpace(rawData))
            return result;

        // Split by comma-separated entries
        var entries = rawData.Split(new[] { "], " }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var entry in entries)
        {
            // Extract the serial number and the rest of the data
            var parts = entry.Split(new[] { ": [" }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                if (int.TryParse(parts[0].Trim(), out var serialNo))
                {
                    // Further split the remaining data into URL, weight, and topic
                    var data = parts[1].Trim('[', ']', ' ').Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);

                    if (data.Length == 3)
                    {
                        var url = data[0].Trim('\'', '\"');
                        var weightStr = data[1];
                        var topic = data[2].Trim('\'', '\"');

                        if (double.TryParse(weightStr, out var weight))
                        {
                            result.Add(new TpConnection
                            {
                                Id = Guid.NewGuid(), // Assign a unique identifier
                                OpenAlexRecordId = openAlexRecordId, // Link to the parent record
                                SerialNo = serialNo, // Assign the serial number
                                Url = url, // Assign the URL
                                Weight = weight, // Assign the weight
                                Topic = topic // Assign the topic
                            });
                        }
                    }
                }
            }
        }

        return result;
    }

    public static List<TopTerms> ParseTopTerms(List<string> inputData, Guid openAlexRecordId)
    {
        var result = new List<TopTerms>();

        if (inputData == null || !inputData.Any())
            return result;

        foreach (var line in inputData)
        {
            // Split each line by colon into key and value
            var parts = line.Split(':', 2); // Split into exactly two parts
            if (parts.Length == 2)
            {
                // Clean up the key and value
                var key = parts[0].Trim('\'', '\"', ' ', '\r', '\n');
                var weightStr = parts[1].Trim();

                if (double.TryParse(weightStr, out var weight))
                {
                    result.Add(new TopTerms
                    {
                        Id = Guid.NewGuid(), // Assign a unique identifier
                        OpenAlexRecordId = openAlexRecordId, // Link to the parent record
                        Key = key, // Assign the key
                        Weight = weight // Assign the weight
                    });
                }
            }
        }

        return result;
    }

    public static List<string> ParseList(string input)
    {
        if (string.IsNullOrEmpty(input))
            return new List<string>();

        return input
            .Trim('[', ']')
            .Split(',')
            .Select(x => x.Trim().Trim('\'')) // Remove surrounding spaces and quotes
            .ToList();
    }

    /// <summary>
    /// Parses a comma-separated string into a list of doubles.
    /// </summary>
    public static List<double> ParseVec(string input)
    {
        if (string.IsNullOrEmpty(input))
            return new List<double>();

        return input
            .Trim('[', ']')
            .Split(',')
            .Select(x => double.TryParse(x.Trim(), out var num) ? num : 0)
            .ToList();
    }
    public static List<string> ParseTopTerms(string input)
    {
        if (string.IsNullOrEmpty(input))
            return new List<string>();

        return input
            .Trim('{', '}') // Remove surrounding braces
            .Split(',')
            .Select(x => x.Trim()) // Trim each key-value pair
            .ToList();
    }

    public static List<string> ParseTpConnections(string input)
    {
        if (string.IsNullOrEmpty(input))
            return new List<string>();

        return input
            .Trim('{', '}') // Remove surrounding braces
            .Split('|') // Split by delimiter for key-value pairs
            .Select(x => x.Trim()) // Trim each entry
            .ToList();
    }

   }
