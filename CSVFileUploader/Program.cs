//using System;
//using System.Collections.Generic;
//using CsvHelper;
//using CsvHelper.Configuration;
//using System.Globalization;
//using System.IO;
//using System.Linq;
//using System.ComponentModel.DataAnnotations.Schema;
//using System.ComponentModel.DataAnnotations;
//using System.Configuration;
//using Microsoft.Extensions.Configuration;
//public class OpenAlexRecord
//{
//   public Guid Id { get; set; } 
//    public int Unnamed_0 { get; set; }
//    public string OpenAlex_id { get; set; }
//    public string Name { get; set; }
//    public int total_no_publications { get; set; }
//    public int no_publications_first_author { get; set; }
//    public string orcid { get; set; }
//    public List<string> publications_list { get; set; }
//    public List<double> vec { get; set; }
//    public List<string> top_terms { get; set; }
//    public List<string> Tp_connections { get; set; }
//}
//public class Program
//{
//    public static IConfiguration Configuration { get; private set; }

//    public static void Main()
//    {

//        // Build configuration
//        var builder = new ConfigurationBuilder();
//        builder.Build();
//        var connectionString = Configuration.GetConnectionString("DefaultConnection");
//        // Input CSV path
//        var inputCsvPath = "complete_data.csv";
//        var outputCsvPath = "output.csv";

//        // Read the data
//        using (var reader = new StreamReader(inputCsvPath))
//        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
//        {
//            var records = new List<OpenAlexRecord>();
//            csv.Read();
//            csv.ReadHeader(); // Skip the header

//            // Read all rows
//            while (csv.Read())
//            {
//                var record = new OpenAlexRecord
//                {
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

//            Console.WriteLine("CSV processed successfully!");
//        }
//    }

//    // Helper method to parse the string list representation
//    public static List<string> ParseList(string input)
//    {
//        if (string.IsNullOrEmpty(input))
//            return new List<string>();

//        return input
//            .Trim('[', ']')
//            .Split(',')
//            .Select(x => x.Trim().Trim('\''))
//            .ToList();
//    }

//    // Helper method to parse the vector (list of doubles)
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
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;

public class OpenAlexRecord
{
    public Guid Id { get; set; }
    public int Unnamed_0 { get; set; }
    public string OpenAlex_id { get; set; }
    public string Name { get; set; }
    public int total_no_publications { get; set; }
    public int no_publications_first_author { get; set; }
    public string orcid { get; set; }
    public List<string> publications_list { get; set; }
    public List<double> vec { get; set; }
    public List<string> top_terms { get; set; }
    public List<string> Tp_connections { get; set; }
}

public class Program
{
    public static IConfiguration Configuration { get; private set; }

    public static void Main(string[] args)
    {
        try
        {
            // Load configuration
            // Load configuration
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true); // Ensure optional is false to enforce loading
            Configuration = builder.Build();

            // Retrieve connection string
            var connectionString = Configuration.GetConnectionString("DefaultConnection"); if (string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("Recorrect connection string again please");
            }
                // Input CSV path
           var inputCsvPath = "complete_data.csv";

            // Step 1: Read CSV data into a list of records
            var records = ReadCsv(inputCsvPath);
            Console.WriteLine("show capacity "+records.Capacity);
            // Step 2: Insert data into MySQL
            InsertDataIntoMySql(records, connectionString);

            Console.WriteLine("Data inserted into MySQL successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    /// <summary>
    /// Reads CSV file and maps it to a list of OpenAlexRecord objects.
    /// </summary>
    public static List<OpenAlexRecord> ReadCsv(string inputCsvPath)
    {
        var records = new List<OpenAlexRecord>();

        using (var reader = new StreamReader(inputCsvPath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            csv.Read();
            csv.ReadHeader(); // Skip the header row

            while (csv.Read())
            {
                var record = new OpenAlexRecord
                {
                    Id = Guid.NewGuid(), // Generate a unique ID for each record
                    Unnamed_0 = csv.GetField<int>("Unnamed: 0"),
                    OpenAlex_id = csv.GetField<string>("OpenAlex_id"),
                    Name = csv.GetField<string>("Name"),
                    total_no_publications = csv.GetField<int>("total_no_publications"),
                    no_publications_first_author = csv.GetField<int>("no_publications_first_author"),
                    orcid = csv.GetField<string>("orcid"),
                    publications_list = ParseList(csv.GetField<string>("publications_list")),
                    vec = ParseVec(csv.GetField<string>("vec")),
                    top_terms = ParseList(csv.GetField<string>("top_terms")),
                    Tp_connections = ParseList(csv.GetField<string>("Tp_connections"))
                };
                records.Add(record);
            }
        }

        return records;
    }

    /// <summary>
    /// Inserts a list of OpenAlexRecord objects into a MySQL database.
    /// </summary>
    public static void InsertDataIntoMySql(List<OpenAlexRecord> records, string connectionString)
    {
        using (var connection = new MySqlConnection(connectionString))
        {
            connection.Open();

            foreach (var record in records)
            {
                var query = @"
                    INSERT INTO openalexrecords 
                    (Id, Unnamed_0, OpenAlex_id, Name, total_no_publications, no_publications_first_author, orcid, 
                     publications_list, vec, top_terms, Tp_connections)
                    VALUES 
                    (@Id, @Unnamed_0, @OpenAlex_id, @Name, @total_no_publications, @no_publications_first_author, @orcid, 
                     @PublicationsList, @Vec, @TopTerms, @TpConnections)";

                using (var command = new MySqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@Id", record.Id);
                    command.Parameters.AddWithValue("@Unnamed_0", record.Unnamed_0);
                    command.Parameters.AddWithValue("@OpenAlex_id", record.OpenAlex_id);
                    command.Parameters.AddWithValue("@Name", record.Name);
                    command.Parameters.AddWithValue("@total_no_publications", record.total_no_publications);
                    command.Parameters.AddWithValue("@no_publications_first_author", record.no_publications_first_author);
                    command.Parameters.AddWithValue("@orcid", record.orcid);
                    command.Parameters.AddWithValue("@PublicationsList", string.Join(",", record.publications_list));
                    command.Parameters.AddWithValue("@Vec", string.Join(",", record.vec));
                    command.Parameters.AddWithValue("@TopTerms", string.Join(",", record.top_terms));
                    command.Parameters.AddWithValue("@TpConnections", string.Join(",", record.Tp_connections));

                    command.ExecuteNonQuery();
                }
            }
        }
    }

    /// <summary>
    /// Parses a comma-separated string into a list of strings.
    /// </summary>
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
}
