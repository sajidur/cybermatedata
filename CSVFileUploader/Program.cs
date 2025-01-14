
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Security.Policy;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Org.BouncyCastle.Bcpg.OpenPgp;
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
    public string? Profile { get; set; }
    public string? University { get; set; }
    public int? total_no_publications { get; set; }
    public string? orcid { get; set; }
    public int? no_publications_first_author { get; set; }
    public string? publications_list { get; set; }
    public string? vec { get; set; }
    public string? top_terms { get; set; }
    public string? keyterms { get; set; }   
    public string? Tp_connections { get; set; }
}
public class Program
{
    public static IConfiguration Configuration { get; private set; }

    public static void Main(string[] args)
    {
        try
        {
         

            var inputCsvPath = "complete_data.csv";

            var records = ReadCsv(inputCsvPath);
            int mx = 0;
            foreach (var record in records)
            {

                if (record.OpenAlex_id == "https://openalex.org/A5014513107")
                {
                    Console.WriteLine(record.Tp_connections.Count);
                }
                //if (record.Tp_connections.Count == 10)
                //{
                //    mx++;
                //    //foreach (var connection in record.Tp_connections)
                //    //{
                //    //    Console.WriteLine(connection);
                //    //}
                //}


                //Console.WriteLine(record.top_terms.Count);
                //foreach (var row in record.top_terms) { Console.WriteLine(row); }
                //string x = ConvertToJsonString(record.top_terms);
                //Console.WriteLine(x);
                //string k = ExtractTermsFromList(record.top_terms);
                //Console.WriteLine(k);
                //if (record.Tp_connections.Count == 10)
                //{
                //    //Console.WriteLine(record.OpenAlex_id);
                //    //break;
                //}
            }
            Console.WriteLine(mx);
            string connectionString = "Server=MYSQL5050.site4now.net;Database=db_a66689_cyberma;Uid=a66689_cyberma;Pwd=Root@pass1;";
           // InsertDataIntoMySql(records, connectionString);

           
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
                };
                records.Add(record);
            }
        }

        return records;
    }
    public static void InsertDataIntoMySql(List<ViewOpenAlexRecordFromCSV> records, string connectionString)
    {
        const int BatchSize = 500; // Adjust based on requirements

        using var connection = new MySqlConnection(connectionString);
        connection.Open();

        int recordCount = 0;

        foreach (var batch in records.Chunk(BatchSize))
        {
            using var transaction = connection.BeginTransaction();

            try
            {
                foreach (var record in batch)
                {
                    InsertRecord(connection, transaction, record);
                }

                transaction.Commit();
                recordCount += batch.Count();

                Console.WriteLine($"Processed {recordCount} records successfully.");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine($"Batch failed: {ex.Message}");
                throw;
            }
        }
    }
    private static void InsertRecord(MySqlConnection connection, MySqlTransaction transaction, ViewOpenAlexRecordFromCSV  record)
    {
        const string query = @"
            INSERT INTO openalexrecords 
            (Id, Unnamed_0, OpenAlex_id, Name, Profile, University, total_no_publications, orcid, no_publications_first_author, 
             publications_list, vec, top_terms, keyterms, Tp_connections)
            VALUES
            (@Id, @Unnamed_0, @OpenAlex_id, @Name, @Profile, @University, @TotalNoPublications, @Orcid, 
             @NoPublicationsFirstAuthor, @PublicationsList, @Vec, @TopTerms, @Keyterms, @TpConnections)";

        using var command = new MySqlCommand(query, connection, transaction)
        {
            CommandTimeout = 300 // Adjust timeout as necessary
        };

        command.Parameters.AddWithValue("@Id", record.Id);
        command.Parameters.AddWithValue("@Unnamed_0", record.Unnamed_0 ?? 0);
        command.Parameters.AddWithValue("@OpenAlex_id", record.OpenAlex_id ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@Name", record.Name ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@Profile", "");
        command.Parameters.AddWithValue("@University", "");
        command.Parameters.AddWithValue("@TotalNoPublications", record.total_no_publications ??0);
        command.Parameters.AddWithValue("@Orcid", record.orcid ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@NoPublicationsFirstAuthor", record.no_publications_first_author ??0);
        command.Parameters.AddWithValue("@PublicationsList", string.Join(",", record.publications_list ?? new List<string>()));
        command.Parameters.AddWithValue("@Vec", string.Join(",", record.vec ?? new List<double>()));
        command.Parameters.AddWithValue("@TopTerms", ConvertToJsonString(record.top_terms) ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@Keyterms", ExtractTermsFromList(record.top_terms) ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@TpConnections", ParseTpConnectionsForQuery(record.Tp_connections, record.OpenAlex_id) ?? (object)DBNull.Value);

        command.ExecuteNonQuery();
    }

    //public static void InsertDataIntoMySql(List<ViewOpenAlexRecordFromCSV> records, string connectionString)
    //{
    //    const int BatchSize = 1; // Adjust for efficiency
    //    using var connection = new MySqlConnection(connectionString);
    //    connection.Open();

    //    int recordCount = 0;
    //    foreach (var batch in records.Chunk(BatchSize))
    //    {
    //        using var transaction = connection.BeginTransaction();
    //        try
    //        {
    //            var openAlexBatchQuery = new StringBuilder();
    //            foreach (var record in batch)
    //            {
    //                AppendOpenAlexRecord(openAlexBatchQuery, record);
    //            }

    //            if (openAlexBatchQuery.Length > 0)
    //            {
    //                ExecuteBatchQuery(connection, transaction, openAlexBatchQuery.ToString());
    //            }

    //            transaction.Commit();
    //            recordCount += batch.Count();
    //        }
    //        catch (Exception ex)
    //        {
    //            transaction.Rollback();
    //            Console.WriteLine($"Batch failed: {ex.Message}");
    //            throw;
    //        }
    //        Console.WriteLine($"Processed {recordCount} records successfully.");
    //    }
    //}

    //private static void AppendOpenAlexRecord(StringBuilder queryBuilder, ViewOpenAlexRecordFromCSV record)
    //{
    //    if (queryBuilder.Length == 0)
    //    {
    //        queryBuilder.Append("INSERT INTO openalexrecords (Id, Unnamed_0, OpenAlex_id, Name, Profile, University, total_no_publications, orcid, no_publications_first_author, publications_list, vec, top_terms, keyterms, Tp_connections) VALUES ");
    //    }


    //    int escapedUnnamed0 = record.Unnamed_0 ?? 0;
    //    string escapedOpenAlexId =record.OpenAlex_id??"";
    //    string escapedName = (record.Name)??"";
    //    string escapedProfile = "";
    //    string escapedUniversity = "";
    //    int totalNoPublications = record.total_no_publications ?? 0;
    //    string escapedOrcid = record.orcid??"";
    //    int noPublicationsFirstAuthor = record.no_publications_first_author ?? 0;
    //    string escapedPublicationsList = string.Join(",", record.publications_list ?? new List<string>());
    //    string escapedVec = string.Join(",", record.vec ?? new List<double>());
    //    string escapedTopTerms = ConvertToJsonString(record.top_terms);
    //    string escapedKeyterms = ExtractTermsFromList(record.top_terms);
    //    string escapedTpConnections = ParseTpConnectionsForQuery(record.Tp_connections, record.OpenAlex_id);

    //    queryBuilder.AppendFormat("('{0}', {1}, '{2}', '{3}', '{4}', '{5}', {6}, '{7}', {8}, '{9}', '{10}', '{11}', '{12}', '{13}'),",
    //        record.Id, escapedUnnamed0, escapedOpenAlexId, escapedName, escapedProfile, escapedUniversity,
    //        totalNoPublications, escapedOrcid, noPublicationsFirstAuthor, escapedPublicationsList,
    //        escapedVec, escapedTopTerms, escapedKeyterms, escapedTpConnections);
    //}

    //private static void ExecuteBatchQuery(MySqlConnection connection, MySqlTransaction transaction, string query)
    //{
    //    if (string.IsNullOrWhiteSpace(query)) return;

    //    query = query.TrimEnd(',') + ";"; // Ensure valid SQL
    //    using var command = new MySqlCommand(query, connection, transaction)
    //    {
    //        CommandTimeout = 300 // Adjust timeout as necessary
    //    };
    //    command.ExecuteNonQuery();
    //}

    private static string ParseTpConnectionsForQuery(List<string> input,string source)
    {
    
        if (input == null || input.Count == 0)
            return "[]"; // Return an empty JSON array string if the input is empty
        int lastSlashIndex = source.LastIndexOf('/');

        // Extract the substring after the last '/'
        string result = source.Substring(lastSlashIndex + 1);
        var formattedData = input.Select(item =>
        {
            // Remove square brackets and split by commas
            var parts = item.Trim('[', ']').Split(new[] { ", " }, StringSplitOptions.None);

            // Parse each part into the corresponding structure
            return new
            {
                source= result,
                target = parts[0].Trim('\''),
                distance = double.Parse(parts[1]),
                category = parts[2].Trim('\'')
            };
        }).ToList();

        // Serialize the list of objects to a JSON string
        return JsonSerializer.Serialize(formattedData);
    
    }
    public static string ConvertToJsonString(List<string> input)
    {
        if (input == null || input.Count == 0)
            return "[]"; // Return empty JSON array if input is null or empty.

        var formattedData = input
            .Select(line =>
            {
                var parts = line.Split(':');
                if (parts.Length == 2)
                {
                    string keyTerm = parts[0].Trim('\'', ' ').Replace("\"", "\\\""); // Escape quotes in the key.
                    string value = parts[1].Trim().Replace("\"", "\\\""); // Escape quotes in the value.
                    return $"{{\"keyTerm\":\"{keyTerm}\",\"value\":\"{value}\"}}"; // Ensure proper JSON structure.
                }
                return null;
            })
            .Where(item => item != null) // Remove null entries.
            .ToList();

        return "[" + string.Join(",", formattedData) + "]"; // Combine formatted data into a JSON array.
    }

    public static string ExtractTermsFromList(List<string> input)
    {
        if (input == null || input.Count == 0)
            return "[]"; // Return empty JSON array if input is null or empty.

        var terms = input
            .Select(line =>
            {
                var parts = line.Split(':');
                if (parts.Length > 0)
                    return parts[0].Trim('\'', ' ').Replace("\"", "\\\""); // Extract key and escape quotes.
                return null;
            })
            .Where(term => !string.IsNullOrEmpty(term)) // Remove null or empty terms.
            .Distinct() // Ensure terms are unique.
            .ToList();

        return "[" + string.Join(",", terms.Select(term => $"\"{term}\"")) + "]"; // Convert terms into a JSON array.
    }

    //public static string ConvertToJsonString(List<string> input)
    //{
    //    if (input == null || input.Count == 0)
    //        return "[]";
    //    try
    //    {
    //        // Process each item in the list to extract key and value pairs
    //        var formattedData = input
    //            .Select(line =>
    //            {
    //                var parts = line.Split(':');
    //                if (parts.Length == 2)
    //                {
    //                    string keyTerm = parts[0].Trim('\'', ' ').Replace("\"", "\\\"");
    //                    string value = parts[1].Trim();
    //                    return $"{{\"keyTerm\":\"{keyTerm}\",\"value\":{value}}}";
    //                }
    //                return null;
    //            })
    //            .Where(item => item != null) // Remove null entries
    //            .ToList();

    //        // Combine all formatted objects into a single JSON-like array string
    //        return "[" + string.Join(",", formattedData) + "]";
    //    }
    //    catch (Exception ex) { throw; }
    //}
    //public static string ExtractTermsFromList(List<string> input)
    //{

    //    if (input == null || input.Count == 0)
    //        return "[]";
    //    try
    //    {
    //        // Process each item in the list to extract the term (key)
    //        var terms = input
    //            .Select(line => line.Split(':')[0].Trim('\'', ' ')) // Extract term (key) and trim quotes/spaces
    //            .Distinct() // Remove duplicates
    //            .ToList();

    //        // Convert the list of terms into a JSON-like string
    //        return "[" + string.Join(",", terms.Select(term => $"\"{term}\"")) + "]";
    //    }
    //    catch (Exception ex) { throw; }
    //}
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

        if (string.IsNullOrWhiteSpace(input))
            return new List<string>();
        try
        {
            // Match entries like ['https://openalex.org/A5012838811', 0.3314645024565294, 'design']
            var pattern = @"\['https://openalex.org/([A-Za-z0-9]+)', ([\d.]+), '([\w\s]+)'\]";
            var matches = Regex.Matches(input, pattern);

            // Format into desired structure
            return matches
                .Select(match => $"['{match.Groups[1].Value}', {match.Groups[2].Value}, '{match.Groups[3].Value}']")
                .ToList();
        }
        catch (Exception ex) { throw; }
    }

    //public static List<string> ParseTpConnections(string input)
    //{
    //    if (string.IsNullOrEmpty(input))
    //        return new List<string>();

    //    return input
    //        .Trim('{', '}') // Remove surrounding braces
    //        .Split('|') // Split by delimiter for key-value pairs
    //        .Select(x => x.Trim()) // Trim each entry
    //        .ToList();
    //}

}
