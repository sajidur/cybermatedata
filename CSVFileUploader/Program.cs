using System;
using System.Collections.Generic;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;

public class OpenAlexRecord
{
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
    public static void Main()
    {
        // Input CSV path
        var inputCsvPath = "complete_data.csv";
        var outputCsvPath = "output.csv";

        // Read the data
        using (var reader = new StreamReader(inputCsvPath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            var records = new List<OpenAlexRecord>();
            csv.Read();
            csv.ReadHeader(); // Skip the header

            // Read all rows
            while (csv.Read())
            {
                var record = new OpenAlexRecord
                {
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

            Console.WriteLine("CSV processed successfully!");
        }
    }

    // Helper method to parse the string list representation
    public static List<string> ParseList(string input)
    {
        if (string.IsNullOrEmpty(input))
            return new List<string>();

        return input
            .Trim('[', ']')
            .Split(',')
            .Select(x => x.Trim().Trim('\''))
            .ToList();
    }

    // Helper method to parse the vector (list of doubles)
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
