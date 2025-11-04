using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace RiskDocumentDownloader
{
    class Program
    {
        static void Main(string[] args)
        {
            //string outputFolder = @"C:\DownloadedDocuments";

            Console.WriteLine("Document Downloader");
            Console.WriteLine("========================\n");

            Console.Write("Enter server name: ");
            string serverName = Console.ReadLine();

            Console.Write("Enter Database Name: ");
            string databaseName = Console.ReadLine();

            Console.Write("Enter User ID: ");
            string userID = Console.ReadLine();

            Console.Write("Enter Password: ");
            string password = Console.ReadLine();

            Console.Write("Enter output folder path (e.g., C:\\Downloads): ");
            string outputFolder = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(outputFolder))
            {
                outputFolder = @"C:\DownloadedDocuments"; // Default fallback
                Console.WriteLine($"No path entered. Using default: {outputFolder}");
            }

            Console.WriteLine($"\nConnection String: Server={serverName};Database={databaseName};User Id={userID};Password=***");
            Console.WriteLine($"Attempting to connect...\n");

            string connectionString = $"Server={serverName};Database={databaseName};User Id={userID};Password={password};";

            try
            {
                if (!Directory.Exists(outputFolder))
                {
                    Directory.CreateDirectory(outputFolder);
                }

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("Connected to database successfully.\n");

                    // Process Risk Documents
                    ProcessRiskDocuments(connection, outputFolder);

                    // Process Incident Documents
                    ProcessIncidentDocuments(connection, outputFolder);

                    // Process Control Documents
                    ProcessControlDocuments(connection, outputFolder);

                    // Process Action Documents
                    ProcessActionDocuments(connection, outputFolder);

                    Console.WriteLine("\n" + new string('=', 50));
                    Console.WriteLine("All downloads complete!");
                    Console.WriteLine($"Output Folder: {outputFolder}");
                    Console.WriteLine(new string('=', 50));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nFatal Error: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        static void ProcessRiskDocuments(SqlConnection connection, string baseFolder)
        {
            Console.WriteLine("\n" + new string('=', 50));
            Console.WriteLine("PROCESSING RISK DOCUMENTS");
            Console.WriteLine(new string('=', 50));

            string riskFolder = Path.Combine(baseFolder, "Risk");
            if (!Directory.Exists(riskFolder))
            {
                Directory.CreateDirectory(riskFolder);
            }

            string query = @"
                SELECT 
                    RADoc.AssessmentDocumentId, 
                    RADoc.AssessmentDetailId,
                    RADet.RiskCode,
                    RADet.RiskTypeId,
                    RRT.FieldName,
                    RADet.Title AS RiskAssessmentDetailTitle,
                    RADoc.Title, 
                    RADoc.FileName, 
                    RADoc.FileData  
                FROM 
                    RISK_AssessmentDocument AS RADoc 
                INNER JOIN 
                    RISK_AssessmentDetail AS RADet 
                ON
                    RADet.AssessmentDetailId = RADoc.AssessmentDetailId 
                INNER JOIN
                    RISK_RiskType AS RRT
                ON
                    RRT.RiskTypeId = RADet.RiskTypeId
                ORDER BY 
                    RADet.RiskTypeId";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.CommandTimeout = 300;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    int totalDocuments = 0;
                    int successCount = 0;
                    int failCount = 0;
                    string currentRiskTitle = "";

                    while (reader.Read())
                    {
                        totalDocuments++;

                        try
                        {
                            int assessmentDocumentId = reader.GetInt32(reader.GetOrdinal("AssessmentDocumentId"));
                            string fieldName = reader["FieldName"]?.ToString() ?? "Unknown_FieldName";
                            string riskCode = reader["RiskCode"]?.ToString() ?? "Unknown_RiskCode";
                            string riskTitle = reader["RiskAssessmentDetailTitle"]?.ToString() ?? "Unknown_Risk";
                            string documentTitle = reader["Title"]?.ToString() ?? "Untitled";
                            string fileName = reader["FileName"]?.ToString();
                            byte[] fileData = reader["FileData"] as byte[];

                            if (fileData == null || fileData.Length == 0)
                            {
                                Console.WriteLine($"  ⚠ Skipping Document ID {assessmentDocumentId} - No file data");
                                failCount++;
                                continue;
                            }

                            string riskIdentifier = $"{fieldName}_{riskCode}";

                            if (currentRiskTitle != riskIdentifier)
                            {
                                currentRiskTitle = riskIdentifier;
                                Console.WriteLine($"\n📁 Processing: {fieldName} → {riskCode} - {riskTitle}");
                            }

                            string fieldNameFolder = Path.Combine(riskFolder, SanitizeFolderName(fieldName));
                            if (!Directory.Exists(fieldNameFolder))
                            {
                                Directory.CreateDirectory(fieldNameFolder);
                            }

                            string riskFolderName = $"{riskCode}";
                            string subFolder = Path.Combine(fieldNameFolder, SanitizeFolderName(riskFolderName));
                            if (!Directory.Exists(subFolder))
                            {
                                Directory.CreateDirectory(subFolder);
                            }

                            if (string.IsNullOrWhiteSpace(fileName))
                            {
                                fileName = documentTitle;
                            }

                            if (!Path.HasExtension(fileName))
                            {
                                fileName += ".docx";
                            }

                            fileName = SanitizeFileName(fileName);
                            string filePath = Path.Combine(subFolder, fileName);
                            filePath = GetUniqueFilePath(filePath);

                            File.WriteAllBytes(filePath, fileData);

                            Console.WriteLine($"  Downloaded: {fileName} ({FormatFileSize(fileData.Length)})");
                            successCount++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  Error downloading document: {ex.Message}");
                            failCount++;
                        }
                    }

                    Console.WriteLine($"\nRisk Documents - Total: {totalDocuments}, Success: {successCount}, Failed: {failCount}");
                }
            }
        }

        static void ProcessIncidentDocuments(SqlConnection connection, string baseFolder)
        {
            Console.WriteLine("\n" + new string('=', 50));
            Console.WriteLine("PROCESSING INCIDENT DOCUMENTS");
            Console.WriteLine(new string('=', 50));

            string incidentFolder = Path.Combine(baseFolder, "Incident");
            if (!Directory.Exists(incidentFolder))
            {
                Directory.CreateDirectory(incidentFolder);
            }

            string query = @"
        SELECT
            I.IncidentId,
            I.IncidentTitle,
            I.IncidentCode,
            E.DocumentId,
            E.[File],
            E.Name,
            E.FilePath
        FROM
            Incident AS I
        INNER JOIN
            EntityDocument AS E ON E.ObjectDataId = I.IncidentId
        WHERE
            E.[File] IS NOT NULL AND E.IsDeleted = 0
        ORDER BY
            I.IncidentCode";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.CommandTimeout = 300;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    int totalDocuments = 0;
                    int successCount = 0;
                    int failCount = 0;
                    string currentIncidentCode = "";

                    while (reader.Read())
                    {
                        totalDocuments++;

                        try
                        {
                            string incidentCode = reader["IncidentCode"]?.ToString() ?? "Unknown_Code";
                            string incidentTitle = reader["IncidentTitle"]?.ToString() ?? "Unknown_Title";
                            string originalFileName = reader["Name"]?.ToString();
                            string filePath = reader["FilePath"]?.ToString();
                            byte[] fileData = reader["File"] as byte[];
                            string namePart = !string.IsNullOrWhiteSpace(originalFileName)
                                                ? Path.GetFileNameWithoutExtension(originalFileName)
                                                : $"Document_{totalDocuments}";
                            

                            if (fileData == null || fileData.Length == 0)
                            {
                                Console.WriteLine($"  ⚠ Skipping document - No file data");
                                failCount++;
                                continue;
                            }

                            // Create subfolder for each incident
                            string incidentFolderName = $"{incidentCode}";
                            if (currentIncidentCode != incidentCode)
                            {
                                currentIncidentCode = incidentCode;
                                Console.WriteLine($"\n📁 Processing: {incidentFolderName}");
                            }

                            string incidentSubFolder = Path.Combine(incidentFolder, SanitizeFolderName(incidentFolderName));
                            if (!Directory.Exists(incidentSubFolder))
                            {
                                Directory.CreateDirectory(incidentSubFolder);
                            }

                            // Get the file extension from original file
                            string extension = "";
                            if (!string.IsNullOrWhiteSpace(originalFileName))
                            {
                                extension = Path.GetExtension(originalFileName);
                            }
                            else if (!string.IsNullOrWhiteSpace(filePath))
                            {
                                extension = Path.GetExtension(filePath);
                            }

                            if (string.IsNullOrWhiteSpace(extension))
                            {
                                extension = GuessFileExtension(fileData) ?? ".bin";
                            }

                            // Name the file using incident code + extension
                            string fileName = $"{incidentCode}_{namePart}{extension}";
                            fileName = SanitizeFileName(fileName);

                            string fullPath = Path.Combine(incidentSubFolder, fileName);
                            fullPath = GetUniqueFilePath(fullPath);

                            File.WriteAllBytes(fullPath, fileData);

                            Console.WriteLine($"  Downloaded: {fileName} ({FormatFileSize(fileData.Length)})");
                            successCount++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  Error downloading document: {ex.Message}");
                            failCount++;
                        }
                    }

                    Console.WriteLine($"\nIncident Documents - Total: {totalDocuments}, Success: {successCount}, Failed: {failCount}");
                }
            }
        }

        static void ProcessControlDocuments(SqlConnection connection, string baseFolder)
        {
            Console.WriteLine("\n" + new string('=', 50));
            Console.WriteLine("PROCESSING CONTROL DOCUMENTS");
            Console.WriteLine(new string('=', 50));

            string controlFolder = Path.Combine(baseFolder, "Control");
            if (!Directory.Exists(controlFolder))
            {
                Directory.CreateDirectory(controlFolder);
            }

            string query = @"
                SELECT
                    ControlDetailId,
                    Title,
                    FileName,
                    FileData
                FROM
                    ControlDetails A INNER JOIN
                    ControlDocuments B ON A.id = B.ControlDetailId
                ORDER BY
                    ControlDetailId";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.CommandTimeout = 300;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    int totalDocuments = 0;
                    int successCount = 0;
                    int failCount = 0;
                    int currentControlId = -1;

                    while (reader.Read())
                    {
                        totalDocuments++;

                        try
                        {
                            int controlDetailId = reader.GetInt32(reader.GetOrdinal("ControlDetailId"));
                            string title = reader["Title"]?.ToString() ?? "Untitled";
                            string fileName = reader["FileName"]?.ToString();
                            byte[] fileData = reader["FileData"] as byte[];

                            if (fileData == null || fileData.Length == 0)
                            {
                                Console.WriteLine($"  ⚠ Skipping Control ID {controlDetailId} - No file data");
                                failCount++;
                                continue;
                            }

                            if (currentControlId != controlDetailId)
                            {
                                currentControlId = controlDetailId;
                                Console.WriteLine($"\n📁 Processing Control ID: {controlDetailId} - {title}");
                            }

                            string controlSubFolder = Path.Combine(controlFolder, SanitizeFolderName($"Control_{controlDetailId}_{title}"));
                            if (!Directory.Exists(controlSubFolder))
                            {
                                Directory.CreateDirectory(controlSubFolder);
                            }

                            if (string.IsNullOrWhiteSpace(fileName))
                            {
                                fileName = title;
                            }

                            if (!Path.HasExtension(fileName))
                            {
                                fileName += ".docx";
                            }

                            fileName = SanitizeFileName(fileName);
                            string filePath = Path.Combine(controlSubFolder, fileName);
                            filePath = GetUniqueFilePath(filePath);

                            File.WriteAllBytes(filePath, fileData);

                            Console.WriteLine($"  Downloaded: {fileName} ({FormatFileSize(fileData.Length)})");
                            successCount++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  Error downloading document: {ex.Message}");
                            failCount++;
                        }
                    }

                    Console.WriteLine($"\nControl Documents - Total: {totalDocuments}, Success: {successCount}, Failed: {failCount}");
                }
            }
        }

        static void ProcessActionDocuments(SqlConnection connection, string baseFolder)
        {
            Console.WriteLine("\n" + new string('=', 50));
            Console.WriteLine("PROCESSING ACTION DOCUMENTS");
            Console.WriteLine(new string('=', 50));

            string actionFolder = Path.Combine(baseFolder, "Action");
            if (!Directory.Exists(actionFolder))
            {
                Directory.CreateDirectory(actionFolder);
            }

            string query = @"
                SELECT
                    ActionDetailId,
                    Title,
                    FileName,
                    FileData
                FROM
                    Action_Document";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.CommandTimeout = 300;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    int totalDocuments = 0;
                    int successCount = 0;
                    int failCount = 0;
                    int currentActionId = -1;

                    while (reader.Read())
                    {
                        totalDocuments++;

                        try
                        {
                            int actionDetailId = reader.GetInt32(reader.GetOrdinal("ActionDetailId"));
                            string title = reader["Title"]?.ToString() ?? "Untitled";
                            string fileName = reader["FileName"]?.ToString();
                            byte[] fileData = reader["FileData"] as byte[];

                            if (fileData == null || fileData.Length == 0)
                            {
                                Console.WriteLine($"  ⚠ Skipping Action ID {actionDetailId} - No file data");
                                failCount++;
                                continue;
                            }

                            if (currentActionId != actionDetailId)
                            {
                                currentActionId = actionDetailId;
                                Console.WriteLine($"\n📁 Processing Action ID: {actionDetailId} - {title}");
                            }

                            string actionSubFolder = Path.Combine(actionFolder, SanitizeFolderName($"Action_{actionDetailId}_{title}"));
                            if (!Directory.Exists(actionSubFolder))
                            {
                                Directory.CreateDirectory(actionSubFolder);
                            }

                            if (string.IsNullOrWhiteSpace(fileName))
                            {
                                fileName = title;
                            }

                            if (!Path.HasExtension(fileName))
                            {
                                fileName += ".docx";
                            }

                            fileName = SanitizeFileName(fileName);
                            string filePath = Path.Combine(actionSubFolder, fileName);
                            filePath = GetUniqueFilePath(filePath);

                            File.WriteAllBytes(filePath, fileData);

                            Console.WriteLine($"  Downloaded: {fileName} ({FormatFileSize(fileData.Length)})");
                            successCount++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  Error downloading document: {ex.Message}");
                            failCount++;
                        }
                    }

                    Console.WriteLine($"\nAction Documents - Total: {totalDocuments}, Success: {successCount}, Failed: {failCount}");
                }
            }
        }

        static string SanitizeFolderName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return "Unknown";

            // Trim leading/trailing whitespace
            name = name.Trim();

            // Replace invalid path characters
            char[] invalidChars = Path.GetInvalidPathChars();
            foreach (char c in invalidChars)
            {
                name = name.Replace(c, '_');
            }

            // Replace additional problematic characters
            name = name.Replace("/", "_")
                       .Replace("\\", "_")
                       .Replace(":", "_")
                       .Replace("*", "_")
                       .Replace("?", "_")
                       .Replace("\"", "_")
                       .Replace("<", "_")
                       .Replace(">", "_")
                       .Replace("|", "_");

            // Limit folder name length to prevent path length issues
            if (name.Length > 100)
            {
                name = name.Substring(0, 100);
            }

            // Trim again in case truncation added trailing spaces
            name = name.Trim();

            return name;
        }

        static string SanitizeFileName(string fileName)
        {
            char[] invalidChars = Path.GetInvalidFileNameChars();
            foreach (char c in invalidChars)
            {
                fileName = fileName.Replace(c, '_');
            }
            return fileName;
        }

        static string GetUniqueFilePath(string filePath)
        {
            if (!File.Exists(filePath))
            {
                return filePath;
            }

            string directory = Path.GetDirectoryName(filePath);
            string fileNameWithoutExt = Path.GetFileNameWithoutExtension(filePath);
            string extension = Path.GetExtension(filePath);

            int counter = 1;
            string newFilePath;
            do
            {
                newFilePath = Path.Combine(directory, $"{fileNameWithoutExt}_{counter}{extension}");
                counter++;
            } while (File.Exists(newFilePath));

            return newFilePath;
        }

        static string FormatFileSize(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB" };
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len = len / 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        static string GuessFileExtension(byte[] fileData)
        {
            if (fileData == null || fileData.Length < 4)
                return null;

            // PDF
            if (fileData[0] == 0x25 && fileData[1] == 0x50 && fileData[2] == 0x44 && fileData[3] == 0x46)
                return ".pdf";

            // DOCX / ZIP
            if (fileData[0] == 0x50 && fileData[1] == 0x4B)
                return ".docx";

            // PNG
            if (fileData[0] == 0x89 && fileData[1] == 0x50 && fileData[2] == 0x4E && fileData[3] == 0x47)
                return ".png";

            // JPG
            if (fileData[0] == 0xFF && fileData[1] == 0xD8)
                return ".jpg";

            // Default fallback
            return null;
        }
    }
}