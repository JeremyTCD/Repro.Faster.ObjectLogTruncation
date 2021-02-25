using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Repro.Faster.ObjectLogTruncation
{
    /// <summary>
    /// Store segment size == device capacity. Expectation is that logs get truncated so at most one segment exists for each log.
    /// 
    /// The main log does get truncated, but contains 1 more file than expected. The object log does not get truncated.
    /// </summary>
    public class Program
    {
        public static async Task Main()
        {
            // Cache directory
            string cacheDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "FasterCache");
            TryDeleteDirectory(cacheDirectory); // Clean up so previous runs don't affect current run

            // Settings
            string logFileName = "dummyLog";
            IDevice logDevice = Devices.CreateLogDevice(Path.Combine(cacheDirectory, $"{logFileName}.log"),
                // Same size as a segment (see log settings below)
                capacity: 4095);
            IDevice objectLogDevice = Devices.CreateLogDevice(Path.Combine(cacheDirectory, $"{logFileName}.obj.log"),
                // Same size as a segment (see log settings below)
                capacity: 4095);
            var logSettings = new LogSettings
            {
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                PageSizeBits = 9,
                MemorySizeBits = 10,
                // Same as device capacity (see log devices above)
                SegmentSizeBits = 12
            };

            // Store
            var fasterKVStore = new FasterKV<int, string>(1L << 20, logSettings);
            var session = fasterKVStore.For(new SimpleFunctions<int, string>()).NewSession<SimpleFunctions<int, string>>();

            // Execution
            int numRecords = 1000;
            Parallel.For(0, numRecords, key => session.Upsert(key, "dummyString"));

            // Log files
            IEnumerable<string> logFiles = Directory.EnumerateFiles(cacheDirectory, $"{logFileName}.log*");
            int numLogFiles = logFiles.Count();
            Console.WriteLine($"Expected num log files: 1, actual num log files: {numLogFiles}");

            // Object log files
            IEnumerable<string> objectLogFiles = Directory.EnumerateFiles(cacheDirectory, $"{logFileName}.obj.log*");
            int numObjectLogFiles = objectLogFiles.Count();
            Console.WriteLine($"\nExpected num object log files: 1, actual num object log files: {numObjectLogFiles}");

            // Print files
            Console.WriteLine($"\nLog files:");
            foreach (string file in logFiles)
            {
                Console.WriteLine("    " + Path.GetFileName(file));
            }
            Console.WriteLine($"\nObject log files:");
            foreach (string filePath in objectLogFiles)
            {
                Console.WriteLine("    " + Path.GetFileName(filePath));
            }

            // Try reading truncated record
            Console.WriteLine($"\nReading truncated record with key 0 ...");
            (Status status, string value) = (await session.ReadAsync(0).ConfigureAwait(false)).Complete();
            Console.WriteLine($"Status: {status}, Value: {value}"); // Status should be NOTFOUND
        }

        private static void TryDeleteDirectory(string directory)
        {
            try
            {
                Directory.Delete(directory, true);
            }
            catch
            {
                // Do nothing
            }
        }
    }
}
