using System;
using System.IO;
using CommandLineParser.Arguments;
using CommandLineParser.Exceptions;
using DownloadScheduledExtractedFiles_dotnetcore.Types;

namespace DownloadScheduledExtractedFiles_dotnetcore
{
    class Program
    {
        DSSClient dssClient = new DSSClient();
        private CommandLineParser.CommandLineParser cmdParser = new CommandLineParser.CommandLineParser()
        { IgnoreCase = true };
        ValueArgument<string> dssUserName = new ValueArgument<string>('u', "username", "DSS Username")
        { Optional = false };
        ValueArgument<string> dssPassword = new ValueArgument<string>('p', "password", "DSS Password")
        { Optional = false };
        ValueArgument<string> scheduleName = new ValueArgument<string>('s', "schedulename", "A schedule name")
        { Optional = false };
        EnumeratedValueArgument<string> fileType = new EnumeratedValueArgument<string>('f', "file", "Type of files (all, note, ric, data)", new string[] { "all", "note", "ric", "data" })
        { IgnoreCase = true, Optional = true, DefaultValue = "all" };

        SwitchArgument awsFlag = new SwitchArgument('x', "aws", "Set whether show or not", false)
        { Optional = true };
        static void Main(string[] args)
        {
            Program prog = new Program();
            if (prog.Init(ref args))
            {
                try
                {
                    prog.Run();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        public Program()
        {
            cmdParser.Arguments.Add(dssUserName);
            cmdParser.Arguments.Add(dssPassword);
            cmdParser.Arguments.Add(scheduleName);
            cmdParser.Arguments.Add(fileType);
            cmdParser.Arguments.Add(awsFlag);

        }
        public bool Init(ref string[] args)
        {
            if (args.Length == 0)
            {
                cmdParser.ShowUsage();
                return false;
            }

            try
            {
                cmdParser.ParseCommandLine(args);

                if (!cmdParser.ParsingSucceeded)
                {
                    cmdParser.ShowUsage();
                    return false;
                }


            }
            catch (CommandLineException e)
            {
                Console.WriteLine(e.Message);
                cmdParser.ShowUsage();
                return false;
            }

            Console.WriteLine($"Download the latest {fileType.Value} extraction file(s) from the schedule {scheduleName.Value}\n");
            return true;
        }
        public void Run()
        {
            dssClient.Login(dssUserName.Value, dssPassword.Value);

            if (string.IsNullOrEmpty(scheduleName.Value.Trim()))
            {
                dssClient.ListAllSchedules();
                return;
            }
            Schedule schedule = dssClient.GetScheduleByName(scheduleName.Value);

            Console.WriteLine($"\nThe schedule ID of {schedule.ScheduleName} is {schedule.ScheduleId} ({schedule.Trigger}).");

            Extraction extraction = dssClient.GetLastExtraction(schedule);

            Console.WriteLine($"\nThe last extraction was extracted on {extraction.ExtractionDateUtc} GMT");

            if(fileType.Value == "all")
            {
                var fileList = dssClient.GetAllFiles(extraction);
                if(fileList.Count == 0)
                {
                    Console.WriteLine($"\nNo file for this extraction {extraction.ReportExtractionId} in this schedule {schedule.ScheduleName}");
                    return;
                }
                foreach(var file in fileList)
                {
                    Console.WriteLine($"\n{file.ExtractedFileName} ({file.Size} bytes) is available on the server.");
                    dssClient.DownloadFile(file, awsFlag.Value);
                }
            }
            else
            {
                var file = dssClient.GetFile(extraction, fileType.Value);
                Console.WriteLine($"\n{file.ExtractedFileName} ({file.Size} bytes) is available on the server.");
                dssClient.DownloadFile(file, awsFlag.Value);

            }

        }
    }
}
