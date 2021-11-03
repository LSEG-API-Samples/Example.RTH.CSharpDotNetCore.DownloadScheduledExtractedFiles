using System;
using System.Collections.Generic;
using System.Text;

namespace DownloadScheduledExtractedFiles_dotnetcore.Types
{
    class Extraction
    {
        public string ReportExtractionId { get; set; }
        public string Status { get; set; }
        public string ExtractionDateUtc { get; set; }
    }
}
