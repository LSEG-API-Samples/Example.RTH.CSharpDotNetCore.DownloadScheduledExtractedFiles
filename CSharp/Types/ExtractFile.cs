using System;
using System.Collections.Generic;
using System.Text;

namespace DownloadScheduledExtractedFiles_dotnetcore.Types
{
    class ExtractFile
    {
        public string ExtractedFileId { get; set; }
        public string ExtractedFileName { get; set; }
        public uint Size { get; set; }
        public string FileType { get; set; }
    }
}
