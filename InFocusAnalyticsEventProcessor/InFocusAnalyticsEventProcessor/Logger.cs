using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace InFocusAnalyticsEventProcessor
{
    /// <summary>
    /// This class contains code for log files.
    /// It is used throughout the application to log different messages.
    /// </summary>
    public static class Logger
    {
        public enum Severity
        {
            Info,
            Error,
            Warning,
            Exception
        }

        /// <summary>
        /// Constructor
        /// </summary>
        static Logger()
        {
            var appversion = Assembly.GetEntryAssembly().GetName().Version;
            var versionNumber = appversion.ToString().Replace(".", "_");

            //string[] args = Environment.GetCommandLineArgs();            

            string LogName = "event_processor_logs\\log.txt";
            log4net.GlobalContext.Properties["LogName"] = LogName;
            log4net.Config.XmlConfigurator.Configure();
            Log = LogManager.GetLogger(typeof(Logger));

        }

        public static ILog Log { get; set; }

        //Exceptional Error
        public static void LogException(Type type, Exception exception, [CallerMemberName] string callingMethod = "", [CallerLineNumber] int callingFileLineNumber = 0)
        {
            WriteEntry(callingMethod, Severity.Exception, exception.Message + "-" + exception.StackTrace, type);
        }

        //Info/Warning/Error
        public static void LogMessage(Type type, Severity severity, string message, [CallerMemberName] string callingMethod = "", [CallerLineNumber] int callingFileLineNumber = 0)
        {
            WriteEntry(callingMethod, severity, message, type);
        }

        //Write Logs
        private static void WriteEntry(string callingMethod, Severity severity, string message, Type type)
        {
            var finalString = Environment.NewLine + "Date - " + DateTime.Now.ToString("D") + " " + DateTime.Now.ToString("HH:mm:ss.fff tt", CultureInfo.InvariantCulture);
            finalString += Environment.NewLine + "TimeZone - " + TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now);
            finalString += Environment.NewLine + "Module - " + type.FullName;
            finalString += Environment.NewLine + "Calling Method - " + callingMethod;
            finalString += Environment.NewLine + "Message - " + message;
            finalString += Environment.NewLine + "Severity - " + severity;
            finalString += Environment.NewLine;

            Log.Debug(finalString);
        }
    }
}
