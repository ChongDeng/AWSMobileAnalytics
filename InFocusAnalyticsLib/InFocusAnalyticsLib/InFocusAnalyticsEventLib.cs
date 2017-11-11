using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

using System.Xml;
using log4net;
using System.Threading;
using System.Diagnostics;
using System.Text;

namespace InFocusAnalyticsLib
{
    public class InFocusAnalyticsEventLib
    {
        public static string ProductionPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\AnalyticsProduction\\";
        public static string StagingPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\AnalyticsStaging\\";

        public static string ProductionFolderConfigFile = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\ProductionFolderConfig.xml";

        public static string TrimThreadLogFile = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\TrimThreadLog.txt";

        private static ILog LogUtils = null;

        private static bool AllowRecordEvent = true;
        public static bool isRecordEventAllowed()
        {
            return AllowRecordEvent;
        }

        //unit is byte; default value is 4MB
        private static long DefalutProductionFolderSize = 4 * 1024 * 1024;
        private static long ProductionFolderSizeThreshold = 0;

        //unit is minute; default value is 24 hours
        private static long DefaultEventFileExpirationDuration = 24 * 60;
        private static long EventFileExpirationDuration = 0;

        private static Object ProductionFolderLock = new Object();

        // Create the named mutex. Only one system object named 
        // "InFocusProductionFolderMutex" can exist; the local Mutex object represents 
        // this system object, regardless of which process or thread
        // caused "InFocusProductionFolderMutex" to be created.
        public static Mutex ProductionFolderMutex = new Mutex(false, "InFocusProductionFolderMutex");
        private static Thread th;
        private static ManualResetEvent UnInitEvent = new ManualResetEvent(false);

        public static Mutex InFocusEventProcessorMutex = new Mutex(false, "InFocusEventProcessorMutex");

        public static void UnInit()
        {
            UnInitEvent.Set();
        }

        public static bool init(ILog Logger = null)
        {
            if (Logger != null)
            {
                LogUtils = Logger;
            }

            if (!CheckEventMessageFolder(StagingPath))
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to check event message file folder: " + StagingPath);
                }
                return false;
            }

            if (!CheckEventMessageFolder(ProductionPath))
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to check event message file folder: " + ProductionPath);
                }
                return false;
            }

            if (!SetProductionFolderTrimThread())
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to set production folder trimming thread");
                }
                return false;
            }

            if (!LaunchEventProcessor())
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to launch EventProcessor to send Analytics Events");
                }
                return false;
            }

            return true;
        }

        private static bool SetProductionFolderTrimThread()
        {
            try
            {
                if (ProductionFolderMutex.WaitOne(0))
                {
                    ProductionFolderMutex.ReleaseMutex();

                    th = new Thread(() =>
                    {
                        try
                        {
                            WriteTrimThreadLog("ProductionFolderTrimThread is running in the process with pid - " + Process.GetCurrentProcess().Id);
                            FolderTrimThreadFunc(InFocusAnalyticsEventLib.ProductionFolderMutex);
                        }
                        catch (Exception ex)
                        {
                            LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "FolderTrimThreadFunc Exception - " + ex);
                        }
                    });
                    th.Start();

                    if (LogUtils != null)
                    {
                        LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Launched a thread for production folder trim!");
                    }
                }
            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Can't launch thread for production folder trim! Exception: " + ex);
                }
                return false;
            }

            return true;
        }

        private static void FolderTrimThreadFunc(Mutex ProductionFolderMutex)
        {
            if (((Mutex)ProductionFolderMutex).WaitOne(0))
            {
                try
                {
                    while (true)
                    {
                        if (!TrimProductionFolder())
                        {
                            if (LogUtils != null)
                            {
                                LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to trim production folder.");
                            }

                            InFocusAnalyticsEventLib.th = null;
                            return;
                        }

                        if (true == InFocusAnalyticsEventLib.UnInitEvent.WaitOne(TimeSpan.FromMinutes((int)getEventFileExpirationDuration())))
                        {
                            return;
                        }
                    }
                }
                finally
                {
                    ((Mutex)ProductionFolderMutex).ReleaseMutex();
                    WriteTrimThreadLog("ProductionFolderTrimThread is gone.");
                }
            }
        }


        private static bool TrimProductionFolder()
        {
            //Console.WriteLine("begin to trim...");

            try
            {
                long ProductionFolderSizeLimit = getProductionFolderSizeThreshold();

                long ProductionFolderSize = getProductionFolderSize();

                if (ProductionFolderSize > ProductionFolderSizeLimit)
                {
                    DeleteOldFiles();
                }
            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to trim old files in producion folder " + ProductionPath + "! Exception: " + ex);
                }
                return false;
            }

            return true;
        }

        private static void DeleteOldFiles()
        {
            DateTime CurrentTime = DateTime.Now;
            DateTime EventExpirationTime = CurrentTime.AddMinutes(-1 * getEventFileExpirationDuration());

            //Console.WriteLine("current time: " + CurrentTime);
            //Console.WriteLine("deadline time: " + EventExpirationTime);

            List<FileInfo> FileInfosToDelete = new DirectoryInfo(ProductionPath).GetFiles()
                                                   .OrderBy(x => x.LastWriteTime).ToList();

            FileInfosToDelete.RemoveAll(s => (DateTime.Compare(s.LastWriteTime, EventExpirationTime) > 0));

            foreach (FileInfo fi in FileInfosToDelete)
            {
                //Console.WriteLine("Deleted the old file " + fi.FullName);
                File.Delete(fi.FullName);
                if (LogUtils != null)
                {
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Deleted the old file " + fi.FullName);
                }
            }
        }


        private static void WriteTrimThreadLog(String LogCont)
        {
            if (!File.Exists(TrimThreadLogFile))
            {
                File.Create(TrimThreadLogFile).Dispose();
            }

            using (StreamWriter tw = new StreamWriter(TrimThreadLogFile, true))
            {
                tw.WriteLine(DateTime.Now + " - " + LogCont);
                tw.Close();
            }
        }

        //check whether the folder that stores event message files exists or not
        public static bool CheckEventMessageFolder(String FolderPath)
        {
            try
            {
                if (!Directory.Exists(FolderPath))
                {
                    Directory.CreateDirectory(FolderPath);
                    if (LogUtils != null)
                    {
                        LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Event file dir path " + FolderPath + "not existed! it is created now!");
                    }
                }
            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to check event message file folder: " + FolderPath + " , exception: " + ex);
                }
                return false;
            }

            return true;
        }

        public static bool LaunchEventProcessor()
        {
            if (InFocusEventProcessorMutex.WaitOne(0))
            {
                try
                {
                    Process[] ProcessNames = Process.GetProcessesByName("InFocusAnalyticsEventProcessor");
                    if (ProcessNames.Length == 0)
                    {
                        string EventProcessorExe = "InFocusEventProcessor\\InFocusAnalyticsEventProcessor";
                        string EnvZoos = Environment.GetEnvironmentVariable("ZOOS");
                        if (EnvZoos != null)
                        {
                            EventProcessorExe = EnvZoos + "\\" + EventProcessorExe;
                        }

                        Process EventProcessor = Process.Start(EventProcessorExe);
                        if (EventProcessor == null)
                        {
                            if (LogUtils != null)
                            {
                                LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to start event processor!");
                            }
                            return false;
                        }

                        if (LogUtils != null)
                        {
                            LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "InFocusAnalyticsEventProcessor is launched here now!");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (LogUtils != null)
                    {
                        LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Can't launch InFocusAnalyticsEventProcessor! Exception - " + ex);
                    }
                    return false;
                }
                finally
                {
                    InFocusEventProcessorMutex.ReleaseMutex();
                }
            }

            return true;
        }

        //unit test
        public static void TestRecordEvent(String EventName)
        {

            string xml = @"<log4net>
                            <root>
                              <appender-ref ref='FileAppender' />
                              <!--<appender-ref ref='UdpAppender'/>-->
                            </root>
                            <appender name='UdpAppender' type='log4net.Appender.UdpAppender'>
                              <threshold value='ALL' />
                              <remoteAddress value='255.255.255.255' />
                              <remotePort value='8008' />
                              <layout type='log4net.Layout.XmlLayoutSchemaLog4j, log4net'>
                                <locationInfo value='true' />
                              </layout>
                            </appender>
                            <appender name='FileAppender' type='log4net.Appender.RollingFileAppender'>
                              <threshold value='INFO' />
                              <file value='logs\log.txt' />
                              <appendToFile value='true' />
                              <maximumFileSize value='10240KB' />
                              <maxSizeRollBackups value='29' />
                              <param name='StaticLogFileName' value='true'/>
                              <param name='RollingStyle' value='Size' />
                              <layout type='log4net.Layout.PatternLayout'>
                                <conversionPattern value='%date [%thread] %-5level %logger %ndc - %message%newline' />
                              </layout>
                            </appender>
                          </log4net>";

            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xml);
            log4net.Config.XmlConfigurator.Configure(doc.DocumentElement);
            ILog LogUtils_2 = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

            //SetupLogger(LogUtils_2);

            if (LogUtils != null)
            {
                LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Begin to Record Event: " + EventName);
            }

            InFocusAnalyticsEvent AWSCustomEvent = new InFocusAnalyticsEvent();

            AWSCustomEvent.EventName = EventName;

            AWSCustomEvent.AttributeMap = new Dictionary<String, String>();
            AWSCustomEvent.AttributeMap.Add("RoomName", "8942");
            AWSCustomEvent.AttributeMap.Add("Test", "2026");
            AWSCustomEvent.AttributeMap.Add("StartTime", "20:02");
            AWSCustomEvent.AttributeMap.Add("EndTime", "20:22");
            AWSCustomEvent.AttributeMap.Add("Duration", "20");
            AWSCustomEvent.AttributeMap.Add("NewlyAdded", "10082");

            AWSCustomEvent.MetricMap = new Dictionary<String, Double>();
            AWSCustomEvent.MetricMap.Add("HistoryDataSize", 2002);

            if (!RecordAnalyticsEvent(AWSCustomEvent))
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to Record Event!");
                }
            }
        }

        //serialize AWSAnalyticsEvent object to record the AWS Event to the local persistent system 
        public static bool RecordAnalyticsEvent(InFocusAnalyticsEvent EventBean)
        {
            if (!isRecordEventAllowed())
            {
                if (LogUtils != null)
                {
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Record Event is not allowed, record events failed!");
                }
                return false;
            }

            //check whether ClientName and EventName of AWSAnalyticsEvent has been set or not.
            if (EventBean.EventName == null)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Record Event exception: EventName has not been set for this AWS Event!");
                }

                return false;
            }

            EventBean.EventTime = DateTime.UtcNow.ToString("yyyy-MM-dd-HH:mm:ss");

            String RecordFile = Guid.NewGuid().ToString();

            try
            {
                //step1: serialize AWSAnalyticsEvent object to string
                MemoryStream stream = new MemoryStream();
                DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(InFocusAnalyticsEvent));
                ser.WriteObject(stream, EventBean);

                stream.Position = 0;
                StreamReader sr = new StreamReader(stream);
                String AWSAnalyticsEventData = sr.ReadToEnd();
                stream.Close();
                stream.Dispose();

                //Console.Write("JSON form of Person object: ");
                //Console.WriteLine(AWSAnalyticsEventData);

                if (LogUtils != null)
                {
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Begin to write Event message file " + RecordFile + " to staging folder.");
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "File Content is: " + AWSAnalyticsEventData);
                }

                //step2: write event data to event message file in the staging folder.               
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(StagingPath + RecordFile, true))
                {
                    file.WriteLine(AWSAnalyticsEventData);
                }

                if (LogUtils != null)
                {
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Begin to move Event message file " + RecordFile + " from staging folder to production folder.");
                }
                //step3: move event message file from the staging folder to product folder.                
                System.IO.File.Move(StagingPath + RecordFile, ProductionPath + RecordFile);
                if (LogUtils != null)
                {
                    LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Successfully moving Event message file " + RecordFile + " from staging folder to production folder.\r\n");
                }

            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to record Analytics Event into file " + RecordFile + "! Exception: " + ex);
                }

                return false;
            }

            if (th != null && !th.IsAlive)
            {
                if (!SetProductionFolderTrimThread())
                {
                    if (LogUtils != null)
                    {
                        LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "SetProductionFolderTrimThread Failed.");
                    }
                }
            }

            return true;
        }

        public static long getProductionFolderSize()
        {
            long Size = 0;

            DirectoryInfo dir = new DirectoryInfo(ProductionPath);
            // Add file sizes.
            FileInfo[] fis = dir.GetFiles();
            foreach (FileInfo fi in fis)
            {
                Size += fi.Length;
            }

            //Console.WriteLine("get the production folder size: " + Size);
            if (LogUtils != null)
            {
                LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "get the production folder size: " + Size);
            }

            return Size;
        }

        private static long getProductionFolderSizeThreshold()
        {
            try
            {
                if (!File.Exists(ProductionFolderConfigFile))
                {
                    if (LogUtils != null)
                    {
                        LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + ProductionFolderConfigFile + " does not exist! Now use default configuration value!");
                    }
                    ProductionFolderSizeThreshold = DefalutProductionFolderSize;
                }
                else
                {
                    XmlDocument doc = new XmlDocument();
                    doc.Load(ProductionFolderConfigFile);

                    XmlNode ProductionFolderSizeThresholdNode = doc.DocumentElement.SelectSingleNode("/config/ProductionFolderSizeThreshold");
                    if (ProductionFolderSizeThresholdNode == null)
                    {
                        if (LogUtils != null)
                        {
                            LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "No ProductionFolderSizeThreshold in " + ProductionFolderConfigFile + ". Now use default configuration value!");
                        }
                        ProductionFolderSizeThreshold = DefalutProductionFolderSize;
                    }
                    else
                    {
                        ProductionFolderSizeThreshold = Convert.ToInt64(ProductionFolderSizeThresholdNode.InnerText.Trim());
                        if (ProductionFolderSizeThreshold == 0)
                        {
                            ProductionFolderSizeThreshold = DefalutProductionFolderSize;
                        }

                        if (LogUtils != null)
                        {
                            LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Now, get the production folder size threshold: " + ProductionFolderSizeThreshold);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to set production folder size threshold config and then use default value!. Exception: " + ex);
                }
                ProductionFolderSizeThreshold = DefalutProductionFolderSize;
            }

            //Console.WriteLine("get the ProductionFolderSizeThreshold: " + ProductionFolderSizeThreshold);
            return ProductionFolderSizeThreshold;
        }

        private static long getEventFileExpirationDuration()
        {
            try
            {
                if (!File.Exists(ProductionFolderConfigFile))
                {
                    if (LogUtils != null)
                    {
                        LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + ProductionFolderConfigFile + " does not exist! Now use default configuration value!");
                    }
                    EventFileExpirationDuration = DefaultEventFileExpirationDuration;
                }
                else
                {
                    XmlDocument doc = new XmlDocument();
                    doc.Load(ProductionFolderConfigFile);

                    XmlNode EventFileExpirationDurationNode = doc.DocumentElement.SelectSingleNode("/config/EventFileExpirationDuration");
                    if (EventFileExpirationDurationNode == null)
                    {
                        if (LogUtils != null)
                        {
                            LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "No EventFileExpirationDuration in " + ProductionFolderConfigFile + ". Now use default configuration value!");
                        }
                        EventFileExpirationDuration = DefaultEventFileExpirationDuration;
                    }
                    else
                    {

                        EventFileExpirationDuration = Convert.ToInt64(EventFileExpirationDurationNode.InnerText.Trim());
                        if (EventFileExpirationDuration == 0)
                        {
                            EventFileExpirationDuration = DefaultEventFileExpirationDuration;
                        }

                        if (LogUtils != null)
                        {
                            LogUtils.Info(typeof(InFocusAnalyticsEventLib) + ":   " + "Now, get the EventFileExpirationDuration: " + EventFileExpirationDuration);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (LogUtils != null)
                {
                    LogUtils.Error(typeof(InFocusAnalyticsEventLib) + ":   " + "Failed to set EventFileExpirationDuration and then use default value! Exception: " + ex);
                }
                EventFileExpirationDuration = DefaultEventFileExpirationDuration;
            }

            //Console.WriteLine("get the EventFileExpirationDuration: " + EventFileExpirationDuration);
            return EventFileExpirationDuration;
        }
    }

    [DataContract]
    public class InFocusAnalyticsEvent
    {
        [DataMember]
        public String EventName;

        [DataMember]
        public String EventTime;

        [DataMember]
        public Dictionary<String, String> AttributeMap;

        [DataMember]
        public Dictionary<String, Double> MetricMap;
    }

}
