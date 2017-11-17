using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon.MobileAnalytics.MobileAnalyticsManager;
using Amazon.CognitoIdentity;
using System.Threading;
using log4net;
using System.Xml;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Windows.Forms;
using System.Runtime.Serialization;

namespace InFocusAnalyticsEventProcessor
{
    class Program
    {
        private static string ConfigXmlFile = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\Config.xml";

        public static string ProductionPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\AnalyticsProduction\\";
        public static string StagingPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\Infocus\\AnalyticsStaging\\";

        private static MobileAnalyticsManager _manager = null;

        private static string ApplicationName = null;
        private static string AppId = null;
        private static string CognitoPoolID = null;

        private static string DefaultApplicationName = "ConX_WBC";
        private static string DefaultAppId = "XXXXXX";
        private static string DefaultCognitoPoolID = "us-east-1:YYYYY";

        private static Object EventFileLock = new Object();

        private static FileSystemWatcher watcher;

        static void Main(string[] args)
        {
            //initialize mobile analytics manager and configuration such as the local persistece db size
            if (!init())
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                    "Failed to initialize analytics manager! Now process is exiting!");

                return;
            }
            //read event message file, then parse and submit AWS events
            try
            {
                StartEventProcess();
            }
            catch (Exception ex)
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                       " Failed to invoke StartEventProcess()! Exception: " + ex);
            }


            Application.Run();
        }

        private static bool ReadCofig()
        {
            try
            {
                if (!File.Exists(ConfigXmlFile))
                {
                    Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                       ConfigXmlFile + " does not exist! Now use default configuration value!");

                    ApplicationName = DefaultApplicationName;
                    AppId = DefaultAppId;
                    CognitoPoolID = DefaultCognitoPoolID;
                }
                else
                {
                    XmlDocument doc = new XmlDocument();
                    doc.Load(ConfigXmlFile);

                    XmlNode ApplicationNameNode = doc.DocumentElement.SelectSingleNode("/config/ApplicationName");
                    if (ApplicationNameNode == null)
                    {
                        Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                           "No ApplicationName in " + ConfigXmlFile + " Now use default configuration value!");

                        ApplicationName = DefaultApplicationName;

                    }
                    ApplicationName = ApplicationNameNode.InnerText.Trim();

                    XmlNode AppIdNode = doc.DocumentElement.SelectSingleNode("/config/AppId");
                    if (AppIdNode == null)
                    {
                        Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                           "No AppId in " + ConfigXmlFile + " Now use default configuration value!");

                        AppId = DefaultAppId;
                    }
                    AppId = AppIdNode.InnerText.Trim();


                    XmlNode CognitoPoolIDNode = doc.DocumentElement.SelectSingleNode("/config/CognitoPoolID");
                    if (CognitoPoolIDNode == null)
                    {
                        Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                           "No CognitoPoolID in " + ConfigXmlFile + " Now use default configuration value!");

                        CognitoPoolID = DefaultCognitoPoolID;
                    }
                    CognitoPoolID = CognitoPoolIDNode.InnerText.Trim();
                }

            }
            catch (Exception ex)
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                       "Exception occured during reading " + ConfigXmlFile + ", exception: " + ex + ". Now use default configuration value");

                return false;
            }

            return true;
        }

        public static bool init()
        {
            bool ret = ReadCofig();
            if (ret == false)
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                       "Failed to read config file " + ConfigXmlFile);
                return false;
            }

            Amazon.AWSConfigs.ApplicationName = ApplicationName;

            MobileAnalyticsManagerConfig config = new MobileAnalyticsManagerConfig();
            config.AllowUseDataNetwork = true;
            //config.DBWarningThreshold = 0.9f;
            config.MaxDBSize = 5242880 * 2;
            //config.MaxRequestSize = 102400;
            //config.SessionTimeout = 5;

            config.ClientContextConfiguration.Platform = "Windows";

            try
            {
                _manager = MobileAnalyticsManager.GetOrCreateInstance(AppId, //Amazon Mobile Analytics App ID
                                                      new CognitoAWSCredentials(CognitoPoolID, //Amazon Cognito Identity Pool ID
                                                                                Amazon.RegionEndpoint.USEast1),
                                                      Amazon.RegionEndpoint.USEast1, config);
            }
            catch (Exception ex)
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                       "Failed to initialize MobileAnalyticsManager instance! Exception: " + ex);
                return false;
            }

            return true;

        }

        //read stored event message files, then parse and submit AWS events
        public static void StartEventProcess()
        {
            Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                       "Start to process event message files to send event.");

            /* step1: set file system watcher: when a event message file is created later, the function ParseAndSendAWSAnalyticsEvent 
            *        will be automatically invoked to submit AWS event sending.
            * */
            watcher = new FileSystemWatcher();
            SetFielsystemWatcher(watcher);

            lock (EventFileLock)
            {
                //step2: iterate folder to parse all the event message file for AWS event sending
                string[] EventMsgFiles = Directory.GetFiles(ProductionPath);
                foreach (String EventMsgFile in EventMsgFiles)
                {
                    Console.WriteLine("Current thread id: " + Thread.CurrentThread.ManagedThreadId.ToString() + ", Found event file:  " + EventMsgFile);
                    Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                       "Current thread id: " + Thread.CurrentThread.ManagedThreadId.ToString() + ", Found event file:  " + EventMsgFile);

                    if (!ParseAndSendAnalyticsEvent(EventMsgFile))
                    {
                        Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                            "Failed to parse and send event from file " + EventMsgFile);
                    }
                }
            }
        }

        private static void SetFielsystemWatcher(FileSystemWatcher watcher)
        {

            watcher.Path = ProductionPath;


            watcher.Created += new FileSystemEventHandler(OnProcess);
            watcher.Error += new ErrorEventHandler(OnError);
            watcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size;

            watcher.EnableRaisingEvents = true;
        }

        private static void OnProcess(object source, FileSystemEventArgs e)
        {
            if (e.ChangeType == WatcherChangeTypes.Created)
            {
                OnCreated(source, e);
            }
        }

        private static void OnError(object source, ErrorEventArgs e)
        {
            Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                            "Failed to set system watcher! Here will set again!");

            watcher.Created -= new FileSystemEventHandler(OnProcess);

            FileSystemWatcher fileSystemWatcher = new FileSystemWatcher();
            SetFielsystemWatcher(fileSystemWatcher);
        }

        /* when a event message file is created later, the function OnCreated 
        *        will be automatically invoked to record and submit AWS event sending.
        */
        private static void OnCreated(object source, FileSystemEventArgs e)
        {
            lock (EventFileLock)
            {
                if (File.Exists(e.FullPath))
                {
                    String NewFile = e.FullPath;
                    Console.WriteLine("Current thread id: " + Thread.CurrentThread.ManagedThreadId.ToString() + ", detected newly created event file:  " + NewFile);
                    Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                            "Current thread id: " + Thread.CurrentThread.ManagedThreadId.ToString() + ", detected newly created event file:  " + NewFile);


                    if (!ParseAndSendAnalyticsEvent(NewFile))
                    {
                        Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                            "Failed to parse and send event from file " + NewFile);
                    }
                }
            }
        }


        private static bool ParseAndSendAnalyticsEvent(String EventDataFile)
        {
            Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                            "Begin to parse event message file: " + EventDataFile);

            try
            {
                //step1: read event data content from event message file
                StreamReader sr = new StreamReader(EventDataFile);
                Byte[] AWSAnalyticsEventData = Encoding.UTF8.GetBytes(sr.ReadLine());
                sr.Close();
                sr.Dispose();

                //step2: deserialize the event data content to AWSAnalyticsEvent object  
                DataContractJsonSerializer js = new DataContractJsonSerializer(typeof(InFocusAnalyticsEvent));
                MemoryStream stream = new MemoryStream(AWSAnalyticsEventData);
                InFocusAnalyticsEvent EventBean = (InFocusAnalyticsEvent)js.ReadObject(stream);
                stream.Close();
                stream.Dispose();

                Console.WriteLine("get CustomEvent: " + EventBean.EventName);
                Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                              "get CustomEvent: EventName is " + EventBean.EventName);

                //step3: initialize CustomEvent object from AWSAnalyticsEvent object
                CustomEvent customEvent = new CustomEvent(EventBean.EventName);

                customEvent.AddAttribute("EventTime", EventBean.EventTime);

                if (EventBean.AttributeMap != null)
                {
                    foreach (KeyValuePair<string, string> attribute in EventBean.AttributeMap)
                    {
                        customEvent.AddAttribute(attribute.Key, attribute.Value);
                    }
                }

                if (EventBean.MetricMap != null)
                {
                    foreach (KeyValuePair<string, Double> attribute in EventBean.MetricMap)
                    {
                        customEvent.AddMetric(attribute.Key, attribute.Value);
                    }
                }                

                //step4: record event which will be automatically submitted later.
                _manager.RecordEvent(customEvent);
                Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                          "Now, a background thread will send AWS Events later!");

                //step5: delete this used event message file
                System.IO.File.Delete(EventDataFile);
                Logger.LogMessage(typeof(Program), Logger.Severity.Info,
                          "Now, deleted event message file " + EventDataFile);

            }
            catch (Exception ex)
            {
                Logger.LogMessage(typeof(Program), Logger.Severity.Error,
                          "Failed to Read and Parse AWS Analytics Event Data from file " + EventDataFile + "!. Exception:" + ex);

                return false;
            }

            return true;
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
