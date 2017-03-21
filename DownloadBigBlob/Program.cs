using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
//using Microsoft.WindowsAzure;
using System.Threading;
using CraftSynth.BuildingBlocks.Common;
using CraftSynth.BuildingBlocks.IO.AzureStorage;
using CraftSynth.BuildingBlocks.Logging;
using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.StorageClient;
 using Microsoft.WindowsAzure.Storage.Blob;

namespace DownloadBigBlob
{
	class Program
	{
		private static CustomTraceLog log;
		private static long attempt = 1;
		private static Timer timeoutTimer;
		static void Main(string[] args)
		{
			
			log = new CustomTraceLog("Starting..............................................", true, false, CustomTraceLogAddLinePostProcessingEvent, null);

			bool retryOnError = false;
			bool pauseAtEnd = false;
			while (true)
			{
				try
				{
					Console.Clear();
					log.AddLine("Attempt:"+attempt);
					log.AddLineAndIncreaseIdent("Reading .ini file...");
					
						string accountKey = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<string>("accountKey", null, true,null, true, null, false, null, '=');
						log.AddLine("accountKey:"+accountKey.FirstXChars(20,"...hidden"));

						string blobUrl = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<string>("blobUrl", null, true, null,true, null, false, null, '=');
						log.AddLine("blobUrl:"+blobUrl);
					
						string blobIsBlockOrPage = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<string>("blobIsBlockOrPage", null, true, null, true, null, false, null, '=');
						log.AddLine("blobIsBlockOrPage:" + blobIsBlockOrPage);
					
						string localFilePath = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<string>("localFilePath", null,true, null, true, null, false, null, '=');
						log.AddLine("localFilePath:" + localFilePath);
					
						retryOnError = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<bool>("retryOnError", null, true,false, true, false, false, false, '=');
						log.AddLine("retryOnError:" + retryOnError);
					
						pauseAtEnd = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<bool>("pauseAtEnd", null, true,false, true, false, false, false, '=');
						log.AddLine("pauseAtEnd:" + pauseAtEnd);

						int delayBeetweenChunksInSeconds = CraftSynth.BuildingBlocks.IO.FileSystem.GetSettingFromIniFile<int>("delayBeetweenChunksInSeconds", null, true, 0, false, 0, false, 0, '=');
						log.AddLine("delayBeetweenChunksInSeconds:" + delayBeetweenChunksInSeconds);

					log.AddLineAndDecreaseIdent("Done.");

					if (blobIsBlockOrPage != "Page")
					{
						throw new Exception("blobIsBlockOrPage can be only 'Page'. Other case is not implemented yet.");
					}

					log.AddLineAndIncreaseIdent("Parsing blob url...");
					CraftSynth.BuildingBlocks.IO.AzureStorage.BlobUrl blobUrlClass = new BlobUrl(blobUrl);
						string accountName = blobUrlClass.StorageName;
						log.AddLine("accountName:"+accountName);

						var containerName = blobUrlClass.ContainerName;
						log.AddLine("containerName:"+containerName);

						var blobName = blobUrlClass.BlobName;
						log.AddLine("blobName:"+blobName);

					log.AddLineAndDecreaseIdent("Done.");

					int segmentSize = 1*1024*1024; //1 MB chunk

					log.AddLineAndIncreaseIdent("Downloading...");
					Download(accountName, accountKey, containerName, blobName, localFilePath, segmentSize, delayBeetweenChunksInSeconds);
					log.AddLine("Downloading done.");

				
					break;
				}
				catch (Exception e)
				{
					Exception de = CraftSynth.BuildingBlocks.Common.Misc.GetDeepestException(e);
					log.AddLine(de.Message);

					if (retryOnError)
					{
						log.AddLine("Retrying in 10 seconds...");
						Thread.Sleep(10000);
						attempt++;
					}
					else
					{
						break;
					}
				}	
			}	
			
			if (pauseAtEnd)
			{
				Console.WriteLine("Press any key to exit...");
				Console.ReadKey();
			}
		}

		private static void CustomTraceLogAddLinePostProcessingEvent(CustomTraceLog sender, string line, bool inNewLine, int level, string lineVersionSuitableForLineEnding, string lineVersionSuitableForNewLine)
		{
			Console.WriteLine(line);
			CraftSynth.BuildingBlocks.Logging.Misc.AddTimestampedLineToApplicationWideLog(line, true, null, false);
		}

		private static void Download(string accountName, string accountKey, string containerName, string blobName, string localFilePath, int segmentSize, int delayBeetweenChunksInSeconds)
		{
			var cloudStorageAccount =CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey);
			var blobContainer = cloudStorageAccount.CreateCloudBlobClient().GetContainerReference(containerName);
			CloudPageBlob blob = blobContainer.GetPageBlobReference(blobName);
			blob.FetchAttributes();

			long startPosition = 0;

			if (File.Exists(localFilePath))
			{
				log.AddLine("Local file exists. Resuming download...");
				using (FileStream fs = new FileStream(localFilePath, FileMode.Open))
				{
					fs.Seek(0, SeekOrigin.End);
					startPosition = fs.Position;
				}
			}

			long blobLength = blob.Properties.Length;
			var blobLengthRemaining = blobLength - startPosition;

			DateTime momentBeforeRead = DateTime.Now;

			double percentDone = 0;
			TimeSpan timeForOneRead = new TimeSpan(0);
			double speedBytesPerSecond = 0;
			TimeSpan timeRemaining = new TimeSpan(0);
			int secondsRemaining = 0;

			timeoutTimer = new Timer(Timer_Tick, null, Timeout.Infinite, Timeout.Infinite);
			while (blobLengthRemaining > 0)
			{
				long blockSize = Math.Min(segmentSize, blobLengthRemaining);
				byte[] blobContents = new byte[blockSize];
				using (MemoryStream ms = new MemoryStream())
				{
					timeoutTimer.Change(new TimeSpan(0, 0, 10), new TimeSpan(0, 0, 10));
					blob.DownloadRangeToStream(ms, startPosition, blockSize);
					timeoutTimer.Change(Timeout.Infinite, Timeout.Infinite);

					ms.Position = 0;
					ms.Read(blobContents, 0, blobContents.Length);
					using (FileStream fs = new FileStream(localFilePath, FileMode.OpenOrCreate))
					{
						fs.Position = startPosition;
						fs.Write(blobContents, 0, blobContents.Length);
					}
				}
				startPosition += blockSize;
				blobLengthRemaining -= blockSize;

				//blobLength:100=startPosition:x
				percentDone = startPosition*100/blobLength;
				timeForOneRead = DateTime.Now.Subtract(momentBeforeRead);
				momentBeforeRead = DateTime.Now;
				speedBytesPerSecond = blockSize/timeForOneRead.TotalSeconds;
				secondsRemaining = (int)Math.Round(blobLengthRemaining/speedBytesPerSecond);
				timeRemaining = new TimeSpan(0,0,secondsRemaining);

				Console.SetCursorPosition(0,19);
				Console.WriteLine("                                                                                                                                                             ");
				Console.SetCursorPosition(0,19);
				log.AddLine(string.Format("{0}%, {1}/{2} bytes, {3} kbytes/sec, remaining (dd:hh:mm:ss): {4}:{5}:{6}:{7}",
					percentDone,
					startPosition,
					blobLength,
					Math.Round(speedBytesPerSecond/1024,2),
					timeRemaining.Days,
					timeRemaining.Hours,
					timeRemaining.Minutes,
					timeRemaining.Seconds
					)
					);


				attempt = 1;

				if (delayBeetweenChunksInSeconds > 0)
				{
					Thread.Sleep(delayBeetweenChunksInSeconds);
				}
			}
		}

		private static void Timer_Tick(object state)
		{
			log.AddLine("Timeout happened.");
			timeoutTimer.Change(Timeout.Infinite, Timeout.Infinite);
			timeoutTimer = null;

			log.AddLine("Retrying in 10 seconds...");
			Thread.Sleep(10000);
			attempt++;
			Main( new string[]{});
		}
	}
}
