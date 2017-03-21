using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using CraftSynth.BuildingBlocks.Common;

namespace CraftSynth.BuildingBlocks.IO.AzureStorage
{
  	public enum BlobUrlKind
	{
		Account,
		Container,
		SubfolderOrBlob
	}

	/// <summary>
	/// Example Url: https://myaccount.blob.core.windows.net/mycontainer/myBlob
	/// ProtocolName= https
	/// AccountName= myaccount.blob.core.windows.net
	/// ContainerName= mycontainer
	/// BlobName= myBlob
	/// 
	/// StorageName= myaccount
	/// Key= someExplicitlySetKey
	/// 
	/// Some rules on naming:
	/// http://msdn.microsoft.com/en-us/library/dd135715.aspx
	/// </summary>
	public class BlobUrl
	{
		public string Url;

		public BlobUrlKind Kind
		{
			get
			{
				if (this.ProtocolName != null && this.AccountName != null && this.ContainerName == null && this.BlobName == null)
				{
					return BlobUrlKind.Account;
				}
				else if (this.ProtocolName != null && this.AccountName != null && this.ContainerName != null && this.BlobName == null)
				{
					return BlobUrlKind.Container;
				}
				else if (this.ProtocolName != null && this.AccountName != null && this.ContainerName != null && this.BlobName != null)
				{
					return BlobUrlKind.SubfolderOrBlob;
				}
				return BlobUrlKind.SubfolderOrBlob;
			}
		}

		public string Key { get; set; }

		public string ConnectionString
		{
			get
			{
				string connectionString = string.Format("DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2}",
					this.ProtocolName, this.StorageName, this.Key);
				return connectionString;
			}
		}

		public string ProtocolName
		{
			get
			{
				return this.Url.Substring(0, this.Url.IndexOf(':'));
			}
		}

		public string AccountName
		{
			get
			{
				return this.Url.Replace("//", "/").Split('/')[1];
			}
		}

		public string AccountUrl
		{
			get
			{
				return string.Format("{0}://{1}", this.ProtocolName, this.AccountName);
			}
		}

		public string ContainerName
		{
			get
			{
				if (this.Url.Replace("//", "/").Split('/').Length > 2)
				{
					return this.Url.Replace("//", "/").Split('/')[2];
				}
				else
				{
					return null;
				}
			}
		}

		public string ContainerUrl
		{
			get
			{
				if (this.ContainerName == null)
				{
					return null;
				}
				else
				{
					return string.Format("{0}://{1}/{2}", this.ProtocolName, this.AccountName, this.ContainerName);
				}
			}
		}

		public string BlobName
		{
			get
			{
				if (this.Url.Replace("//", "/").Split('/').Length > 3)
				{
					return this.Url.Substring(this.Url.Replace("//", "/").IndexOfNthOccurrence("/", 3) + 2);
				}
				else
				{
					return null;
				}
			}
		}

		//public string BlobUrl
		//{
		//	get
		//	{
		//		if (this.ContainerName == null || this.BlobName==null)
		//		{
		//			return null;
		//		}
		//		else
		//		{
		//			return string.Format("{0}://{1}/{2}/{3}", this.ProtocolName, this.AccountName, this.ContainerName, this.BlobName)
		//		}
		//	}
		//}

		public string StorageName
		{
			get
			{
				return this.Url.GetSubstring("://", ".");
			}
		}


		/// <summary>
		/// Sanitizes string, validates and creates url instance. It does not check url for existance.
		/// </summary>
		/// <param name="url"></param>
		public BlobUrl(string url)
		{
			url = url.Trim().Replace(@"\", @"/").Trim('/');
			if (!url.ToLower().StartsWith("http://") && !url.ToLower().StartsWith("https://"))
			{
				url = "http://" + url;
			}
			this.Url = url;

			//validate
			this.Url = new BlobUrl(this.ProtocolName, this.AccountName, this.ContainerName, this.BlobName).Url;

		}

		/// <summary>
		/// Sanitizes string and creates url instance.
		/// </summary>
		/// <param name="protocolName"></param>
		/// <param name="accountName"></param>
		/// <param name="containerName"></param>
		/// <param name="blobName"></param>
		public BlobUrl(string protocolName, string accountName, string containerName = null, string blobName = null)
		{
			string url = string.Empty;

			protocolName = protocolName.ToNonNullString("http").Trim().ToLower().Replace(@"\", @"/").Trim('/').Trim(':');
			url = url + protocolName;

			accountName = accountName.Trim().ToLower().Replace(@"\", @"/").Trim('/');
			url = url + "://" + accountName;

			if (!string.IsNullOrEmpty(containerName))
			{
				containerName = containerName.Trim().ToLower().Replace(@"\", @"/").Trim('/');
				url = url + "/" + containerName;
			}

			if (!string.IsNullOrEmpty(blobName))
			{
				blobName = blobName.Trim().Replace(@"\", @"/").Trim('/');
				url = url + "/" + blobName;
			}

			this.Url = url;
		}

		public object Tag;
	}
}
