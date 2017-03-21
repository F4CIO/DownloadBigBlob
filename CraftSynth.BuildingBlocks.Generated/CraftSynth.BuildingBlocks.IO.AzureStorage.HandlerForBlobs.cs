using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;

namespace CraftSynth.BuildingBlocks.IO.AzureStorage
{
	public class HandlerForBlobs
	{
		#region Private Members
		private CloudBlobClient _currentClient;
		#endregion

		#region Properties

		#endregion

		#region Public Methods
		public bool Exists(CloudBlob blob)
		{
			try
			{
				blob.FetchAttributes();
				return true;
			}
			catch (StorageClientException e)
			{
				if (e.ErrorCode == StorageErrorCode.ResourceNotFound)
				{
					return false;
				}
				else
				{
					throw;
				}
			}
		}

		public List<BlobUrl> GetChildren(BlobUrl parentUrl, bool onlyTopLevel)
		{
			List<BlobUrl> r = new List<BlobUrl>(); 

			PrepareConnection(parentUrl);

			if (parentUrl.Kind == BlobUrlKind.Account)
			{
				if (onlyTopLevel)
				{
					//list containers
					var allContainers = this._currentClient.ListContainers(null, ContainerListingDetails.None);
					foreach (CloudBlobContainer c in allContainers)
					{
						r.Add(new BlobUrl(c.Uri.AbsoluteUri));
					}
				}
				else
				{
					throw new Exception("Listing of all blobs in storage account not implemented.");
				}

			}
			else if (parentUrl.Kind == BlobUrlKind.Container)
			{
				var container = this._currentClient.GetContainerReference(parentUrl.Url);
				if (container == null)
				{
					throw new Exception("Container not found. Url:"+parentUrl.Url);
				}
				
				if (onlyTopLevel)
				{
					var allBlobs = container.ListBlobs(new BlobRequestOptions(){BlobListingDetails = BlobListingDetails.None,UseFlatBlobListing = false});
					//list top-level directories
					foreach (var dir in allBlobs.OfType<CloudBlobDirectory>())
					{
						r.Add(new BlobUrl(dir.Uri.AbsoluteUri));
					}

					//and top-level blobs
					foreach (var blob in allBlobs.OfType<CloudBlockBlob>())
					{
						r.Add(new BlobUrl(blob.Uri.AbsoluteUri));
					}
				}
				else
				{//list all blobs
					var allBlobs = container.ListBlobs(new BlobRequestOptions() { BlobListingDetails = BlobListingDetails.None, UseFlatBlobListing = true });
					foreach (IListBlobItem b in allBlobs)
					{
						r.Add(new BlobUrl(b.Uri.AbsoluteUri));
					}
				}
			}
			else if (parentUrl.Kind == BlobUrlKind.SubfolderOrBlob)
			{
				var container = this._currentClient.GetContainerReference(parentUrl.ContainerUrl);
				if (container == null)
				{
					throw new Exception("Container not found. Url:" + parentUrl.Url);
				}

				var directory = container.GetDirectoryReference(parentUrl.BlobName);
				if (directory == null)
				{
					throw new Exception("Directory not found. Container Url="+container.Uri.AbsoluteUri+"; Directory="+parentUrl.BlobName);
				}

				if (onlyTopLevel)
				{
					var allBlobs = directory.ListBlobs(new BlobRequestOptions() { BlobListingDetails = BlobListingDetails.None, UseFlatBlobListing = false });
					//list top-level directories
					foreach (var dir in allBlobs.OfType<CloudBlobDirectory>())
					{
						r.Add(new BlobUrl(dir.Uri.AbsoluteUri));
					}

					//and top-level blobs
					foreach (var blob in allBlobs.OfType<CloudBlockBlob>())
					{
						r.Add(new BlobUrl(blob.Uri.AbsoluteUri));
					}
				}
				else
				{//list all blobs
					var allBlobs = directory.ListBlobs(new BlobRequestOptions() { BlobListingDetails = BlobListingDetails.None, UseFlatBlobListing = true });
					foreach (IListBlobItem b in allBlobs)
					{
						r.Add(new BlobUrl(b.Uri.AbsoluteUri));
					}
				}
			}

			return r;
		}

		public int Delete(BlobUrl url)
		{
			int itemsDeleted = 0;

			PrepareConnection(url);

			if (url.Kind == BlobUrlKind.Account)
			{
				throw new Exception("Deletion of storage account is not implemented.");
			}
			else if (url.Kind == BlobUrlKind.Container)
			{
				CloudBlobContainer container = this._currentClient.GetContainerReference(url.Url);
				if (container == null)
				{
					throw new Exception(string.Format("Container '{0}' not found.", url.Url));
				}

				container.Delete();

				itemsDeleted++;
			}
			else if (url.Kind == BlobUrlKind.SubfolderOrBlob)
			{
				CloudBlob blob = this._currentClient.GetBlobReference(url.Url);

				if (blob!=null && this.Exists(blob))
				{
					blob.Delete();
					itemsDeleted++;
				}
				else
				{
					CloudBlobContainer container = this._currentClient.GetContainerReference(url.ContainerUrl);
					if (container==null)
					{
						throw new Exception(string.Format("Container '{0}' not found.", url.ContainerUrl));
					}

					CloudBlobDirectory dir = container.GetDirectoryReference(url.BlobName);
					if (dir == null)
					{
						throw new Exception("Directory not found. Container Url=" + container.Uri.AbsoluteUri + "; Directory=" + url.BlobName);
					}

					var matchedBlobs = dir.ListBlobs(new BlobRequestOptions() { BlobListingDetails = BlobListingDetails.None, UseFlatBlobListing = true });
					foreach (IListBlobItem listBlobItem in matchedBlobs)
					{
						CloudBlockBlob b = container.GetBlockBlobReference(listBlobItem.Uri.AbsoluteUri);
						b.Delete();
						itemsDeleted++;
					}
				}
			}

			return itemsDeleted;
		}
		#endregion

		#region Constructors And Initialization

		public HandlerForBlobs()
		{
			
		}
		public HandlerForBlobs(string connectionString)
		{
			//string currentConnectionString = connectionString;//string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", accountName, accountKey);
			CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
			this._currentClient = account.CreateCloudBlobClient();
		}
		private void PrepareConnection(BlobUrl blobUrl)
		{
			if (_currentClient != null && blobUrl.StorageName!=this._currentClient.Credentials.AccountName)
			{
				this._currentClient = null;
			}

			if (_currentClient == null)
			{
					CloudStorageAccount account = CloudStorageAccount.Parse(blobUrl.ConnectionString);
					this._currentClient = account.CreateCloudBlobClient();
			}

			if (_currentClient == null)
			{
				throw new Exception("Connection to Azure storage has not been created yet.");
			}
		}
		#endregion

		#region Deinitialization And Destructors

		#endregion

		#region Event Handlers

		#endregion

		#region Private Methods

		
		#endregion

		#region Helpers

		#endregion
	}
}
