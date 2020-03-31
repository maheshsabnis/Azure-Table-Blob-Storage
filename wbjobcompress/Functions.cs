using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Blob;
using EntityStores;
using System.Threading;
using System.IO.Compression;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Configuration;

namespace wbjobcompress
{
    public class Functions
    {
        // This function will get triggered/executed when a new message is written 
        // on an Azure Queue called profileRequestQueue.
        public async static Task ProcessQueueMessageAsyncCancellationToken(
         [QueueTrigger("profileRequestQueue")] BlobInfo blobName,
         [Blob("profiles/{BLOBName}", FileAccess.Read)] Stream blobInput,
         [Blob("profiles/{BlobNameWithNoExtension}_Profile.docx")] CloudBlockBlob blobOutput,
         CancellationToken token)
        {

            Console.WriteLine("In Web Job");
            using (Stream output = blobOutput.OpenWrite())
            {
               await CompressProfile(blobInput, output);
            
            //Update the Table Information
            UpdateEntity(GetEntity(blobName.Profession,blobName.ProfileId.ToString()),output.Length);
            }
            

        }

        /// <summary>
        /// Method to Compress the Profile File
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        private static Task<Stream> CompressProfile(Stream input,Stream output)
        {

            byte[] b;

            b = new byte[output.Length];
            output.Read(b, 0, (int)output.Length);
            
            using (GZipStream gz = new GZipStream(input, CompressionMode.Compress, false))
            {
                gz.Write(b, 0, b.Length);
            }

            return new Task<Stream>(()=>output);
        }

        /// <summary>
        /// Method to Get the Profile Information
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        private static ProfileEntity GetEntity(string partitionKey, string rowKey)
        {
            ProfileEntity entity = null;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationSettings.AppSettings["azwebjob"]); ;
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();


            CloudTable table = tableClient.GetTableReference("ProfileEntityTable");

            TableOperation tableOperation = TableOperation.Retrieve<ProfileEntity>(partitionKey, rowKey);
            entity = table.Execute(tableOperation).Result as ProfileEntity;

            return entity;
        }

        /// <summary>
        /// Method to Update Entity with the Profile Path in the BLOB
        /// and the Compressed FIle Size
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        private static ProfileEntity UpdateEntity(ProfileEntity entity,long size)
        {

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationSettings.AppSettings["azwebjob"]); ;
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference("ProfileEntityTable");

            TableOperation tableOperation = TableOperation.Retrieve<ProfileEntity>(entity.Profession, entity.ProfileId.ToString());

            var Res = table.Execute(tableOperation).Result as ProfileEntity;

            if (Res != null)
            {
                Res.ProfilePath = entity.ProfilePath;
                

                TableOperation updateOperation = TableOperation.Replace(Res);

                table.Execute(updateOperation);
            }


            return Res;
        }
    }
}
