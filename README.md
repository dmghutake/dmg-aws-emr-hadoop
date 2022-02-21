# AWS Elastic MapReduce with Springboot and Hadoop EMR Cluster.
  ### Steps to create EMR cluster and execute spring boot Job to process and reduce on file and genrate output.  
  * Create keypair for ec2 instance.
  * Need disk drive to put input data and output data - S3 harddrive.
  * Upload data file to S3 bucket.
  * create map reduce cluster
	  * Provide keypair
	  * Select cluster framework 
	  * Choose ec2 instances 
 * Upload java application jar file to S3 Bucket.
 * Once EMR Cluster provisions with EC2 instances , go to Step section.
 * Add step to locate application jar file , input file location and output folder name in argument box.
 * Once Step execution completes you can checkout output.
 * Clean up - delet s3 and terminate EC2 instances , EMR Cluster
 
