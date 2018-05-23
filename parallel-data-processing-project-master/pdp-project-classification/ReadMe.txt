To run the programs on git:

1. Clone the programs to your machine.
2. Open as a Maven project in Eclipse IDE.
3. Do maven clean and maven install to resolve dependencies
4. Run the programs with run configurations to set arguments for input and output locations

5. You can use "make alone" command for local standalone execution via terminal

For AWS execution:

1. Edit the make file with job.name set to the name of the class to run along with package name
2. set aws.bucket.name based on your program version for separation
3. via terminal and aws cli set up, carry out following commands:
		make make-bucket
		make upload-input-aws  // input folder should be in same directory as the makeFile 
		make upload-app-aws // Target folder should be in the same directory as the makeFile

In the make file, change the following: 

aws.num.nodes=<number of workers required> example: aws.num.nodes=15

and set:

aws.subnet.id=<your subnet id>


For this assignment, I have the buckets ready. Use make upload-input-aws to upload input folder on s3.

Change makeFile for job.name to run following:

	pdp_project.pdp_project_classification.Prediciting

But bucketname can be anything in your case: aws.bucket.name=<bucketname>

Finally, you can run the following command:

	make cloud

This will start the cluster with configurations specified.

