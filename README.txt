Distributed System Programming - Assignment 2:

Students:
Amir Tsur - 203795828
Kobi Eini - 201553245

S3 Result bucket : https://s3.console.aws.amazon.com/s3/buckets/amirtsurmapreduce/?region=eu-west-3&tab=overview

Instructions to run the project:
	1) Create a S3 bucket named "amirtsurmapreduce".
	2) Create the following folders in "amirtsurmapreduce": 
	     - logs
	3) Upload the following jar files to:
	     -FirstLevel
	     -SecondLevel
	     -ThirdLevel
	4) Run Main file with AWS Credentials: java -jar Main.jar valueOf(minPmi) valueOf(relMinPmi) 

Job Flow Description:
	First Step:
		Mapper<LongWritable, Text, FirstStepKey, LongWritable>: 
			Creates the following Key-Value pairs:
			a) (firstWord, SecondWord, Decade) : occurrences. - for couples count
			b) (firstWord, *, Decade) : occurrences. - for first words count
			c) (*, SecondWord, Decade) : occurrences. - for second word count
			d) (*, *, Decade) : occurrences. - for decade count
		Combiner<LongWritable, Text, FirstStepKey, LongWritable>: 
			Accumulate the number of occurrences for each certain key
		Partitioner: Partition by the firstWord and Decade.
		Reducer<FirstStepKey, LongWritable, FirstStepKey, FirstStepValue>: 
			Performs the following for each Key:
			a) Emits (seconWord, firstWord, Decade), with the counts of the certain pair and the nubmer of pairs begin with firstWord.
			b) Saves the number received form the accumolation as CW1 representing the number of pairs begin with firstWord.
			c) Emits (secondWord, *, Decade) with the number recieved from the accumolation as the number of pairs ends with secondWord.
			d) Emits the Decade and the number received form the accumolation to a bucket created in S3.
	
	Second Step:
		Mapper<FirstStepKey, FirstStepValue, FirstStepKey, FirstStepValue>: 
			Emits the given Key-Value pairs:
			a) (secondWord, *) : occurrences. - for second word count
			b) (secondWord, firstWord) : occurrences. - for firstWord and couples counts.
		Partitioner: Partition by the firstWord and Decade.
		Reducer<FirstStepKey, FirstStepValue, SecondStepKey, DoubleWritable>: 
			Setup: Pulls the decade counts from the S3 bucket and saves into a HashTable in the lockal computer.
			Reduce: Performs the following for each Key:
				a) Saves the number received form the accumolation as CW2 representing the number of pairs ends with secondWord.
				b) 1) Emits (firstWorf, secondWord, Decade, Npmi) with the calculated Npmi as value. - for couple's npmi
				    2) Emits (*, *, Decade, 0.0) with the calculated Npmi as value. - for accumolation of all the Npmis in a decade

	Third Step:
		Mapper<SecondStepKey, DoubleWritable, SecondStepKey, DoubleWritable>: 
			Emits the given Key-Value pairs:
			a) (firstWorf, secondWord, Decade, Npmi) : Npmi - for couple's npmi
			b) (*, *, Decade, 0.0) : Npmi - for accumolation of all the npmis in a decade
		Combiner<SecondStepKey, DoubleWritable, SecondStepKey, DoubleWritable>: 
			Accumulate the npmi for each certain key
		Partitioner: Partition by the Decade.
		Reducer<SecondStepKey, DoubleWritable, Text, Text>: 
			Performs the following for each Key:
			a)  Emits (keyText) with the npmiText as value.
				keyText = firstWord +" "+ secondWord +" "+ Decade.				
				npmiText = "npmi: " + the number received form the accumolation +" relative npmi: " + the number received form the accumolation devided by decadeNpmi.
			b) Saves the number received form the accumolation as decadeNpmi representing the sum of all the Nmpis in that decade.

		



