# readWords

This is a c-based program using pthread to read words in different files.

There are 3 agruments. One is the num of threads and the second is the url String of the directory.The Thrid agruemnt is the path of the 
output file. PS: The num of threads has better lower than the nums of files in the directory.

Each file is only read by one pthread. when one pthread finish reading one file, it would check if there is other files remained to be 
read. Each thread has one datastruct hsahmap to store the words in the files which the thread read. At last all the hashmaps should be 
merged in the divide-and-conquer way parallelly. 
