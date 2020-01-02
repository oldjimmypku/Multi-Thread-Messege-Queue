# Multi-Thread-Messege-Queue
My idea is that for each Topic or Queue, serializing all messages and write them to a corresponding file. When doing Consumption, consumers read data from the disk sequentially.

Most importantly, the bottleneck of this question was on disk IO. To keep the question simple and clear, I abandon irrelevant optimizations. To get the most out of disk IO, All I did is making two things done:

1.	Sequential write and read
2.	Use the Linux memory mapping (mmap) technique, which is the MappedByteBuffer in the Java nio package

In addition, minimizing memory copies in the process of serialization and avoiding unnecessary String serialization and deserialization are also critical. 
