# MIT-6.824

## 进度

- [x] MapReduce
  - [x] 阅读[论文](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf)
  - [x] 阅读课程的[Lab提示](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
  - [x] 通过 test
  - [x] 总结、复盘
- [x] Raft
  - [x] 阅读[官网介绍](https://raft.github.io/)
  - [x] 阅读[可视化过程](https://thesecretlivesofdata.com/raft/)
  - [x] 阅读[论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
    - [x] 先简单的看一遍
  - [x] 阅读课程的[Lab提示](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
  - [x] 通过 Test
    - [x] 2A
    - [x] 2B
    - [x] 2C
    - [x] 2D
  - [x] 总结、复盘





## 讲解

### MapReduce

#### 要求

MapReduce Lab 要求我们实现一个和MapReduce论文类似的机制，也就是数单词个数Word Count。

用于测试的文件在src/main目录下，以`pg-*.txt`形式命名。每个`pg-*.txt`文件都是一本电子书，非常长。我们的任务是统计出所有电子书中出现过的单词，以及它们的出现次数。



#### 流程

测试时，启动一个master和多个worker，也就是运行一次mrcoordinator.go、运行多次mrworker.go。其中，要给mrcoordinator.go输入电子书文件列表`pg-*.txt`，给mrworker.go指定动态库wc.so。

master进程启动一个rpc服务器，每个worker进程通过rpc机制向Master要任务。任务可能包括map和reduce过程，具体如何给worker分配取决于master。

每个单词和它出现的次数以key-value键值对形式出现。map进程将每个出现的单词机械地分离出来，并给每一次出现标记为1次。很多单词在电子书中重复出现，也就产生了很多相同键值对。还没有对键值对进行合并，故此时产生的键值对的值都是1。此过程在下图中mapper伸出箭头表示。

已经分离出的单词以键值对形式分配给特定reduce进程，reduce进程个数远小于单词个数，每个reduce进程都处理一定量单词。相同的单词应由相同的reduce进程处理。处理的方式和上面描述的算法类似，对单词排序，令单词在数组中处在相邻位置，再统计单词个数。最终，每个reduce进程都有一个输出，合并这些输出，就是Word Count结果。此过程在下图中箭头进入reducer、以及后面的合并表示。

![img](https://oiv0xvlqb8.feishu.cn/space/api/box/stream/download/asynccode/?code=YmE5ZjUwNWUxYTY0Zjk1MzZjMmYxYjc3MWUxYjRlODhfaDM3WUplcmpqZ09xNHcxd0RBY0xXVjhKQXlqc2RnelpfVG9rZW46S0lHVmI5c1JMb29BN2d4TXBaSGNDNXRSbktmXzE3MDY1MTk5MTk6MTcwNjUyMzUxOV9WNA)

图中，相同的单词由相同reducer处理。如第一个reducer接受单词A, B，最后一个reducer接受单词C。

测试流程要求，输出的文件个数和参数nReduce相同，即每个输出文件对应一个reduce任务，格式和mrsequential的输出格式相同，命名为`mr-out*`。我们的代码应保留这些文件，不做进一步合并，测试脚本将进行这一合并。合并之后的最终完整输出，必须和mrsequential的输出完全相同。
