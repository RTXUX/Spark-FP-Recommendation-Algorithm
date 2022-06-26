# 面向大数据的高效关联规则推荐算法

基于 Spark，使用 PFP-Growth 算法和关联规则进行购物篮推荐的程序

## 算法流程

1. 使用 PFP-Growth 算法以给定支持度挖掘输入数据集的频繁模式
2. 基于挖掘到的频繁模式生成关联规则
3. 根据关联规则和用户概貌推荐购物篮条目

## 特点

- 改进 PFP-Growth 算法实现：对于候选 `transaction`， 首先基于其最后一个条目的位置哈希，进行 `repartitionAndSortWithinPartitions` ，以重新分布数据，达到负载均衡
- 改进的 `AssociationRule` 数据结构：考虑到可假定关联规则后项仅一个条目，使用简单基本类型而不是数组存储后项，降低开销
- 自适应资源利用机制：考虑到在全量数据集上运行算法的内存需求极大，本算法自动根据分配的内存和核数选择合适的并行度和分区数，避免 `Out of Memory`。经测算，每个并行执行线程大约需要 `8G` 内存。

## 使用

1. 编译 SBT 项目得到 `jar` 文件
2. 向 Spark 集群提交任务，具体方法因集群部署方式而异，一个简单的示例如下：

```bash
spark-submit \
	--master yarn\
	--deploy-mode cluster \
	--class AR.Main \
	--name "AR" \
	--executor-memory 100G \
	--executor-cores 32G \
	--num-executors 1 \
	${JAR_PATH} \
	${INPUT_DATA_DIRECTORY_PATH} \
	${OUTPUT_DATA_DIRECTORY_PATH} \
	${TEMP_DIRECTORY} \
```

本程序假设 `${INPUT_DATA_DIRECTORY_PATH}` 下存有购物篮数据集 `D.dat` 和用户概貌数据集 `U.dat` ，`${OUTPUT_DATA_DIRECTORY_PATH}` 和 `${TEMP_DIRECTORY}` 存在且为空。生成的频繁模式和推荐结果分别保存在 `${OUTPUT_DATA_DIRECTORY_PATH}` 下的 `Freq` 和 `Rec`。