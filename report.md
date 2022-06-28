#  实验报告

## 并行化设计思路和方法 

算法首先基于PFP-Growth算法,它是一个对于FP-Growth算法的基础上使用了Map-Reduce的思想进行并行化处理得到的算法。
FP-Growths算法的基本思路是：
1. 第一次扫描数据库，寻找频繁1-项集，并按照由大到小的顺序排序。
2. 创建FP模式树的根结点，记为“null”。
3. 根据频繁1-项集的顺序对数据库中的每条事务数据进行排序，并存储在FP模式树中，并建立项头表。
4. 为每一个频繁1-项集寻找前缀路径，组成条件模式基，并建立条件FP树。
5. 递归挖掘条件FP树，获得频繁项集。

对应的伪代码如下：其中DB代表事务数据库，ξ代表最小支持度阈值

```
Procedure: FPGrowth(DB, ξ)
Define and clear F-List : F[];
foreach T ransaction Ti in DB do
foreach Item aj in Ti do
        F[ai] ++;
    end
end
Sort F[];
Define and clear the root of FP-tree : r;
foreach T ransaction Ti in DB do
    Make Ti ordered according to F;
    Call ConstructT ree(Ti, r);
end
foreach item ai in I do
    Call Growth(r, ai, ξ);
end

Procedure: Growth(r, a, ξ)
if r contains a single path Z then
    foreach combination(denoted as γ) of the nodes in
    Z do
        Generate pattern β = γ ∪ a with support =
        minimum support of nodes in γ;
        if β.support > ξ then
            Call Output(β);
        end
    end
else
    foreach bi in r do
        Generate pattern β = bi ∪ a with support =
        bi.support;
        if β.support > ξ then
            Call Output(β);
        end
        Construct β's conditional database ;
        Construct β's conditional FP-tree Treeβ;
        if T reeβ != φ then
            Call Growth(T reeβ, β, ξ);
        end
    end
end

```

PFP-Growth算法对于数据挖掘中的海量大数据进行分片，采用并行化处理的方式来解决问题。在FP-Growth算法的基础上进行了处理：
1. 在存储，上对于在FP-Growth算法上巨大的FP-tree进行了划分得到更小局部FP-tree。因此，新的数据可以在内存中直接存放。
2. 在计算上，对于FP-Growth的算法进行了并行化处理，尤其是对于递归函数Growth()进行了并行。
3. 在通信上，对于FP-Growth算法的并行中，通过对于数据的合理划分，减少了跨组的交易通信，得到更好的秉性度。
4. 在参数ξ最小支持度阈值上，可以支持很小的阈值。

PFP-Growth算法的基本步骤如下：
1. 分片 对于DB进行划分，存储在不同的partition上。
2. 并行计算 进行一个Map-Reduce的过程。计数不同的item，每个mapper出入一个DB的分片，reducer进行计数，结果存储在F-list中。
3. Item分组 对于F-list中的item进行group划分为G-list，每个G-list对应一个不同的gid。
4. 并行FP-growth计算 这步需要进行1个完成整的Map-Reduce的过程。Mapper对应读取G-list中的一个gid，对应得到当前gid的交易。Reducer根据gid把交易划分到不同的分片中。在分片中，Reducer还会对应生成一个局部的FP-tree
5. 聚合 这步聚合步骤4的结果，得到最后的频繁规则。

对应伪代码如下：

```
// The Parallel Counting Algorithm
Procedure: Mapper(key, value=Ti)
foreach item ai in Ti do
    Call Output(hai,0 10i);
end
Procedure: Reducer(key=ai, value=S(ai)) 
C ← 0;
foreach item '1' in Ti do
    C ← C + 1;
end
Call Output(<null, ai + C>);

//The Parallel FP-Growth Algorithm
Procedure: Mapper(key, value=Ti)
Load G-List;
Generate Hash Table H from G-List;
a[] ← Split(Ti);
for j = |Ti| − 1 to 0 do
    HashNum ← getHashNum(H, a[j]);
    if HashNum = Null then
        Delete all pairs which hash value is HashNum
        in H;
        Call
        Output(<HashNum, a[0] + a[1] + ... + a[j]>);
    end
end
Procedure: Reducer(key=gid,value=DBgid)
Load G-List;
nowGroup ← G-Listgid;
LocalF P tree ← clear;
foreach Ti in DB(gid) do
    Call insert − build − f p − tree(LocalF P tree, Ti);
end
foreach ai in nowGroup do
    Define and clear a size K max heap : HP;
    Call T opKF P Growth(LocalF P tree, ai, HP);
    foreach vi in HP do
        Call Output(<null, vi + supp(vi)>);
    end
end

//The Aggregating Algorithm
Procedure: Mapper(key, value=v + supp(v))
foreach item ai in v do
    Call Output(hai, v + supp(v)i);
end
Procedure: Reducer(key=ai, value=S(v + supp(v)))
Define and clear a size K max heap : HP;
foreach pattern v in v + supp(v) do
    if |HP| < K then
        insert v + supp(v) into HP;
    else
        if supp(HP[0].v) < supp(v) then
            delete top element in HP;
            insert v + supp(v) into HP;
        end
    end
end
Call Output(<null, ai + C>);
```

## 详细的算法设计与实现 

## 实验结果与分析

## 程序代码说明
