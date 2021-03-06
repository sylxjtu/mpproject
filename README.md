# MapReduce 课程大作业

## 数据与预处理

### 语料预处理 `corpus`

语料来源：高中地理课本，教辅，习题答案，部分维基百科

分词工具：jieba，去除所有标点、数词、停用词、外文词

停用词表：https://github.com/goto456/stopwords/blob/master/%E4%B8%AD%E6%96%87%E5%81%9C%E7%94%A8%E8%AF%8D%E8%A1%A8.txt

词性参考：https://gist.github.com/luw2007/6016931

### 词表 `word`

来源：输入法词表等收集到的词表

## 计算

*TODO*

### 计算单词词频

### 计算共现频率

### 计算全文词数量总计

### 计算PMI

## 结果

### 结果位置

```text
/user/2019st17/project/pmi/part-m-0000
```

### 部分结果

```text
一次能源###中国	6.217147779332265
一次能源###二次能源	17.94006201476878
一次能源###二氧化碳	7.7673219977194154
一次能源###再生	10.060478765156
一次能源###再生能源	10.722104316904668
一次能源###分解	9.637423090981232
一次能源###利用	6.261351851261958
一次能源###化石	8.628029956023921
一次能源###原煤	12.600212011884157
一次能源###发展	5.269129201853678
一次能源###发电机	12.06969729518538
一次能源###发电量	10.360746077188768
一次能源###增长	7.374959936408468
一次能源###天然气	8.422146621852912
一次能源###工业	4.973015923905658
一次能源###废弃物	9.804352728664384
一次能源###开发	7.316180524755325
一次能源###总量	7.706741932037961
一次能源###损耗	11.800510662369987
...
一氧化二氮###中国	5.287537107223663
一氧化二氮###二氧化碳	11.329564421940487
一氧化二氮###京都	13.462014717964138
一氧化二氮###人类	8.002302882750087
一氧化二氮###俄罗斯	7.154492781198748
一氧化二氮###六氟化硫	16.94006201476878
一氧化二氮###养分	10.099958510371465
一氧化二氮###农业	4.784249909532559
一氧化二氮###加速	9.01270731660055
一氧化二氮###化石	8.020347378802681
一氧化二氮###国家	6.304473440977658
一氧化二氮###土壤	7.2077734305883485
一氧化二氮###大气	6.548358113173364
一氧化二氮###大气层	7.8732040019955125
一氧化二氮###工业革命	10.725049123797932
一氧化二氮###废气	10.870899990261385
一氧化二氮###影响	5.488801832511086
一氧化二氮###悬浮	11.12536511737001
一氧化二氮###指标	9.975927467325787
一氧化二氮###排出	11.312788709189661
一氧化二氮###排放	10.963600177506205
一氧化二氮###施肥	12.223854980769374
一氧化二氮###有机质	9.97923261204946
一氧化二氮###气体	11.375724050800244
...
```

## 运行说明

```bash
hadoop jar mp_project-1.0-SNAPSHOT.jar mpproject.Main config.json
```

