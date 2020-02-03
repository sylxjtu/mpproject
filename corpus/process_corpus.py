from jieba import posseg
import sys
import multiprocessing

input_file = sys.argv[1]
output_file = sys.argv[2]

stopword = set([x.strip() for x in open('stopword.txt', encoding='utf8').readlines()])

corpus = [x.strip() for x in open(input_file, encoding='utf8').readlines()]
corpus = [[y for y, z in posseg.cut(x) if z not in ['x', 'm', 'eng'] and y not in stopword] for x in corpus]

open(output_file, 'w', encoding='utf8').writelines([' '.join(x) + '\n' for x in corpus])
