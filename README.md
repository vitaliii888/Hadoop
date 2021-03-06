# Hadoop:

## wordcount
Подсчитать количество слов в файле - аналог "Hello, world" для Hadoop.

## wordlen
Подсчитать длины слов в файле путем использования регулярных выражений на стадии `map`, а на стадии `reduce` подсчитать, сколько слов каждой длины имеется в файле.

## simplemgen
Применение Hadoop для генерации матриц большой размерности. Интересной и нетривиальной задачей в ходе разработки приложения была цель избавиться от необходимости использования каких-либо входных данных для стадии `map`. 

Традиционная схема разработки MapReduce-приложений предполагает наличие пар (ключ, значение) в качестве входных данных как для стадии `map`, так и для стадии `reduce`. В ходе преобразования на каждой стадии пара `(key1, value1)` переходит в некоторую пару `(key2, value2)`, но при этом результатом обработки одной пары не может быть, к примеру, 10 или 1000 других пар (ключ, значение). 

Исходя из данной логики, для генерации матрицы размером 1000 * 1000 надо было бы сгенерировать 1000000 пар (ключ, значение) и сохранить их в каком-либо хранилище, чтобы просто запустить работу алгоритма. Очевидно, данный подход является весьма неэффективным и излишним, поэтому была успешно решена задача отказа от использования каких-либо входных данных на стадии `map`.

## mm
Реализация алгоритма блочного перемножения матриц большой размерности, позволяющего эффективно использовать возможности Hadoop в части организации параллельных вычислений для ускорения процесса умножения матриц.

## candle
Применение Hadoop для предобработки финансовых данных. Задача заключалась в построении [японских свечей](https://ru.wikipedia.org/wiki/Японские_свечи) на основе биржевых данных.
