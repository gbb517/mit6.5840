#include <mapreduce/MapReduce.h>

using std::vector;
using std::string;
using std::isalpha;

/*
 * key: file name
 * value: file content
 */
extern "C"
vector<KeyValue> wordCountMapF(string& key, string& value) {
    const int N = value.size();
    int i = 0;
    // 跳过开头的非字母字符
    while(i < N && !isalpha(value[i])) {
        i++;
    }

    vector<KeyValue> rs;
    int j = i;
    while (i < N) {
        // 当前单词
        while (j < N && isalpha(value[j])) {
            j++;
        }
        // 计数
        rs.push_back({value.substr(i, j - i), "1"});
        // 跳过中间字符
        while (j < N && !isalpha(value[j])) {
            j++;
        }
        i = j;
    }
    return rs;
}

/*
 * key: a word
 * value: a list of counts
 */
extern "C"
vector<string> wordCountReduceF(string& key, vector<string>& value) {
    // 转换为出现次数
    return {std::to_string(value.size())};
}
