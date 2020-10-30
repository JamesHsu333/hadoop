#include <algorithm>
#include <iostream>
#include <set>
#include <string>
using namespace std;

bool getSubstr(string *str, string delimiter, string *substring){
    size_t position = 0;
    if((position = (*str).find(delimiter)) != string::npos){
        *substring = (*str).substr(0, position);
        (*str).erase(0, position+delimiter.length());
        return true;
    }
    return false;
}

void check_duplicate(string key, set<string> &s){
    set<string>::iterator it;
    for(it = s.begin() ; it != s.end() ; it++){
        cout << key << " " << *it << endl;
    }
    s.clear();
}

int main() {
    string line;
    string key;
    string last_key;
    set<string> s;
    while(getline(cin, line)){
        // Trim the last whitespace from raw data
        line.erase(line.end()-1, line.end());
        getSubstr(&line, " ", &key);

        if(last_key==key){
            s.insert(line);
        }else{
            if(!last_key.empty()){
                check_duplicate(last_key, s);
                s.insert(line);
                last_key = key;
            }else{
                last_key = key;
                s.insert(line);
            }
        }
    }
    check_duplicate(last_key, s);
    return 0;
}
