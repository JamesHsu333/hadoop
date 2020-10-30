#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
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

vector<string> intersection(vector<string> &v1, vector<string> &v2){
    vector<string> v3;
    set_intersection(v1.begin(),v1.end(), v2.begin(), v2.end(), back_inserter(v3));
    return v3;
}

void print_result(vector<vector<string>> &values, string key){
    vector<vector<string>>::iterator it;
    for(it=values.begin();it!=values.end()-1;it++){
        vector<string> value = intersection(values.back(), *it);
        if(value.size() > 1){
            cout << key;
            for(vector<string>::iterator i=value.begin();i!=value.end();i++){
                cout << " " << *i;
            }
            cout << endl;
        }
    }
}

int main() {
    string line;
    string delimiter = " ";
    string key;
    string current_key;
    vector<vector<string>> values;
    vector<string> value;
    string tmp;

    while(getline(cin, line)){

        // Trim the last whitespace from raw data
        line.erase(line.end()-1, line.end());

        getSubstr(&line, delimiter, &key);

        if(current_key == key) {
            while(getSubstr(&line, delimiter, &tmp)){
                value.push_back(tmp);
            }
            value.push_back(line);
            values.push_back(value);
        }else{
            if(current_key.empty()){
                current_key = key;
                while(getSubstr(&line, delimiter, &tmp)){
                    value.push_back(tmp);
                }
                value.push_back(line);
                values.push_back(value);
            }else{
                while(!values.empty()){
                    print_result(values, current_key);
                    values.pop_back();
                }
                cout << current_key << " " << current_key << endl;
                current_key = key;
                values.clear();

                while(getSubstr(&line, delimiter, &tmp)){
                    value.push_back(tmp);
                }
                value.push_back(line);
                values.push_back(value);
            }
        }
        value.clear();
    }
    while(!values.empty()){
        print_result(values, current_key);
        values.pop_back();
    }
    cout << current_key << " " << current_key << endl;
    values.clear();
    return 0;
}
